# main.py  – drop in and commit
import os, hmac, hashlib, time, json, logging
from datetime import datetime, timedelta, timezone

import requests
from fastapi import FastAPI, Header, Request

logging.basicConfig(level=logging.INFO)
app = FastAPI()

# ── secrets from Render ────────────────────────────────────────────────
SQUARE_TOKEN       = os.environ["SQUARE_ACCESS_TOKEN"].strip()
ZOHO_CLIENT_ID     = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH       = os.environ["ZOHO_REFRESH_TOKEN"].strip()

# optional – if unset we still start (makes local dev easy)
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY", "").strip()

# ── Square helpers ─────────────────────────────────────────────────────
def verify_square_sig(body: bytes, header_sig: str) -> bool:
    """return True if signature is valid OR key is not configured"""
    if not SQUARE_WEBHOOK_KEY:
        return True
    digest = hmac.new(SQUARE_WEBHOOK_KEY.encode(), body, hashlib.sha1).hexdigest()
    return hmac.compare_digest(digest, header_sig)

def get_square_customer(customer_id: str):
    r = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers={
            "Authorization": f"Bearer {SQUARE_TOKEN}",
            "Square-Version": "2024-04-17",
        },
        timeout=15,
    )
    if r.status_code == 200:
        return r.json()["customer"]
    return None

# ── Zoho helpers ───────────────────────────────────────────────────────
ZOHO_DOMAIN = "https://www.zohoapis.com"

def zoho_access_token() -> str:
    """lazy refresh – token cached in memory for its 1 h lifetime"""
    if getattr(zoho_access_token, "token", None) and time.time() < zoho_access_token.exp:
        return zoho_access_token.token

    payload = {
        "client_id":     ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type":    "refresh_token",
        "refresh_token": ZOHO_REFRESH,
    }
    r = requests.post(f"{ZOHO_DOMAIN}/oauth/v2/token", data=payload, timeout=15).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho token error: {r}")
    zoho_access_token.token = r["access_token"]
    zoho_access_token.exp   = time.time() + int(r["expires_in"]) - 30
    return zoho_access_token.token


def zoho_search_email(email: str):
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    # Leads first
    url = f"{ZOHO_DOMAIN}/crm/v6/Leads/search?email={email}"
    r = requests.get(url, headers=hdr, timeout=15).json()
    if r.get("data"):
        return ("Leads", r["data"][0])
    # then Contacts
    url = f"{ZOHO_DOMAIN}/crm/v6/Contacts/search?email={email}"
    r = requests.get(url, headers=hdr, timeout=15).json()
    if r.get("data"):
        return ("Contacts", r["data"][0])
    return (None, None)


def zoho_event_exists(square_id: str):
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    criteria = f"(Square_Meeting_ID:equals:{square_id})"
    url = f"{ZOHO_DOMAIN}/crm/v6/Events/search?criteria={criteria}"
    r = requests.get(url, headers=hdr, timeout=15).json()
    return bool(r.get("data"))

def create_zoho_lead(customer):
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    lead = {
        "Lead_Source": "Square",
        "First_Name":  customer.get("given_name", ""),
        "Last_Name":   customer.get("family_name", "Square"),
        "Email":       customer.get("email_address"),
        "Phone":       customer.get("phone_number"),
    }
    r = requests.post(f"{ZOHO_DOMAIN}/crm/v6/Leads",
                      headers=hdr, json={"data":[lead]}, timeout=15).json()
    return r["data"][0]


def create_zoho_event(module: str, record_id: str, square_booking: dict,
                      customer: dict, location_name: str):
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}

    square_id   = square_booking["id"]
    duration    = square_booking["appointment_segments"][0]["duration_minutes"]
    start_utc   = datetime.fromisoformat(square_booking["start_at"].replace("Z","+00:00"))
    end_utc     = start_utc + timedelta(minutes=duration)

    title = f"Himplant Consultation - {customer['given_name']} {customer['family_name']} ({location_name})"

    event = {
        "Event_Title":        title,
        "Start_DateTime":     start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "End_DateTime":       end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "Square_Meeting_ID":  square_id,
        "Meeting_Status":     "Planned",
        "$se_module":         module,
        "What_Id":            record_id,
        "Participants": [{
            "participant": customer["email_address"],
            "type": "email",
            "name": f"{customer['given_name']} {customer['family_name']}",
            "invited": True,
            "status": "invited"
        }],
    }
    requests.post(f"{ZOHO_DOMAIN}/crm/v6/Events",
                  headers=hdr, json={"data":[event]}, timeout=15)


# ── FastAPI endpoint ───────────────────────────────────────────────────
@app.post("/square/webhook")
async def square_webhook(request: Request,
                         x_square_signature: str = Header(None)):
    body = await request.body()

    # 1. signature check
    if not verify_square_sig(body, x_square_signature or ""):
        logging.warning("Signature mismatch – request dropped")
        return {"ok": True}

    try:
        payload = request.json() if isinstance(request, dict) else json.loads(body)
    except Exception:
        logging.error("Empty / non-JSON body from Square")
        return {"ok": True}

    event_type   = payload.get("type", "")
    square_data  = payload["data"]["object"]["booking"]
    square_id    = square_data["id"]

    # 2. only act on booking.created / updated / canceled
    if event_type not in {"booking.created", "booking.updated", "booking.canceled"}:
        return {"ignored": event_type}

    # 3. duplicate guard
    if zoho_event_exists(square_id):
        return {"duplicate": square_id}

    # 4. get customer details
    cust_id = square_data["customer_id"]
    customer = get_square_customer(cust_id) or {
        "given_name": "Square",
        "family_name": "Customer",
        "email_address": f"{cust_id}@square.local",
        "phone_number": ""
    }

    # 5. find Lead/Contact or create Lead
    module, record = zoho_search_email(customer["email_address"])
    if not record:
        record = create_zoho_lead(customer)
        module = "Leads"
    record_id = record["id"]

    # 6. make human-readable location (Square keeps only ID)
    location_name = "Square Location"

    create_zoho_event(module, record_id, square_data, customer, location_name)

    return {"ok": True}
