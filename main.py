# ──────────────────────────────────────────────────────────────
#  main.py  –  Square → Zoho CRM bridge
# ──────────────────────────────────────────────────────────────
import os, time, hashlib, hmac, json, typing as t
from datetime import datetime, timedelta, timezone

import requests
from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv

load_dotenv()        # pull secrets from Render env or .env locally
app = FastAPI()

# ─── Config from environment ──────────────────────────────────
SQUARE_TOKEN        = os.environ["SQUARE_ACCESS_TOKEN"].strip()
SQUARE_WEBHOOK_KEY  = os.environ["SQUARE_WEBHOOK_KEY"].strip()          # “Signature Key” from Square webhook
ZOHO_CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"].strip()
ZOHO_API_DOMAIN     = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")
TIME_ZONE           = os.getenv("TZ", "US/Pacific")                     # used only for human-friendly title
# cache token in memory
_ZOHO_TOKEN: tuple[str, float] | None = None    # (token, epoch-expiry)

# ─── Helpers ──────────────────────────────────────────────────
def zoho_access_token() -> str:
    """
    Keep a fresh Zoho OAuth access token in memory.
    """
    global _ZOHO_TOKEN
    if _ZOHO_TOKEN and _ZOHO_TOKEN[1] > time.time() + 60:
        return _ZOHO_TOKEN[0]

    rsp = requests.post(
        f"{ZOHO_API_DOMAIN}/oauth/v2/token",
        params=dict(
            refresh_token = ZOHO_REFRESH_TOKEN,
            client_id     = ZOHO_CLIENT_ID,
            client_secret = ZOHO_CLIENT_SECRET,
            grant_type    = "refresh_token",
        ),
        timeout=15,
    ).json()
    if "access_token" not in rsp:
        raise RuntimeError(f"Zoho token error: {rsp}")
    _ZOHO_TOKEN = (rsp["access_token"], time.time() + int(rsp["expires_in"]) - 30)
    return _ZOHO_TOKEN[0]

def square_get(path: str) -> dict:
    """
    Safe GET wrapper for Square; always returns dict ({} on error).
    """
    try:
        r = requests.get(
            f"https://connect.squareup.com{path}",
            headers={"Authorization": f"Bearer {SQUARE_TOKEN}",
                     "Accept": "application/json"},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}

def verify_square_sig(raw_body: bytes, sig: str) -> bool:
    h = hmac.new(SQUARE_WEBHOOK_KEY.encode(), raw_body, hashlib.sha1).hexdigest()
    return h == sig

def iso_to_zoho(iso: str, minutes: int) -> tuple[str, str]:
    """
    Return (start, end) in Zoho’s `yyyy-MM-ddTHH:mm:ssZ` (UTC) format.
    """
    start_dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    end_dt   = start_dt + timedelta(minutes=minutes)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start_dt.strftime(fmt), end_dt.strftime(fmt)

def zoho_find_lead_or_contact(email: str) -> tuple[str | None, str]:
    """
    Look up existing Lead or Contact by email.
    Returns (record_id or None, module_name).
    """
    crit = f"(Email:equals:{email})"
    hdr  = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    for module in ("Leads", "Contacts"):
        r = requests.get(f"{ZOHO_API_DOMAIN}/crm/v4/{module}/search",
                         params={"criteria": crit},
                         headers=hdr, timeout=15).json()
        data = r.get("data")
        if data:
            return data[0]["id"], module
    return None, "Leads"

def zoho_create_lead(email: str, first: str, last: str, phone: str) -> str:
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    body = {"data": [{
        "First_Name": first, "Last_Name": last or "Square",
        "Email": email,
        "Phone": phone,
        "Lead_Source": "Square"
    }]}
    r = requests.post(f"{ZOHO_API_DOMAIN}/crm/v4/Leads",
                      headers=hdr, json=body, timeout=15).json()
    return r["data"][0]["details"]["id"]

def zoho_create_event(record_id: str, module: str, title: str,
                      start: str, end: str, square_id: str):
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    body = {"data": [{
        "Event_Title"     : title,
        "Start_DateTime"  : start,
        "End_DateTime"    : end,
        "All_day"         : False,
        "Meeting_Status"  : "Scheduled",
        "Square_Meeting_ID": square_id,
        "$se_module"      : module,
        "What_Id"         : {"id": record_id}
    }]}
    requests.post(f"{ZOHO_API_DOMAIN}/crm/v4/Events",
                  headers=hdr, json=body, timeout=15)

# ─── FastAPI routes ───────────────────────────────────────────
@app.post("/square/webhook")
async def square_webhook(request: Request):
    raw = await request.body()
    sig = request.headers.get("x-square-hmacsha256-signature", "")
    if not verify_square_sig(raw, sig):
        raise HTTPException(401, "Bad signature")

    payload = await request.json()
    event_type = payload.get("type", "")
    if not event_type.startswith("booking."):
        return {"ignored": event_type}

    booking = payload["data"]["object"]["booking"]
    square_id   = booking["id"]
    customer_id = booking.get("customer_id")
    # --- check if we already created this Event ---
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    dup = requests.get(f"{ZOHO_API_DOMAIN}/crm/v4/coql",
                       headers=hdr,
                       json={"select_query":
                             f"select id from Events where Square_Meeting_ID = '{square_id}'"},
                       timeout=15).json()
    if dup.get("data"):
        return {"status": "duplicate skipped"}

    # --- get customer profile ---
    cust = square_get(f"/v2/customers/{customer_id}")
    profile = cust.get("customer", {})
    email = profile.get("email_address", "")
    phone = profile.get("phone_number", "")
    first = profile.get("given_name", "")
    last  = profile.get("family_name", "")

    # --- Lead/Contact handling ---
    rec_id, module = (None, "Leads")
    if email:
        rec_id, module = zoho_find_lead_or_contact(email)
    if not rec_id:
        rec_id = zoho_create_lead(email, first, last, phone)
        module = "Leads"

    # --- Build meeting data ---
    loc = square_get(f"/v2/locations/{booking.get('location_id')}").get("location", {})
    surgeon_or_loc = loc.get("name", "Square")
    title = f"Himplant Consultation - {first} {last} ({surgeon_or_loc})"

    start_iso = booking["start_at"]
    dur = booking["appointment_segments"][0]["duration_minutes"]
    start, end = iso_to_zoho(start_iso, dur)

    zoho_create_event(rec_id, module, title, start, end, square_id)
    return {"status": "created", "record": rec_id}
