# ---------------------------  main.py  ---------------------------
import os
import time
import hmac
import json
import hashlib
from datetime import datetime, timedelta, timezone

import requests
from fastapi import FastAPI, Request, HTTPException, status

# ───────────────────────────────────────────────────────────────────
#  Environment variables (required in Render → Environment tab)
# ───────────────────────────────────────────────────────────────────
SQUARE_ACCESS_TOKEN = os.environ["SQUARE_ACCESS_TOKEN"].strip()
SQUARE_WEBHOOK_KEY  = os.environ["SQUARE_WEBHOOK_KEY"].strip()

ZOHO_CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"].strip()

# ───────────────────────────────────────────────────────────────────
app = FastAPI()

# Health-check for Render
@app.get("/", status_code=200)
def root():
    return {"ok": True}

# ───────────────────────────────────────────────────────────────────
#  Utility: get fresh Zoho access-token (cached for 55 min)
# ───────────────────────────────────────────────────────────────────
_zoho_cached_token = {"token": None, "exp": 0}

def zoho_access_token() -> str:
    now = time.time()
    if _zoho_cached_token["token"] and now < _zoho_cached_token["exp"]:
        return _zoho_cached_token["token"]

    resp = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id":     ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type":    "refresh_token",
        },
        timeout=15,
    ).json()

    if "access_token" not in resp:
        raise RuntimeError(f"Zoho token error: {resp}")

    _zoho_cached_token["token"] = resp["access_token"]
    _zoho_cached_token["exp"]   = now + 55 * 60  # 55 minutes
    return _zoho_cached_token["token"]

# ───────────────────────────────────────────────────────────────────
#  Square signature verification
# ───────────────────────────────────────────────────────────────────
def verify_square_sig(body: bytes, header_sig: str) -> bool:
    mac = hmac.new(
        SQUARE_WEBHOOK_KEY.encode(),
        msg=body,
        digestmod=hashlib.sha1,
    ).hexdigest()
    return hmac.compare_digest(mac, header_sig)

# ───────────────────────────────────────────────────────────────────
#  Main webhook
# ───────────────────────────────────────────────────────────────────
@app.post("/square/webhook")
async def square_webhook(request: Request):
    body = await request.body()
    sig  = request.headers.get("x-square-signature", "")

    if not verify_square_sig(body, sig):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid Square signature")

    payload = json.loads(body)
    event   = payload["type"]

    # We only care about *booking.created*
    if event != "booking.created":
        return {"ignored": event}

    booking  = payload["data"]["object"]["booking"]
    cust_id  = booking["customer_id"]
    start_at = datetime.fromisoformat(booking["start_at"].replace("Z", "+00:00"))
    duration = booking["appointment_segments"][0]["duration_minutes"]
    end_at   = start_at + timedelta(minutes=duration)

    # ── Fetch customer details from Square ─────────────────────────
    cust_resp = requests.get(
        f"https://connect.squareup.com/v2/customers/{cust_id}",
        headers={
            "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
            "Square-Version": "2024-05-15",
        },
        timeout=15,
    ).json()

    customer = cust_resp["customer"]
    first = customer.get("given_name", "")
    last  = customer.get("family_name", "")
    email = customer.get("email_address", "")
    phone = customer.get("phone_number", "")
    # ensure +countrycode########
    phone = phone.replace(" ", "").replace("-", "")

    address = customer.get("address", {})
    street  = address.get("address_line_1", "")
    city    = address.get("locality", "")
    state   = address.get("administrative_district_level_1", "")
    zipc    = address.get("postal_code", "")

    # ── Zoho: upsert Lead / Contact by Email OR Phone ──────────────
    zhdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}",
            "Content-Type":  "application/json"}

    lead_id = None
    for module in ("Leads", "Contacts"):
        criteria = f"(Email:equals:{email})"
        if not email:
            criteria = f"(Phone:equals:{phone})"
        sr = requests.get(
            f"https://www.zohoapis.com/crm/v5/{module}/search",
            headers=zhdr,
            params={"criteria": criteria},
            timeout=15,
        ).json()

        data = sr.get("data", [])
        if data:
            lead_id = data[0]["id"]
            lead_owner = data[0]["Owner"]
            se_module  = module
            break

    if not lead_id:
        create_map = {
            "First_Name": first,
            "Last_Name":  last or "(Square)",
            "Email":      email,
            "Phone":      phone,
            "Lead_Source": "Square",
            "Street": street,
            "City":   city,
            "State":  state,
            "Zip_Code": zipc,
        }
        cr = requests.post(
            "https://www.zohoapis.com/crm/v5/Leads",
            headers=zhdr,
            json={"data": [create_map]},
            timeout=15,
        ).json()
        lead_id   = cr["data"][0]["details"]["id"]
        se_module = "Leads"
        lead_owner = {"id": cr["data"][0]["details"]["Created_By"]["id"]}

    # ── Build meeting (Event) record ───────────────────────────────
    location_name = booking.get("location_id", "")
    title = f"Himplant virtual consultation with {location_name} - {first} {last}"

    evt = {
        "Event_Title": title,
        "Start_DateTime": start_at.astimezone(timezone.utc).isoformat(),
        "End_DateTime":   end_at.astimezone(timezone.utc).isoformat(),
        "What_Id":        {"id": lead_id},
        "$se_module":     se_module,
        "Meeting_Status": "Scheduled",
        "Booking_status": booking["status"],
        "Square_Meeting_ID": booking["id"],
        "All_day": False,
    }

    requests.post(
        "https://www.zohoapis.com/crm/v5/Events",
        headers=zhdr,
        json={"data": [evt]},
        timeout=15,
    )

    return {"status": "created", "lead_id": lead_id}
# ------------------------------------------------------------------
