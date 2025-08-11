import os
import hmac
import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests
from fastapi import FastAPI, Request, Header, HTTPException

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("square-zoho-bridge")

# -------------------------
# FastAPI
# -------------------------
app = FastAPI()

# -------------------------
# Env
# -------------------------
SQUARE_WEBHOOK_KEY   = os.getenv("SQUARE_WEBHOOK_KEY")         # signature key from Square webhook subscription
SQUARE_ACCESS_TOKEN  = os.getenv("SQUARE_ACCESS_TOKEN")        # Square API access token
WEBHOOK_URL          = os.getenv("WEBHOOK_URL")                # must exactly match Square webhook URL

ZOHO_CLIENT_ID       = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET   = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN   = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ACCOUNTS_BASE   = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ZOHO_CRM_BASE        = os.getenv("ZOHO_CRM_BASE", "https://www.zohoapis.com")

DEFAULT_PIPELINE     = os.getenv("DEFAULT_PIPELINE", "Default")
DEFAULT_DEAL_STAGE   = os.getenv("DEFAULT_DEAL_STAGE", "Qualification")

# -------------------------
# Helpers
# -------------------------
def normalize_phone(phone: Optional[str]) -> str:
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit() or c == "+")
    if digits and not digits.startswith("+"):
        digits = "+" + digits
    return digits

def ensure_end_15min(start_iso: Optional[str]) -> Optional[str]:
    if not start_iso:
        return None
    try:
        # Accept both 'Z' and offset forms
        dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = dt + timedelta(minutes=15)
        return end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def verify_square_signature(raw_body: str, provided_sig: str) -> bool:
    """
    Square v2 signature verification:
    signature = Base64( HMAC_SHA256( WEBHOOK_SIGNATURE_KEY, WEBHOOK_URL + rawBody ) )
    """
    if not (SQUARE_WEBHOOK_KEY and WEBHOOK_URL and provided_sig):
        return False
    message = (WEBHOOK_URL + raw_body).encode("utf-8")
    key = SQUARE_WEBHOOK_KEY.encode("utf-8")
    digest = hmac.new(key, message, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    # Constant-time compare
    return hmac.compare_digest(expected, provided_sig)

# -------------------------
# Zoho Auth
# -------------------------
def zoho_get_access_token() -> str:
    url = f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
    }
    resp = requests.post(url, data=data, timeout=30)
    if resp.status_code != 200:
        logger.error(f"Zoho token error: {resp.status_code} {resp.text}")
        raise HTTPException(500, "Failed to fetch Zoho access token")
    return resp.json()["access_token"]

def zheaders(access_token: str) -> dict:
    return {"Authorization": f"Zoho-oauthtoken {access_token}"}

# -------------------------
# Zoho Search
# -------------------------
def zoho_search_contact_or_lead(access_token: str, email: Optional[str], phone: Optional[str]) -> Tuple[str, dict]:
    """
    Search Zoho first in Contacts (email->phone), then Leads (email->phone).
    Returns (module_name, record) where module_name is "Contacts" or "Leads".
    Raises HTTPException(404) if none found.
    """
    email = (email or "").strip()
    phone = normalize_phone(phone)

    # Contacts by email
    if email:
        rec = zoho_search_module(access_token, "Contacts", f"(Email:equals:{email})")
        if rec: return "Contacts", rec
    # Contacts by phone
    if phone:
        rec = zoho_search_module(access_token, "Contacts", f"(Phone:equals:{phone})")
        if rec: return "Contacts", rec
        rec = zoho_search_module(access_token, "Contacts", f"(Mobile:equals:{phone})")
        if rec: return "Contacts", rec

    # Leads by email
    if email:
        rec = zoho_search_module(access_token, "Leads", f"(Email:equals:{email})")
        if rec: return "Leads", rec
    # Leads by phone
    if phone:
        rec = zoho_search_module(access_token, "Leads", f"(Phone:equals:{phone})")
        if rec: return "Leads", rec
        rec = zoho_search_module(access_token, "Leads", f"(Mobile:equals:{phone})")
        if rec: return "Leads", rec

    raise HTTPException(404, "No matching Contact/Lead found in Zoho")

def zoho_search_module(access_token: str, module: str, criteria: str) -> Optional[dict]:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/search"
    params = {"criteria": criteria}
    resp = requests.get(url, headers=zheaders(access_token), params=params, timeout=30)
    if resp.status_code == 204:
        return None
    if resp.status_code != 200:
        logger.warning(f"Zoho search {module} failed: {resp.status_code} {resp.text}")
        return None
    data = resp.json().get("data", [])
    return data[0] if data else None

# -------------------------
# Zoho Events (create or update by Subject=booking id)
# -------------------------
def zoho_upsert_event(
    access_token: str,
    module_name: str,
    record_id: str,
    subject: str,
    start_iso: str,
    end_iso: str,
    description: str = "",
) -> dict:
    """
    Tries to find an existing Event by exact Subject.
    If found: updates start/end/description.
    Else: creates a new Event related to the Contact/Lead via Who_Id.
    """
    # Try to find existing Event
    found = zoho_search_module(access_token, "Events", f"(Subject:equals:{subject})")

    payload = {
        "data": [{
            "Subject": subject,
            "Start_DateTime": start_iso,
            "End_DateTime": end_iso,
            "Description": description,
            # link to person
            "Who_Id": {"id": record_id},
        }]
    }
    url_create = f"{ZOHO_CRM_BASE}/crm/v2/Events"

    if found:
        event_id = found.get("id")
        url_update = f"{ZOHO_CRM_BASE}/crm/v2/Events"
        payload["data"][0]["id"] = event_id
        resp = requests.put(url_update, headers=zheaders(access_token), json=payload, timeout=30)
        if resp.status_code not in (200, 202):
            logger.error(f"Zoho Event update failed: {resp.status_code} {resp.text}")
            raise HTTPException(500, "Failed to update Zoho Event")
        return resp.json()
    else:
        resp = requests.post(url_create, headers=zheaders(access_token), json=payload, timeout=30)
        if resp.status_code not in (200, 201, 202):
            logger.error(f"Zoho Event create failed: {resp.status_code} {resp.text}")
            raise HTTPException(500, "Failed to create Zoho Event")
        return resp.json()

# -------------------------
# Zoho Deal / Lead convert
# -------------------------
def zoho_create_deal_for_contact(access_token: str, contact_id: str, deal_name: str) -> dict:
    url = f"{ZOHO_CRM_BASE}/crm/v2/Deals"
    payload = {
        "data": [{
            "Deal_Name": deal_name,
            "Pipeline": DEFAULT_PIPELINE,
            "Stage": DEFAULT_DEAL_STAGE,
            "Contact_Name": {"id": contact_id}
        }]
    }
    resp = requests.post(url, headers=zheaders(access_token), json=payload, timeout=30)
    if resp.status_code not in (200, 201, 202):
        logger.error(f"Zoho Deal create failed: {resp.status_code} {resp.text}")
        raise HTTPException(500, "Failed to create Deal for Contact")
    return resp.json()

def zoho_convert_lead(access_token: str, lead_id: str, deal_name: str) -> dict:
    url = f"{ZOHO_CRM_BASE}/crm/v2/Leads/{lead_id}/actions/convert"
    payload = {
        "data": [{
            "overwrite": True,
            "notify_lead_owner": False,
            "notify_new_entity_owner": False,
            "Deals": {
                "Deal_Name": deal_name,
                "Pipeline": DEFAULT_PIPELINE,
                "Stage": DEFAULT_DEAL_STAGE
            }
        }]
    }
    resp = requests.post(url, headers=zheaders(access_token), json=payload, timeout=30)
    if resp.status_code not in (200, 201, 202):
        logger.error(f"Zoho Lead convert failed: {resp.status_code} {resp.text}")
        raise HTTPException(500, "Failed to convert Lead to Deal")
    return resp.json()

# -------------------------
# Square helpers
# -------------------------
def square_get_booking(booking_id: str) -> dict:
    url = f"https://connect.squareup.com/v2/bookings/{booking_id}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}, timeout=30)
    if resp.status_code != 200:
        logger.error(f"Square booking fetch failed: {resp.status_code} {resp.text}")
        raise HTTPException(500, "Failed to fetch Square booking")
    return resp.json().get("booking", {})

def square_get_customer(customer_id: str) -> dict:
    url = f"https://connect.squareup.com/v2/customers/{customer_id}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}, timeout=30)
    if resp.status_code != 200:
        logger.error(f"Square customer fetch failed: {resp.status_code} {resp.text}")
        raise HTTPException(500, "Failed to fetch Square customer")
    return resp.json().get("customer", {})

# -------------------------
# Routes
# -------------------------
@app.get("/", status_code=200)
def root():
    return {"service": "square→zoho", "status": "ok"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    # 1) Raw body + signature verification
    raw = await req.body()
    raw_str = raw.decode("utf-8")
    logger.info(f"Incoming headers: {dict(req.headers)}")
    if not x_square_signature:
        raise HTTPException(401, "Missing x-square-signature header")
    if not verify_square_signature(raw_str, x_square_signature):
        raise HTTPException(401, "Invalid Square signature")

    # 2) Parse JSON + filter event types
    try:
        payload = await req.json()
    except Exception as e:
        raise HTTPException(400, f"Invalid JSON: {e}")

    event_type = payload.get("type")
    booking_id = payload.get("data", {}).get("id")
    logger.info(f"Event {event_type} for booking {booking_id}")

    if event_type not in ("booking.created", "booking.updated"):
        # You can add booking.canceled handling here if you want
        return {"ignored": True}

    # 3) Get booking + customer from Square
    booking = square_get_booking(booking_id)
    customer_id = booking.get("customer_id")
    if not customer_id:
        raise HTTPException(400, "Booking missing customer_id")
    customer = square_get_customer(customer_id)

    # 4) Pull identifiers
    email = (customer.get("email_address") or "").strip()
    phones = customer.get("phone_numbers") or []
    phone = ""
    if isinstance(phones, list) and phones:
        phone = phones[0].get("phone_number") or ""
    phone = normalize_phone(phone)

    # 5) Prepare times (start/end). Square uses RFC3339; we ensure 15-min end.
    start_at = booking.get("start_at")
    end_at = booking.get("end_at") or ensure_end_15min(start_at)
    if not (start_at and end_at):
        raise HTTPException(400, "Booking missing start time")

    # 6) Get Zoho access token and find person (Contacts first, then Leads)
    access_token = zoho_get_access_token()
    module, person = zoho_search_contact_or_lead(access_token, email, phone)
    person_id = person["id"]
    person_name = (person.get("Full_Name") or person.get("Last_Name") or "").strip()

    # 7) Create or update a 15-minute Event
    subject = f"Consultation — Square Booking {booking_id}"
    desc = f"Square booking ID: {booking_id}\nEmail: {email}\nPhone: {phone}"
    zoho_upsert_event(access_token, module, person_id, subject, start_at, end_at, desc)

    # 8) Deal: convert lead OR create for contact
    deal_name = f"{person_name or 'Consultation'} — {booking_id}"
    if module == "Leads":
        conv = zoho_convert_lead(access_token, person_id, deal_name)
        logger.info(f"Lead converted: {conv}")
    else:
        deal = zoho_create_deal_for_contact(access_token, person_id, deal_name)
        logger.info(f"Deal created for Contact: {deal}")

    return {"ok": True, "booking_id": booking_id, "matched_module": module, "person_id": person_id}
