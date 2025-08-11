import os
import hmac
import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests
from fastapi import FastAPI, Request, Header, HTTPException

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("square-zoho-bridge")

# ---------- FastAPI ----------
app = FastAPI()

# ---------- Env ----------
SQUARE_WEBHOOK_KEY   = os.getenv("SQUARE_WEBHOOK_KEY")         # Signature key from Square Webhook subscription
SQUARE_ACCESS_TOKEN  = os.getenv("SQUARE_ACCESS_TOKEN")        # Square API token (Sandbox or Production)
WEBHOOK_URL          = os.getenv("WEBHOOK_URL")                # Must EXACTLY match Square webhook URL

ZOHO_CLIENT_ID       = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET   = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN   = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ACCOUNTS_BASE   = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ZOHO_CRM_BASE        = os.getenv("ZOHO_CRM_BASE", "https://www.zohoapis.com")

DEFAULT_PIPELINE     = os.getenv("DEFAULT_PIPELINE", "Default")
DEFAULT_DEAL_STAGE   = os.getenv("DEFAULT_DEAL_STAGE", "Qualification")
CANCELED_DEAL_STAGE  = os.getenv("CANCELED_DEAL_STAGE", "Closed Lost")  # stage to use on booking.canceled

# ---------- Helpers ----------
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
        dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = dt + timedelta(minutes=15)  # 15-minute duration
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
    return hmac.compare_digest(expected, provided_sig)

# ---------- Zoho Auth ----------
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

# ---------- Zoho Search ----------
def zoho_search_contact_or_lead(access_token: str, email: Optional[str], phone: Optional[str]) -> Tuple[Optional[str], Optional[dict]]:
    """
    Search Zoho first in Contacts (email->phone), then Leads (email->phone).
    Returns (module_name, record) or (None, None) if no match.
    """
    email = (email or "").strip()
    phone = normalize_phone(phone)

    # Contacts by email
    if email:
        rec = zoho_search_module(access_token, "Contacts", f"(Email:equals:{email})")
        if rec: return "Contacts", rec
    # Contacts by phone/mobile
    if phone:
        for field in ("Phone", "Mobile"):
            rec = zoho_search_module(access_token, "Contacts", f"({field}:equals:{phone})")
            if rec: return "Contacts", rec

    # Leads by email
    if email:
        rec = zoho_search_module(access_token, "Leads", f"(Email:equals:{email})")
        if rec: return "Leads", rec
    # Leads by phone/mobile
    if phone:
        for field in ("Phone", "Mobile"):
            rec = zoho_search_module(access_token, "Leads", f"({field}:equals:{phone})")
            if rec: return "Leads", rec

    return None, None

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

# ---------- Zoho CRUD ----------
def zoho_create_lead(access_token: str, *, first_name: str, last_name: str, email: str, phone: str) -> dict:
    url = f"{ZOHO_CRM_BASE}/crm/v2/Leads"
    payload = {
        "data": [{
            "First_Name": first_name or "",
            "Last_Name": last_name or (email or phone or "Unknown"),
            "Email": email or "",
            "Phone": phone or ""
        }]
    }
    resp = requests.post(url, headers=zheaders(access_token), json=payload, timeout=30)
    if resp.status_code not in (200, 201, 202):
        logger.error(f"Zoho Lead create failed: {resp.status_code} {resp.text}")
        raise HTTPException(500, "Failed to create Lead")
    return resp.json()["data"][0]

def zoho_upsert_event(access_token: str, record_id: str, subject: str, start_iso: str, end_iso: str, description: str = "") -> dict:
    """
    Upsert Event by Subject (exact match). Links via Who_Id.
    """
    found = zoho_search_module(access_token, "Events", f"(Subject:equals:{subject})")
    payload = {
        "data": [{
            "Subject": subject,
            "Start_DateTime": start_iso,
            "End_DateTime": end_iso,
            "Description": description,
            "Who_Id": {"id": record_id},
        }]
    }
    url = f"{ZOHO_CRM_BASE}/crm/v2/Events"
    if found:
        payload["data"][0]["id"] = found.get("id")
        resp = requests.put(url, headers=zheaders(access_token), json=payload, timeout=30)
        if resp.status_code not in (200, 202):
            logger.error(f"Zoho Event update failed: {resp.status_code} {resp.text}")
            raise HTTPException(500, "Failed to update Zoho Event")
        return resp.json()
    else:
        resp = requests.post(url, headers=zheaders(access_token), json=payload, timeout=30)
        if resp.status_code not in (200, 201, 202):
            logger.error(f"Zoho Event create failed: {resp.status_code} {resp.text}")
            raise HTTPException(500, "Failed to create Zoho Event")
        return resp.json()

def zoho_mark_event_canceled(access_token: str, subject: str) -> None:
    """
    Mark Event as canceled by prefixing subject and appending to description (best-effort).
    If found, update Subject to include [CANCELED].
    """
    ev = zoho_search_module(access_token, "Events", f"(Subject:equals:{subject})")
    if not ev:
        return
    ev_id = ev.get("id")
    new_subject = f"[CANCELED] {subject}" if not ev.get("Subject", "").startswith("[CANCELED]") else ev.get("Subject")
    desc = (ev.get("Description") or "") + "\nCanceled via Square webhook."
    url = f"{ZOHO_CRM_BASE}/crm/v2/Events"
    payload = {"data": [{"id": ev_id, "Subject": new_subject, "Description": desc}]}
    resp = requests.put(url, headers=zheaders(access_token), json=payload, timeout=30)
    if resp.status_code not in (200, 202):
        logger.warning(f"Zoho Event cancel update failed: {resp.status_code} {resp.text}")

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

def zoho_search_deal_by_name(access_token: str, deal_name: str) -> Optional[dict]:
    return zoho_search_module(access_token, "Deals", f"(Deal_Name:equals:{deal_name})")

def zoho_update_deal_stage(access_token: str, deal_id: str, new_stage: str) -> None:
    url = f"{ZOHO_CRM_BASE}/crm/v2/Deals"
    payload = {"data": [{"id": deal_id, "Stage": new_stage}]}
    resp = requests.put(url, headers=zheaders(access_token), json=payload, timeout=30)
    if resp.status_code not in (200, 202):
        logger.warning(f"Zoho Deal stage update failed: {resp.status_code} {resp.text}")

# ---------- Square helpers ----------
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

# ---------- Routes ----------
@app.get("/", status_code=200)
def root():
    return {"service": "square→zoho", "status": "ok"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    # 1) Verify signature
    raw = await req.body()
    raw_str = raw.decode("utf-8")
    if not x_square_signature:
        raise HTTPException(401, "Missing x-square-signature header")
    if not verify_square_signature(raw_str, x_square_signature):
        raise HTTPException(401, "Invalid Square signature")

    # 2) Parse JSON
    try:
        payload = await req.json()
    except Exception as e:
        raise HTTPException(400, f"Invalid JSON: {e}")

    event_type = payload.get("type")
    booking_id = payload.get("data", {}).get("id")

    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    # 3) Fetch booking + (if not canceled) customer
    booking = square_get_booking(booking_id)
    # Pull times
    start_at = booking.get("start_at")
    end_at = booking.get("end_at") or ensure_end_15min(start_at)
    subject = f"Consultation — Square Booking {booking_id}"

    access_token = zoho_get_access_token()

    if event_type == "booking.canceled":
        # Mark Event canceled and set Deal stage to Closed Lost (or your custom stage)
        zoho_mark_event_canceled(access_token, subject)
        # Try to find a matching Deal and move stage
        # We built deal names using "{PersonName or 'Consultation'} — {booking_id}" on create,
        # but that's hard to reconstruct. As a fallback, search Deals by booking id in a custom way:
        # If you prefer deterministic, you can also search by subject→event→related deal via custom fields.
        # Here we best-effort: search for a Deal with the booking id suffix in name.
        possible = zoho_search_deal_by_name(access_token, f"{booking_id}")
        if possible and possible.get("id"):
            zoho_update_deal_stage(access_token, possible["id"], CANCELED_DEAL_STAGE)
        return {"ok": True, "event": "canceled_handled", "booking_id": booking_id}

    # For created/updated:
    customer_id = booking.get("customer_id")
    if not customer_id:
        raise HTTPException(400, "Booking missing customer_id")
    customer = square_get_customer(customer_id)

    # 4) Identify person
    email = (customer.get("email_address") or "").strip()
    phones = customer.get("phone_numbers") or []
    phone = ""
    if isinstance(phones, list) and phones:
        phone = phones[0].get("phone_number") or ""
    phone = normalize_phone(phone)

    module, person = zoho_search_contact_or_lead(access_token, email, phone)

    # 5) Auto-create Lead if no match
    if not person:
        first = (customer.get("given_name") or "").strip()
        last  = (customer.get("family_name") or "").strip()
        lead = zoho_create_lead(access_token, first_name=first, last_name=last, email=email, phone=phone)
        person_id = lead["details"]["id"]
        module = "Leads"
    else:
        person_id = person["id"]

    # 6) Event upsert (15-min)
    if not (start_at and end_at):
        raise HTTPException(400, "Booking missing start time")
    desc = f"Square booking ID: {booking_id}\nEmail: {email}\nPhone: {phone}"
    zoho_upsert_event(access_token, person_id, subject, start_at, end_at, desc)

    # 7) Deal handling: convert if Lead; create if Contact
    # Build a deterministic deal name using booking_id for later lookups
    # (Person name may be blank; booking_id guarantees uniqueness)
    deal_name = f"Consultation — {booking_id}"
    if module == "Leads":
        zoho_convert_lead(access_token, person_id, deal_name)
    else:
        # Avoid duplicate Deal: if it already exists by name, skip create
        existing = zoho_search_deal_by_name(access_token, deal_name)
        if not existing:
            zoho_create_deal_for_contact(access_token, person_id, deal_name)

    return {"ok": True, "booking_id": booking_id, "matched_module": module, "person_id": person_id}
