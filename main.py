import os
import hmac
import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests
from fastapi import FastAPI, Request, HTTPException

# ===== Logging =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("square-zoho-bridge")
DEBUG_SIGNATURE = os.getenv("DEBUG_SIGNATURE", "0") == "1"

# ===== App =====
app = FastAPI()

# ===== Env =====
SQUARE_WEBHOOK_KEY   = (os.getenv("SQUARE_WEBHOOK_KEY") or "").strip()
SQUARE_ACCESS_TOKEN  = (os.getenv("SQUARE_ACCESS_TOKEN") or "").strip()
WEBHOOK_URL          = (os.getenv("WEBHOOK_URL") or "").strip()  # MUST match Square subscription URL exactly

ZOHO_CLIENT_ID       = (os.getenv("ZOHO_CLIENT_ID") or "").strip()
ZOHO_CLIENT_SECRET   = (os.getenv("ZOHO_CLIENT_SECRET") or "").strip()
ZOHO_REFRESH_TOKEN   = (os.getenv("ZOHO_REFRESH_TOKEN") or "").strip()
ZOHO_ACCOUNTS_BASE   = (os.getenv("ZOHO_ACCOUNTS_BASE") or "https://accounts.zoho.com").strip()
ZOHO_CRM_BASE        = (os.getenv("ZOHO_CRM_BASE") or "https://www.zohoapis.com").strip()

DEFAULT_PIPELINE     = (os.getenv("DEFAULT_PIPELINE") or "Default").strip()
DEFAULT_DEAL_STAGE   = (os.getenv("DEFAULT_DEAL_STAGE") or "Qualification").strip()
CANCELED_DEAL_STAGE  = (os.getenv("CANCELED_DEAL_STAGE") or "Closed Lost").strip()

# ===== Small helpers =====
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
    dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end = dt + timedelta(minutes=15)
    return end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _safe_tail(s: str, n: int = 6) -> str:
    return s[:6] + "..." + s[-6:] if len(s) > 12 else s

# ===== Signature =====
def get_signature_header(req: Request) -> Optional[str]:
    # Square uses x-square-hmacsha256-signature; support legacy x-square-signature too.
    return req.headers.get("x-square-hmacsha256-signature") or req.headers.get("x-square-signature")

def verify_square_signature(raw_body: bytes, provided_sig: str) -> bool:
    """
    expected = Base64( HMAC_SHA256( WEBHOOK_SIGNATURE_KEY, WEBHOOK_URL + raw_body ) )
    """
    if not (SQUARE_WEBHOOK_KEY and WEBHOOK_URL and provided_sig):
        if DEBUG_SIGNATURE:
            logger.error("Sig precheck fail -> key:%s url:%s header:%s",
                         bool(SQUARE_WEBHOOK_KEY), bool(WEBHOOK_URL), bool(provided_sig))
        return False
    message = (WEBHOOK_URL + raw_body.decode("utf-8")).encode("utf-8")
    digest = hmac.new(SQUARE_WEBHOOK_KEY.encode("utf-8"), message, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    ok = hmac.compare_digest(expected, provided_sig)
    if DEBUG_SIGNATURE:
        logger.info("Sig URL: %s", WEBHOOK_URL)
        logger.info("Sig key(masked): %s", _safe_tail(SQUARE_WEBHOOK_KEY))
        logger.info("Sig provided: %s", provided_sig)
        logger.info("Sig expected: %s", expected)
        logger.info("Sig match: %s", ok)
    return ok

# ===== Zoho auth & API helpers =====
def zoho_access_token() -> str:
    url = f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        logger.error("Zoho token error: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Zoho auth failed")
    return r.json()["access_token"]

def Z(token: str) -> dict:
    return {"Authorization": f"Zoho-oauthtoken {token}"}

def zoho_search_module(token: str, module: str, criteria: str) -> Optional[dict]:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/search"
    r = requests.get(url, headers=Z(token), params={"criteria": criteria}, timeout=30)
    if r.status_code == 204:
        return None
    if r.status_code != 200:
        logger.warning("Zoho search %s -> %s %s", module, r.status_code, r.text)
        return None
    data = r.json().get("data", [])
    return data[0] if data else None

def zoho_find_person(token: str, email: Optional[str], phone: Optional[str]) -> Tuple[Optional[str], Optional[dict]]:
    email = (email or "").strip()
    phone = normalize_phone(phone)
    # Contacts
    if email:
        rec = zoho_search_module(token, "Contacts", f"(Email:equals:{email})")
        if rec: return "Contacts", rec
    if phone:
        for f in ("Phone", "Mobile"):
            rec = zoho_search_module(token, "Contacts", f"({f}:equals:{phone})")
            if rec: return "Contacts", rec
    # Leads
    if email:
        rec = zoho_search_module(token, "Leads", f"(Email:equals:{email})")
        if rec: return "Leads", rec
    if phone:
        for f in ("Phone", "Mobile"):
            rec = zoho_search_module(token, "Leads", f"({f}:equals:{phone})")
            if rec: return "Leads", rec
    return None, None

def zoho_create_lead(token: str, first_name: str, last_name: str, email: str, phone: str) -> str:
    url = f"{ZOHO_CRM_BASE}/crm/v2/Leads"
    payload = {
        "data": [{
            "First_Name": first_name or "",
            "Last_Name": last_name or (email or phone or "Unknown"),
            "Email": email or "",
            "Phone": phone or ""
        }]
    }
    r = requests.post(url, headers=Z(token), json=payload, timeout=30)
    if r.status_code not in (200, 201, 202):
        logger.error("Lead create failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Lead create failed")
    return r.json()["data"][0]["details"]["id"]

def zoho_upsert_event(token: str, who_id: str, subject: str, start_iso: str, end_iso: str, desc: str = ""):
    existing = zoho_search_module(token, "Events", f"(Subject:equals:{subject})")
    url = f"{ZOHO_CRM_BASE}/crm/v2/Events"
    payload = {
        "data": [{
            "Subject": subject,
            "Start_DateTime": start_iso,
            "End_DateTime": end_iso,
            "Description": desc,
            "Who_Id": {"id": who_id},
        }]
    }
    if existing:
        payload["data"][0]["id"] = existing["id"]
        r = requests.put(url, headers=Z(token), json=payload, timeout=30)
    else:
        r = requests.post(url, headers=Z(token), json=payload, timeout=30)
    if r.status_code not in (200, 201, 202):
        logger.error("Event upsert failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Event upsert failed")

def zoho_convert_lead(token: str, lead_id: str, deal_name: str):
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
    r = requests.post(url, headers=Z(token), json=payload, timeout=30)
    if r.status_code not in (200, 201, 202):
        logger.error("Lead convert failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Lead convert failed")

def zoho_create_deal_for_contact(token: str, contact_id: str, deal_name: str):
    url = f"{ZOHO_CRM_BASE}/crm/v2/Deals"
    payload = {
        "data": [{
            "Deal_Name": deal_name,
            "Pipeline": DEFAULT_PIPELINE,
            "Stage": DEFAULT_DEAL_STAGE,
            "Contact_Name": {"id": contact_id}
        }]
    }
    r = requests.post(url, headers=Z(token), json=payload, timeout=30)
    if r.status_code not in (200, 201, 202):
        logger.error("Deal create failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Deal create failed")

def zoho_mark_event_canceled(token: str, subject: str):
    ev = zoho_search_module(token, "Events", f"(Subject:equals:{subject})")
    if not ev:
        return
    url = f"{ZOHO_CRM_BASE}/crm/v2/Events"
    new_subject = ev.get("Subject", "")
    if not new_subject.startswith("[CANCELED]"):
        new_subject = f"[CANCELED] {new_subject or subject}"
    payload = {
        "data": [{
            "id": ev["id"],
            "Subject": new_subject,
            "Description": (ev.get("Description") or "") + "\nCanceled via Square webhook."
        }]
    }
    r = requests.put(url, headers=Z(token), json=payload, timeout=30)
    if r.status_code not in (200, 202):
        logger.warning("Event cancel note failed: %s %s", r.status_code, r.text)

def zoho_search_deal_by_name(token: str, deal_name: str) -> Optional[dict]:
    return zoho_search_module(token, "Deals", f"(Deal_Name:equals:{deal_name})")

def zoho_update_deal_stage(token: str, deal_id: str, stage: str):
    url = f"{ZOHO_CRM_BASE}/crm/v2/Deals"
    payload = {"data": [{"id": deal_id, "Stage": stage}]}
    r = requests.put(url, headers=Z(token), json=payload, timeout=30)
    if r.status_code not in (200, 202):
        logger.warning("Deal stage update failed: %s %s", r.status_code, r.text)

# ===== Square helpers =====
def sq_get_booking(booking_id: str) -> dict:
    r = requests.get(
        f"https://connect.squareup.com/v2/bookings/{booking_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
        timeout=30
    )
    if r.status_code != 200:
        logger.error("Square booking fetch failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Square booking fetch failed")
    return r.json().get("booking", {})

def sq_get_customer(customer_id: str) -> dict:
    r = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
        timeout=30
    )
    if r.status_code != 200:
        logger.error("Square customer fetch failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Square customer fetch failed")
    return r.json().get("customer", {})

# ===== Routes =====
@app.get("/")
def health():
    return {"service": "square→zoho", "status": "ok"}

async def _handle_square(req: Request):
    raw = await req.body()
    sig = get_signature_header(req)
    if not sig or not verify_square_signature(raw, sig):
        raise HTTPException(401, "Invalid Square signature")

    try:
        payload = await req.json()
    except Exception as e:
        raise HTTPException(400, f"Invalid JSON: {e}")

    event_type = payload.get("type")
    data = payload.get("data", {})
    booking_id = data.get("id") or data.get("object", {}).get("id")
    logger.info("Event %s booking_id=%s", event_type, booking_id)

    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    token = zoho_access_token()
    subject = f"Consultation — Square Booking {booking_id}"

    if event_type == "booking.canceled":
        zoho_mark_event_canceled(token, subject)
        deal_name = f"Consultation — {booking_id}"
        deal = zoho_search_deal_by_name(token, deal_name)
        if deal and deal.get("id"):
            zoho_update_deal_stage(token, deal["id"], CANCELED_DEAL_STAGE)
        return {"ok": True, "event": "canceled", "booking_id": booking_id}

    # created/updated: prefer embedded booking, fallback to API
    obj = data.get("object", {}) if isinstance(data, dict) else {}
    booking = obj.get("booking") if isinstance(obj, dict) else None
    if not booking:
        try:
            booking = sq_get_booking(booking_id)
        except HTTPException:
            logger.info("Booking %s not yet available via API; acknowledging.", booking_id)
            return {"ok": True, "note": "booking_not_available_yet"}

    start_at = booking.get("start_at")
    end_at = booking.get("end_at") or ensure_end_15min(start_at)
    if not (start_at and end_at):
        logger.info("Missing start/end; acknowledging test/partial payload.")
        return {"ok": True, "note": "partial_payload"}

    cust_id = booking.get("customer_id")
    if not cust_id:
        logger.info("Missing customer_id; acknowledging.")
        return {"ok": True, "note": "no_customer"}

    customer = sq_get_customer(cust_id)
    email = (customer.get("email_address") or "").strip()
    phone = ""
    phones = customer.get("phone_numbers") or []
    if isinstance(phones, list) and phones:
        phone = phones[0].get("phone_number") or ""
    phone = normalize_phone(phone)
    first = (customer.get("given_name") or "").strip()
    last  = (customer.get("family_name") or "").strip()

    module, person = zoho_find_person(token, email, phone)
    if not person:
        who_id = zoho_create_lead(token, first, last, email, phone)
        module = "Leads"
    else:
        who_id = person["id"]

    desc = f"Square booking ID: {booking_id}\nEmail: {email}\nPhone: {phone}"
    zoho_upsert_event(token, who_id, subject, start_at, end_at, desc)

    deal_name = f"Consultation — {booking_id}"
    if module == "Leads":
        zoho_convert_lead(token, who_id, deal_name)
    else:
        if not zoho_search_deal_by_name(token, deal_name):
            zoho_create_deal_for_contact(token, who_id, deal_name)

    return {"ok": True, "booking_id": booking_id}

# Register BOTH paths to avoid trailing-slash 404s
@app.post("/square/webhook")
async def square_webhook(req: Request):
    return await _handle_square(req)

@app.post("/square/webhook/")
async def square_webhook_slash(req: Request):
    return await _handle_square(req)
