import os
import hmac
import base64
import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import requests
from fastapi import FastAPI, Request, HTTPException

# ===================== Logging =====================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("square-zoho-bridge")
DEBUG_SIGNATURE = os.getenv("DEBUG_SIGNATURE", "0") == "1"

# ===================== FastAPI app =====================
app = FastAPI()

# ===================== Environment =====================
SQUARE_WEBHOOK_KEY  = (os.getenv("SQUARE_WEBHOOK_KEY") or "").strip()
SQUARE_ACCESS_TOKEN = (os.getenv("SQUARE_ACCESS_TOKEN") or "").strip()
WEBHOOK_URL         = (os.getenv("WEBHOOK_URL") or "").strip()

ZOHO_CLIENT_ID      = (os.getenv("ZOHO_CLIENT_ID") or "").strip()
ZOHO_CLIENT_SECRET  = (os.getenv("ZOHO_CLIENT_SECRET") or "").strip()
ZOHO_REFRESH_TOKEN  = (os.getenv("ZOHO_REFRESH_TOKEN") or "").strip()
ZOHO_ACCOUNTS_BASE  = (os.getenv("ZOHO_ACCOUNTS_BASE") or "https://accounts.zoho.com").strip()
ZOHO_CRM_BASE       = (os.getenv("ZOHO_CRM_BASE") or "https://www.zohoapis.com").strip()

# CRM business defaults
DEFAULT_PIPELINE    = (os.getenv("DEFAULT_PIPELINE") or "Default").strip()
DEFAULT_DEAL_STAGE  = (os.getenv("DEFAULT_DEAL_STAGE") or "Qualification").strip()
CANCELED_DEAL_STAGE = (os.getenv("CANCELED_DEAL_STAGE") or "Closed Lost").strip()

# Events/Meetings module config
EVENT_MODULE        = (os.getenv("EVENT_MODULE") or "Events").strip()          # Zoho API module name
EVENT_EXT_ID_FIELD  = (os.getenv("EVENT_EXT_ID_FIELD") or "Square_Meeting_ID").strip()  # your unique field
SUBJECT_FIELD_ENV   = (os.getenv("SUBJECT_FIELD") or "Event_Title").strip()    # your org uses Event_Title

# Retry knobs
SQUARE_FETCH_RETRIES = int(os.getenv("SQUARE_FETCH_RETRIES", "2"))             # quick retries for race
SQUARE_FETCH_DELAY_S = float(os.getenv("SQUARE_FETCH_DELAY_S", "0.6"))

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# ===================== Small helpers =====================
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
    return s[:n] + "..." + s[-n:] if s and len(s) > 2*n else s

# ===================== Signature =====================
def get_signature_header(req: Request) -> Optional[str]:
    # Square uses either of these headers depending on version
    return req.headers.get("x-square-hmacsha256-signature") or req.headers.get("x-square-signature")

def verify_square_signature(raw_body: bytes, provided_sig: str) -> bool:
    """
    Per Square docs:
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

# ===================== Zoho auth & HTTP =====================
def zoho_access_token() -> str:
    url = f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
    }
    r = requests.post(url, data=data, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        logger.error("Zoho token error: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Zoho auth failed")
    token = r.json().get("access_token")
    if not token:
        logger.error("Zoho token missing in response: %s", r.text)
        raise HTTPException(500, "Zoho auth failed (no access_token)")
    return token

def Z(token: str) -> Dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {token}"}

def zoho_get(module: str, path_suffix: str, token: str, params: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}{path_suffix}"
    return requests.get(url, headers=Z(token), params=params, timeout=HTTP_TIMEOUT)

def zoho_post(module: str, token: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}"
    return requests.post(url, headers=Z(token), json=payload, timeout=HTTP_TIMEOUT)

def zoho_put(module: str, token: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}"
    return requests.put(url, headers=Z(token), json=payload, timeout=HTTP_TIMEOUT)

def zoho_search_module(token: str, module: str, criteria: str) -> Optional[dict]:
    """
    Returns first record for criteria or None.
    Defensive: if Zoho returns 400 INVALID_QUERY, log and return None so we fall back to create.
    """
    try:
        r = zoho_get(module, "/search", token, {"criteria": criteria})
    except Exception as e:
        logger.warning("Zoho search %s exception: %s", module, e)
        return None

    if r.status_code == 204:
        return None
    if r.status_code != 200:
        logger.warning("Zoho search %s -> %s %s", module, r.status_code, r.text)
        return None

    data = r.json().get("data", [])
    return data[0] if data else None

# ===================== Zoho records (Leads/Contacts/Deals) =====================
def zoho_find_person(token: str, email: Optional[str], phone: Optional[str]) -> Tuple[Optional[str], Optional[dict]]:
    email = (email or "").strip()
    phone = normalize_phone(phone)

    # Contacts by email / phone
    if email:
        rec = zoho_search_module(token, "Contacts", f"(Email:equals:{email})")
        if rec:
            return "Contacts", rec
    if phone:
        for f in ("Phone", "Mobile"):
            rec = zoho_search_module(token, "Contacts", f"({f}:equals:{phone})")
            if rec:
                return "Contacts", rec

    # Leads by email / phone
    if email:
        rec = zoho_search_module(token, "Leads", f"(Email:equals:{email})")
        if rec:
            return "Leads", rec
    if phone:
        for f in ("Phone", "Mobile"):
            rec = zoho_search_module(token, "Leads", f"({f}:equals:{phone})")
            if rec:
                return "Leads", rec

    return None, None

def zoho_create_lead(token: str, first_name: str, last_name: str, email: str, phone: str) -> str:
    payload = {
        "data": [{
            "First_Name": first_name or "",
            "Last_Name": last_name or (email or phone or "Unknown"),
            "Email": email or "",
            "Phone": phone or ""
        }]
    }
    r = zoho_post("Leads", token, payload)
    logger.info("Zoho Leads create HTTP %s: %s", r.status_code, r.text)
    if r.status_code not in (200, 201, 202):
        raise HTTPException(500, "Lead create failed")
    return r.json()["data"][0]["details"]["id"]

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
    r = requests.post(url, headers=Z(token), json=payload, timeout=HTTP_TIMEOUT)
    logger.info("Zoho Lead convert HTTP %s: %s", r.status_code, r.text)
    if r.status_code not in (200, 201, 202):
        raise HTTPException(500, "Lead convert failed")

def zoho_search_deal_by_name(token: str, deal_name: str) -> Optional[dict]:
    return zoho_search_module(token, "Deals", f"(Deal_Name:equals:{deal_name})")

def zoho_create_deal_for_contact(token: str, contact_id: str, deal_name: str):
    payload = {
        "data": [{
            "Deal_Name": deal_name,
            "Pipeline": DEFAULT_PIPELINE,
            "Stage": DEFAULT_DEAL_STAGE,
            "Contact_Name": {"id": contact_id}
        }]
    }
    r = zoho_post("Deals", token, payload)
    logger.info("Zoho Deals create HTTP %s: %s", r.status_code, r.text)
    if r.status_code not in (200, 201, 202):
        raise HTTPException(500, "Deal create failed")

def zoho_update_deal_stage(token: str, deal_id: str, stage: str):
    payload = {"data": [{"id": deal_id, "Stage": stage}]}
    r = zoho_put("Deals", token, payload)
    logger.info("Zoho Deals update stage HTTP %s: %s", r.status_code, r.text)
    if r.status_code not in (200, 202):
        logger.warning("Deal stage update failed: %s %s", r.status_code, r.text)

# ===================== Events/Meetings logic =====================
def _event_search(token: str, booking_id: str) -> Optional[dict]:
    criteria = f"({EVENT_EXT_ID_FIELD}:equals:{booking_id})"
    return zoho_search_module(token, EVENT_MODULE, criteria)

def _subject_keys_from_record(rec: dict) -> Tuple[str, str]:
    # Prefer the configured field; fall back if record uses another key.
    # Common variants: "Event_Title" (Meetings UI), "Subject" (classic).
    # We’ll read both to preserve existing text on cancel/update.
    subj_env = SUBJECT_FIELD_ENV
    current = rec.get(subj_env) or rec.get("Subject") or ""
    return subj_env, current

def zoho_upsert_event(token: str, who_id: str, subject: str,
                      start_iso: str, end_iso: str, desc: str, booking_id: str) -> str:
    """
    Idempotent upsert without /upsert endpoint:
      1) search by unique field
      2) PUT if exists, else POST
    With robust logging and subject field fallback (Event_Title <-> Subject).
    """
    existing = _event_search(token, booking_id)

    def _build_payload(subject_key: str, record_id: Optional[str] = None) -> Dict[str, Any]:
        data = {
            subject_key: subject,
            "Start_DateTime": start_iso,
            "End_DateTime": end_iso,
            "Description": desc,
            "Who_Id": {"id": who_id},
            EVENT_EXT_ID_FIELD: booking_id
        }
        if record_id:
            data["id"] = record_id
        return {"data": [data]}

    # Try with configured subject field first
    subject_keys_to_try = [SUBJECT_FIELD_ENV]
    if SUBJECT_FIELD_ENV != "Subject":
        subject_keys_to_try.append("Subject")

    for idx, subj_key in enumerate(subject_keys_to_try):
        payload = _build_payload(subj_key, existing.get("id") if existing else None)
        if existing:
            r = zoho_put(EVENT_MODULE, token, payload)
            action = "update"
        else:
            r = zoho_post(EVENT_MODULE, token, payload)
            action = "create"

        logger.info("Zoho %s %s HTTP %s: %s",
                    EVENT_MODULE, action, r.status_code, r.text)

        # Success
        if r.status_code in (200, 201, 202):
            try:
                data = r.json().get("data", [])[0]
                ev_id = (data.get("details") or {}).get("id")
                logger.info("Zoho %s %s OK, id=%s (subject_key=%s)",
                            EVENT_MODULE, action, ev_id, subj_key)
                return ev_id or ""
            except Exception:
                logger.warning("Zoho %s %s succeeded but no id found in: %s",
                               EVENT_MODULE, action, r.text)
                return ""

        # If first attempt failed with INVALID_DATA, try alternate subject field once
        body = {}
        try:
            body = r.json()
        except Exception:
            pass
        code = (body or {}).get("code") or ""
        if idx == 0 and r.status_code == 400 and "INVALID_DATA" in code:
            logger.warning("Retrying with alternate subject field (from %s) due to INVALID_DATA", subj_key)
            continue  # try next subject key

        # Not retriable / or already tried both
        raise HTTPException(500, f"Event {action} failed: {r.status_code} {r.text}")

    # Should not reach here
    raise HTTPException(500, "Event upsert failed after subject fallback attempts")

def zoho_mark_event_canceled(token: str, booking_id: str):
    rec = _event_search(token, booking_id)
    if not rec:
        logger.info("Cancel: no existing %s found for %s", EVENT_MODULE, booking_id)
        return

    subj_key, current_subject = _subject_keys_from_record(rec)
    new_subject = current_subject or "Consultation"
    if not new_subject.startswith("[CANCELED]"):
        new_subject = f"[CANCELED] {new_subject}"

    payload = {"data": [{"id": rec["id"], subj_key: new_subject,
                         "Description": (rec.get("Description") or "") + "\nCanceled via Square webhook."}]}
    r = zoho_put(EVENT_MODULE, token, payload)
    logger.info("Zoho %s cancel-note HTTP %s: %s", EVENT_MODULE, r.status_code, r.text)
    if r.status_code not in (200, 202):
        logger.warning("Cancel note update failed: %s %s", r.status_code, r.text)

# ===================== Square helpers =====================
def sq_get_booking(booking_id: str) -> dict:
    r = requests.get(
        f"https://connect.squareup.com/v2/bookings/{booking_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
        timeout=HTTP_TIMEOUT
    )
    if r.status_code != 200:
        logger.error("Square booking fetch failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Square booking fetch failed")
    return r.json().get("booking", {})

def sq_get_customer(customer_id: str) -> dict:
    r = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
        timeout=HTTP_TIMEOUT
    )
    if r.status_code != 200:
        logger.error("Square customer fetch failed: %s %s", r.status_code, r.text)
        raise HTTPException(500, "Square customer fetch failed")
    return r.json().get("customer", {})

# ===================== Routes =====================
@app.get("/")
def health():
    return {"service": "square→zoho", "status": "ok", "module": EVENT_MODULE, "subject_field": SUBJECT_FIELD_ENV}

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
    logger.info("Square event=%s booking_id=%s", event_type, booking_id)

    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    token = zoho_access_token()

    if event_type == "booking.canceled":
        # Mark meeting canceled & move deal to Closed Lost if present
        zoho_mark_event_canceled(token, booking_id)
        deal_name = f"Consultation — {booking_id}"
        deal = zoho_search_deal_by_name(token, deal_name)
        if deal and deal.get("id"):
            zoho_update_deal_stage(token, deal["id"], CANCELED_DEAL_STAGE)
        return {"ok": True, "event": "canceled", "booking_id": booking_id}

    # created/updated:
    # Try to read embedded object first; fallback to Square API with a short retry (race-safe).
    obj = data.get("object", {}) if isinstance(data, dict) else {}
    booking = obj.get("booking") if isinstance(obj, dict) else None

    if not booking:
        last_err = None
        for attempt in range(1, SQUARE_FETCH_RETRIES + 2):  # first try + retries
            try:
                booking = sq_get_booking(booking_id)
                break
            except HTTPException as e:
                last_err = e
                logger.info("Booking not available yet (attempt %s/%s); sleeping %.1fs",
                            attempt, SQUARE_FETCH_RETRIES + 1, SQUARE_FETCH_DELAY_S)
                time.sleep(SQUARE_FETCH_DELAY_S)
        if not booking:
            # Acknowledge so Square will retry later; don't 500 the webhook
            logger.info("Booking still not available; acking webhook. last_err=%s", last_err)
            return {"ok": True, "note": "booking_not_available_yet"}

    start_at = booking.get("start_at")
    end_at = booking.get("end_at") or ensure_end_15min(start_at)
    if not (start_at and end_at):
        logger.info("Missing start/end; acknowledging partial payload.")
        return {"ok": True, "note": "partial_payload"}

    cust_id = booking.get("customer_id")
    if not cust_id:
        logger.info("Missing customer_id; acknowledging.")
        return {"ok": True, "note": "no_customer"}

    customer = sq_get_customer(cust_id)
    email = (customer.get("email_address") or "").strip()
    # Square can return list of phone_numbers or a single phone_number
    phone = ""
    if isinstance(customer.get("phone_numbers"), list) and customer["phone_numbers"]:
        phone = customer["phone_numbers"][0].get("phone_number") or ""
    phone = phone or customer.get("phone_number") or ""
    phone = normalize_phone(phone)

    first = (customer.get("given_name") or "").strip()
    last  = (customer.get("family_name") or "").strip()

    module, person = zoho_find_person(token, email, phone)
    if not person:
        who_id = zoho_create_lead(token, first, last, email, phone)
        module = "Leads"
        logger.info("Created Lead id=%s (first=%s last=%s email=%s phone=%s)", who_id, first, last, email, phone)
    else:
        who_id = person["id"]
        logger.info("Matched %s id=%s (email=%s phone=%s)", module, who_id, email, phone)

    subject = f"Consultation — Square Booking {booking_id}"
    desc = f"Square booking ID: {booking_id}\nEmail: {email}\nPhone: {phone}"

    ev_id = zoho_upsert_event(token, who_id, subject, start_at, end_at, desc, booking_id)
    logger.info("Event linked to %s id=%s; event_id=%s", module or "Unknown", who_id, ev_id)

    # Deal creation/convert
    deal_name = f"Consultation — {booking_id}"
    if module == "Leads":
        zoho_convert_lead(token, who_id, deal_name)
    else:
        if not zoho_search_deal_by_name(token, deal_name):
            zoho_create_deal_for_contact(token, who_id, deal_name)

    return {"ok": True, "booking_id": booking_id, "event_id": ev_id}

# Register BOTH paths to avoid trailing-slash 404s
@app.post("/square/webhook")
async def square_webhook(req: Request):
    return await _handle_square(req)

@app.post("/square/webhook/")
async def square_webhook_slash(req: Request):
    return await _handle_square(req)
