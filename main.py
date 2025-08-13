import os
import hmac
import base64
import hashlib
import logging
import time
from typing import Optional, Tuple, Dict, Any

import requests
from fastapi import FastAPI, Request, Header, HTTPException

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("square-zoho-bridge")

# -----------------------------------------------------------------------------
# Env
# -----------------------------------------------------------------------------
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY", "")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # must match Square webhook URL

ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ZOHO_CRM_BASE = os.getenv("ZOHO_CRM_BASE", "https://www.zohoapis.com")

# CRM config (already set correctly in your Render env)
EVENT_MODULE = os.getenv("EVENT_MODULE", "Events")  # Meetings module API-name is "Events"
EVENT_EXT_ID_FIELD = os.getenv("EVENT_EXT_ID_FIELD", "Square_Meeting_ID")
SUBJECT_FIELD = os.getenv("SUBJECT_FIELD", "Event_Title")  # field for subject/title in Events
DEFAULT_PIPELINE = os.getenv("DEFAULT_PIPELINE", "Default")
DEFAULT_DEAL_STAGE = os.getenv("DEFAULT_DEAL_STAGE", "Qualification")
CANCELED_DEAL_STAGE = os.getenv("CANCELED_DEAL_STAGE", "Closed Lost")

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI()


# -----------------------------------------------------------------------------
# Helpers – Square
# -----------------------------------------------------------------------------
def is_valid_webhook_event_signature(body: str, signature: str, signature_key: str, notification_url: str) -> bool:
    """
    Square signature: base64(hmac_sha1(key, notification_url + body))
    Doc: https://developer.squareup.com/docs/webhooks/validate-signatures
    """
    if not signature or not signature_key or not notification_url:
        return False
    message = (notification_url + body).encode("utf-8")
    digest = hmac.new(signature_key.encode("utf-8"), message, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(signature, expected)


def parse_square_booking_id(raw_id: str) -> Tuple[str, Optional[int]]:
    """
    Webhooks sometimes carry 'bookingId:version'. Square API wants only bookingId.
    Returns (base_id, version or None).
    """
    if not raw_id:
        return "", None
    parts = str(raw_id).split(":")
    base = parts[0]
    ver = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    return base, ver


def extract_booking_id_from_payload(payload: dict) -> str:
    """
    Square webhook shapes vary slightly. Try common spots.
    """
    data = payload.get("data") or {}
    bid = data.get("id")
    if bid:
        return bid

    # Sometimes nested object contains booking.id
    obj = data.get("object") or {}
    if isinstance(obj, dict):
        if "booking" in obj and isinstance(obj["booking"], dict) and obj["booking"].get("id"):
            return obj["booking"]["id"]
        if obj.get("id"):
            return obj.get("id")

    # Older shapes
    bid = data.get("object_id")
    return bid or ""


def fetch_square_booking_or_retry(base_booking_id: str) -> requests.Response:
    """
    Fetch booking with a tiny backoff because brand-new bookings can 404 briefly.
    """
    last = None
    for attempt in range(3):
        r = requests.get(
            f"https://connect.squareup.com/v2/bookings/{base_booking_id}",
            headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
            timeout=30,
        )
        if r.status_code == 200:
            return r
        last = r
        if r.status_code == 404:
            time.sleep(0.6 + 0.4 * attempt)  # ~0.6s, 1.0s, 1.4s
        else:
            break
    return last


def normalize_phone(phone: Optional[str]) -> str:
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit() or c == "+")
    if not digits:
        return ""
    if not digits.startswith("+"):
        digits = "+" + "".join(c for c in digits if c.isdigit())
    return digits


# -----------------------------------------------------------------------------
# Helpers – Zoho Auth
# -----------------------------------------------------------------------------
_zoho_token_cache: Dict[str, Any] = {"access_token": None, "expiry": 0}


def zoho_token() -> str:
    """
    Fetch/refresh Zoho access token using the refresh token.
    """
    now = int(time.time())
    if _zoho_token_cache["access_token"] and now < _zoho_token_cache["expiry"] - 30:
        return _zoho_token_cache["access_token"]

    r = requests.post(
        f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token",
        params={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    if r.status_code != 200:
        log.error("Zoho token refresh failed: %s %s", r.status_code, r.text)
        raise HTTPException(status_code=500, detail="Zoho auth failed")

    data = r.json()
    access = data["access_token"]
    expires_in = int(data.get("expires_in", 3600))
    _zoho_token_cache["access_token"] = access
    _zoho_token_cache["expiry"] = now + expires_in
    return access


def zoho_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Zoho-oauthtoken {zoho_token()}",
        "Content-Type": "application/json",
    }


# -----------------------------------------------------------------------------
# Helpers – Zoho API ops
# -----------------------------------------------------------------------------
def zoho_search(module: str, criteria: str) -> Dict[str, Any]:
    """
    Zoho search via criteria string.
    """
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/search"
    r = requests.get(url, headers=zoho_headers(), params={"criteria": criteria}, timeout=30)
    if r.status_code == 204:
        return {"data": []}
    return r.json()


def zoho_get(module: str, rec_id: str) -> Dict[str, Any]:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/{rec_id}"
    r = requests.get(url, headers=zoho_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def zoho_create(module: str, records: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}"
    return requests.post(url, headers=zoho_headers(), json={"data": [records]}, timeout=30)


def zoho_update(module: str, rec_id: str, records: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/{rec_id}"
    return requests.put(url, headers=zoho_headers(), json={"data": [records]}, timeout=30)


def zoho_upsert_events(record: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Create Event (Meetings) and dedupe by external field EVENT_EXT_ID_FIELD.
    We use duplicate_check_fields to get a 202 + existing id on duplicates.
    """
    payload = {
        "data": [record],
        "duplicate_check_fields": [EVENT_EXT_ID_FIELD],
    }
    url = f"{ZOHO_CRM_BASE}/crm/v2/{EVENT_MODULE}"
    r = requests.post(url, headers=zoho_headers(), json=payload, timeout=30)

    if r.status_code in (200, 201, 202):
        resp = r.json()
        details = resp["data"][0].get("details", {})
        code = resp["data"][0].get("code")
        if code == "DUPLICATE_DATA":
            # Zoho returns the existing record id in details.id
            event_id = details.get("id")
            log.info("Zoho %s create OK (duplicate), id=%s", EVENT_MODULE, event_id)
            return event_id, False
        else:
            event_id = details.get("id")
            log.info("Zoho %s create OK, id=%s", EVENT_MODULE, event_id)
            return event_id, True

    log.error("Zoho %s upsert failed: %s %s", EVENT_MODULE, r.status_code, r.text)
    raise HTTPException(status_code=500, detail="Zoho event upsert failed")


def zoho_create_deal(record: Dict[str, Any]) -> str:
    r = zoho_create("Deals", record)
    if r.status_code in (200, 201):
        resp = r.json()
        details = resp["data"][0].get("details", {})
        deal_id = details.get("id")
        log.info("Zoho Deals create OK, id=%s", deal_id)
        return deal_id

    log.error("Zoho Deals create failed: %s %s", r.status_code, r.text)
    raise HTTPException(status_code=500, detail="Zoho deal create failed")


def zoho_create_task(record: Dict[str, Any]) -> Optional[str]:
    r = zoho_create("Tasks", record)
    if r.status_code in (200, 201):
        resp = r.json()
        return resp["data"][0]["details"].get("id")
    log.warning("Zoho Task create failed: %s %s", r.status_code, r.text)
    return None


# -----------------------------------------------------------------------------
# Contact matching / creation logic
# -----------------------------------------------------------------------------
def find_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    # Zoho criteria: (Email:equals:address)
    resp = zoho_search("Contacts", f"(Email:equals:{email})")
    data = resp.get("data") or []
    return data[0] if data else None


def find_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    if not phone:
        return None
    # Try both Phone and Mobile fields (normalized)
    resp = zoho_search("Contacts", f"(Phone:equals:{phone})")
    data = resp.get("data") or []
    if data:
        return data[0]
    resp = zoho_search("Contacts", f"(Mobile:equals:{phone})")
    data = resp.get("data") or []
    return data[0] if data else None


# -----------------------------------------------------------------------------
# Root
# -----------------------------------------------------------------------------
@app.get("/", status_code=200)
def root():
    return {"status": "OK"}


# -----------------------------------------------------------------------------
# Webhook
# -----------------------------------------------------------------------------
@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    raw_body_bytes = await req.body()
    raw_body = raw_body_bytes.decode("utf-8", errors="ignore")

    # Signature verify
    if not x_square_signature:
        raise HTTPException(status_code=401, detail="Missing Square signature")
    if not SQUARE_WEBHOOK_KEY or not WEBHOOK_URL:
        log.error("SQUARE_WEBHOOK_KEY or WEBHOOK_URL not set")
        raise HTTPException(status_code=500, detail="Server not configured")

    if not is_valid_webhook_event_signature(raw_body, x_square_signature, SQUARE_WEBHOOK_KEY, WEBHOOK_URL):
        log.warning("Invalid Square webhook signature")
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = await req.json()
    event_type = payload.get("type")
    # Only process booking.* events; ignore customer.* etc
    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    raw_booking_id = extract_booking_id_from_payload(payload)
    base_booking_id, booking_version = parse_square_booking_id(raw_booking_id)
    log.info("Square event=%s booking_id=%s (base=%s, version=%s)", event_type, raw_booking_id, base_booking_id, booking_version)

    # Fetch booking from Square (robust)
    br = fetch_square_booking_or_retry(base_booking_id)
    if br.status_code != 200:
        log.error("Square booking fetch failed: %s %s", br.status_code, br.text)
        raise HTTPException(status_code=500, detail="Square booking fetch failed")

    booking = br.json().get("booking", {})
    customer_id = booking.get("customer_id")

    # Fetch Customer details (for email/phone/name)
    cust_email = ""
    cust_phone = ""
    first_name = ""
    last_name = ""

    if customer_id:
        cr = requests.get(
            f"https://connect.squareup.com/v2/customers/{customer_id}",
            headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
            timeout=30,
        )
        if cr.status_code == 200:
            cust = cr.json().get("customer", {})
            first_name = cust.get("given_name") or ""
            last_name = cust.get("family_name") or ""
            cust_email = (cust.get("email_address") or "").strip()
            # Square may store multiple phones; take primary
            phones = cust.get("phone_numbers") or []
            if phones:
                cust_phone = phones[0].get("phone_number") or ""
        else:
            log.warning("Square customer fetch failed: %s %s", cr.status_code, cr.text)

    # Fallback to booking fields if customer call didn’t return these
    if not first_name or not last_name:
        attendee = (booking.get("attendees") or [{}])[0]
        first_name = first_name or attendee.get("given_name") or ""
        last_name = last_name or attendee.get("family_name") or ""
        cust_email = cust_email or (attendee.get("email_address") or "")
        cust_phone = cust_phone or (attendee.get("phone_number") or "")

    # Normalize
    email_norm = (cust_email or "").strip().lower()
    phone_norm = normalize_phone(cust_phone)

    # Times & meta for Event/Deal
    start_at = booking.get("start_at")  # ISO
    end_at = booking.get("end_at")      # ISO
    service_names = []
    for seg in booking.get("appointment_segments") or []:
        service_names.append(seg.get("service_variation_name") or seg.get("service_variation_id") or "Consultation")
    service_label = ", ".join([s for s in service_names if s]) or "Consultation"

    # -------------------------------------------------------------------------
    # Find or prepare Contact
    # Priority: email -> phone
    # -------------------------------------------------------------------------
    contact = None
    if email_norm:
        contact = find_contact_by_email(email_norm)
    if not contact and phone_norm:
        contact = find_contact_by_phone(phone_norm)

    owner_id = None
    contact_id = None
    if contact:
        contact_id = contact.get("id")
        owner = contact.get("Owner") or {}
        owner_id = owner.get("id")
        log.info("Matched Contacts id=%s (email=%s phone=%s)", contact_id, email_norm, phone_norm)
    else:
        # No match: create Task for human duplicate review
        subject = f"Review possible duplicate — {first_name} {last_name}".strip()
        desc_lines = [
            f"Square Booking ID: {base_booking_id}",
            f"Name: {first_name} {last_name}".strip(),
            f"Email: {email_norm or '—'}",
            f"Phone: {phone_norm or '—'}",
        ]
        task_payload = {
            "Subject": subject,
            "Description": "\n".join(desc_lines),
            "Status": "Not Started",
            # Optionally put Due_Date, Priority, etc.
        }
        zoho_create_task(task_payload)

    # -------------------------------------------------------------------------
    # Upsert Event (Meetings) in Zoho via external unique field
    # -------------------------------------------------------------------------
    event_subject = f"{service_label} — Booking {base_booking_id}"
    event_record = {
        SUBJECT_FIELD: event_subject,
        EVENT_EXT_ID_FIELD: base_booking_id,  # external-id for dedupe
    }
    # If we have dates, map them
    if start_at:
        event_record["Start_DateTime"] = start_at
    if end_at:
        event_record["End_DateTime"] = end_at
    # Link Contact if known
    if contact_id:
        event_record["Who_Id"] = {"id": contact_id}

    event_id, created = zoho_upsert_events(event_record)

    # -------------------------------------------------------------------------
    # Create Deal (always), associate to Contact if present, inherit owner
    # -------------------------------------------------------------------------
    deal_name = f"{first_name} {last_name}".strip() or (email_norm or phone_norm or "Consultation")
    deal_name = f"{deal_name} — {base_booking_id}"

    deal_payload = {
        "Deal_Name": deal_name,
        "Pipeline": DEFAULT_PIPELINE,
        "Stage": DEFAULT_DEAL_STAGE,
    }
    # Link contact (Contact_Name) + inherit owner if available
    if contact_id:
        deal_payload["Contact_Name"] = {"id": contact_id}
    if owner_id:
        deal_payload["Owner"] = {"id": owner_id}

    # Include quick reference fields on Deal if you want them visible
    if email_norm:
        deal_payload["Email"] = email_norm
    if phone_norm:
        deal_payload["Phone"] = phone_norm

    deal_id = zoho_create_deal(deal_payload)

    # Attach Event to Deal ("Related To" / What_Id) if you want the linkage
    try:
        zoho_update(EVENT_MODULE, event_id, {"What_Id": {"id": deal_id}})
    except Exception as e:
        log.warning("Could not relate Event to Deal: %s", e)

    # -------------------------------------------------------------------------
    # If booking was canceled, move deal to your canceled stage
    # -------------------------------------------------------------------------
    if event_type == "booking.canceled":
        try:
            zoho_update("Deals", deal_id, {"Stage": CANCELED_DEAL_STAGE})
        except Exception as e:
            log.warning("Failed to move deal to canceled stage: %s", e)

    return {"ok": True, "event_id": event_id, "deal_id": deal_id}
