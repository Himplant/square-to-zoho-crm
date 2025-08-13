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

# CRM config (kept compatible with your working setup/screenshots)
EVENT_MODULE = os.getenv("EVENT_MODULE", "Events")                  # Meetings API name
EVENT_EXT_ID_FIELD = os.getenv("EVENT_EXT_ID_FIELD", "Square_Meeting_ID")
SUBJECT_FIELD = os.getenv("SUBJECT_FIELD", "Event_Title")           # Meetings subject field

# New defaults per your note (can still be overridden in Render env)
DEFAULT_PIPELINE = os.getenv("DEFAULT_PIPELINE", "Himplant")
DEFAULT_DEAL_STAGE = os.getenv("DEFAULT_DEAL_STAGE", "Consultation Scheduled")
CANCELED_DEAL_STAGE = os.getenv("CANCELED_DEAL_STAGE", "Closed Lost")

# If true, create a Contact when no match is found (and still create a Task for dup review)
CREATE_CONTACT_IF_NOT_FOUND = os.getenv("CREATE_CONTACT_IF_NOT_FOUND", "true").lower() == "true"

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI()

# -----------------------------------------------------------------------------
# Helpers – Square
# -----------------------------------------------------------------------------
def is_valid_webhook_event_signature(body: str, signature: str, signature_key: str, notification_url: str) -> bool:
    """Square signature: base64(hmac_sha1(key, notification_url + body))"""
    if not signature or not signature_key or not notification_url:
        return False
    message = (notification_url + body).encode("utf-8")
    digest = hmac.new(signature_key.encode("utf-8"), message, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(signature, expected)


def parse_square_booking_id(raw_id: str) -> Tuple[str, Optional[int]]:
    """Webhooks may send 'bookingId:version' – API wants only bookingId."""
    if not raw_id:
        return "", None
    parts = str(raw_id).split(":")
    base = parts[0]
    ver = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    return base, ver


def extract_booking_id_from_payload(payload: dict) -> str:
    """Try common locations for booking id in webhook payload."""
    data = payload.get("data") or {}
    bid = data.get("id")
    if bid:
        return bid

    obj = data.get("object") or {}
    if isinstance(obj, dict):
        b = obj.get("booking")
        if isinstance(b, dict) and b.get("id"):
            return b["id"]
        if obj.get("id"):
            return obj["id"]

    return data.get("object_id") or ""


def fetch_square_booking_or_retry(base_booking_id: str) -> requests.Response:
    """Small backoff to avoid immediate 404s on fresh bookings."""
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
            time.sleep(0.6 + 0.4 * attempt)
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
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/search"
    r = requests.get(url, headers=zoho_headers(), params={"criteria": criteria}, timeout=30)
    if r.status_code == 204:
        return {"data": []}
    return r.json()


def zoho_create(module: str, records: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}"
    return requests.post(url, headers=zoho_headers(), json={"data": [records]}, timeout=30)


def zoho_update(module: str, rec_id: str, records: Dict[str, Any]) -> requests.Response:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/{rec_id}"
    return requests.put(url, headers=zoho_headers(), json={"data": [records]}, timeout=30)


def zoho_upsert_events(record: Dict[str, Any]) -> Tuple[str, bool]:
    """Create Event (Meetings) deduped by EVENT_EXT_ID_FIELD."""
    payload = {
        "data": [record],
        "duplicate_check_fields": [EVENT_EXT_ID_FIELD],
    }
    url = f"{ZOHO_CRM_BASE}/crm/v2/{EVENT_MODULE}"
    r = requests.post(url, headers=zoho_headers(), json=payload, timeout=30)

    if r.status_code in (200, 201, 202):
        resp = r.json()
        node = resp["data"][0]
        details = node.get("details", {})
        code = node.get("code")
        if code == "DUPLICATE_DATA":
            event_id = details.get("id")
            log.info("Zoho %s create OK (duplicate), id=%s", EVENT_MODULE, event_id)
            return event_id, False
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
        return r.json()["data"][0]["details"].get("id")
    log.warning("Zoho Task create failed: %s %s", r.status_code, r.text)
    return None


def zoho_create_contact(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = zoho_create("Contacts", record)
    if r.status_code in (200, 201):
        node = r.json()["data"][0]
        if node.get("code") in ("SUCCESS", "SUCCESS_DUPLICATE"):
            cid = node["details"]["id"]
            # Fetch to have Owner/structure
            g = requests.get(f"{ZOHO_CRM_BASE}/crm/v2/Contacts/{cid}", headers=zoho_headers(), timeout=30)
            if g.status_code == 200:
                data = (g.json().get("data") or [None])[0]
                return data
            return {"id": cid}
    log.warning("Zoho Contact create failed: %s %s", r.status_code, r.text)
    return None


# -----------------------------------------------------------------------------
# Contact matching / creation logic
# -----------------------------------------------------------------------------
def find_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    resp = zoho_search("Contacts", f"(Email:equals:{email})")
    return (resp.get("data") or [None])[0]


def find_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    if not phone:
        return None
    resp = zoho_search("Contacts", f"(Phone:equals:{phone})")
    data = resp.get("data") or []
    if data:
        return data[0]
    resp = zoho_search("Contacts", f"(Mobile:equals:{phone})")
    return (resp.get("data") or [None])[0]


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
    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    raw_booking_id = extract_booking_id_from_payload(payload)
    base_booking_id, booking_version = parse_square_booking_id(raw_booking_id)
    log.info("Square event=%s booking_id=%s (base=%s, version=%s)", event_type, raw_booking_id, base_booking_id, booking_version)

    # Fetch booking
    br = fetch_square_booking_or_retry(base_booking_id)
    if br.status_code != 200:
        log.error("Square booking fetch failed: %s %s", br.status_code, br.text)
        raise HTTPException(status_code=500, detail="Square booking fetch failed")
    booking = br.json().get("booking", {})

    # Fetch customer (email/phone/name)
    customer_id = booking.get("customer_id")
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
            phones = cust.get("phone_numbers") or []
            if phones:
                cust_phone = phones[0].get("phone_number") or ""
        else:
            log.warning("Square customer fetch failed: %s %s", cr.status_code, cr.text)

    if not first_name or not last_name:
        attendee = (booking.get("attendees") or [{}])[0]
        first_name = first_name or attendee.get("given_name") or ""
        last_name = last_name or attendee.get("family_name") or ""
        cust_email = cust_email or (attendee.get("email_address") or "")
        cust_phone = cust_phone or (attendee.get("phone_number") or "")

    email_norm = (cust_email or "").strip().lower()
    phone_norm = normalize_phone(cust_phone)

    # Build simple service label and times
    start_at = booking.get("start_at")
    end_at = booking.get("end_at")
    service_names = []
    for seg in booking.get("appointment_segments") or []:
        service_names.append(seg.get("service_variation_name") or seg.get("service_variation_id") or "Consultation")
    service_label = ", ".join([s for s in service_names if s]) or "Consultation"

    # -------------------------------------------------------------------------
    # Find or create Contact
    # Priority: email -> phone
    # -------------------------------------------------------------------------
    contact = None
    if email_norm:
        contact = find_contact_by_email(email_norm)
    if not contact and phone_norm:
        contact = find_contact_by_phone(phone_norm)

    created_contact = False
    if not contact and CREATE_CONTACT_IF_NOT_FOUND:
        # Create a new contact (and still create a task for dup-review)
        contact_payload = {
            "First_Name": first_name or "",
            "Last_Name": last_name or (email_norm or phone_norm or "Patient"),
        }
        if email_norm:
            contact_payload["Email"] = email_norm
        if phone_norm:
            # put into both Phone and Mobile so either field can match later
            contact_payload["Phone"] = phone_norm
            contact_payload["Mobile"] = phone_norm

        contact = zoho_create_contact(contact_payload)
        created_contact = contact is not None

        # Always create a task for ops to review duplicates when we had no match
        subject = f"Review possible duplicate — {first_name} {last_name}".strip()
        desc_lines = [
            f"Square Booking ID: {base_booking_id}",
            f"Name: {first_name} {last_name}".strip(),
            f"Email: {email_norm or '—'}",
            f"Phone: {phone_norm or '—'}",
        ]
        zoho_create_task({
            "Subject": subject,
            "Description": "\n".join(desc_lines),
            "Status": "Not Started",
        })
    elif not contact:
        # If creation disabled (not our case), still leave a task
        subject = f"Review possible duplicate — {first_name} {last_name}".strip()
        desc_lines = [
            f"Square Booking ID: {base_booking_id}",
            f"Name: {first_name} {last_name}".strip(),
            f"Email: {email_norm or '—'}",
            f"Phone: {phone_norm or '—'}",
        ]
        zoho_create_task({
            "Subject": subject,
            "Description": "\n".join(desc_lines),
            "Status": "Not Started",
        })

    owner_id = None
    contact_id = None
    if contact:
        contact_id = contact.get("id")
        owner = contact.get("Owner") or {}
        owner_id = owner.get("id")
        log.info("Matched/Created Contact id=%s (email=%s phone=%s new=%s)", contact_id, email_norm, phone_norm, created_contact)

    # -------------------------------------------------------------------------
    # Upsert Meeting (Events module)
    # -------------------------------------------------------------------------
    event_subject = f"{service_label} — Booking {base_booking_id}"
    event_record = {
        SUBJECT_FIELD: event_subject,
        EVENT_EXT_ID_FIELD: base_booking_id,
    }
    if start_at:
        event_record["Start_DateTime"] = start_at
    if end_at:
        event_record["End_DateTime"] = end_at
    if contact_id:
        event_record["Who_Id"] = {"id": contact_id}

    event_id, _ = zoho_upsert_events(event_record)

    # -------------------------------------------------------------------------
    # Create Deal (always), associate to Contact, pipeline/stage per defaults
    # -------------------------------------------------------------------------
    deal_name = f"{(first_name + ' ' + last_name).strip() or (email_norm or phone_norm or 'Consultation')} — {base_booking_id}"
    deal_payload = {
        "Deal_Name": deal_name,
        "Pipeline": DEFAULT_PIPELINE,
        "Stage": DEFAULT_DEAL_STAGE,
    }
    if contact_id:
        deal_payload["Contact_Name"] = {"id": contact_id}
    if owner_id:
        deal_payload["Owner"] = {"id": owner_id}
    if email_norm:
        deal_payload["Email"] = email_norm
    if phone_norm:
        deal_payload["Phone"] = phone_norm

    deal_id = zoho_create_deal(deal_payload)

    # Relate Event to Deal
    try:
        zoho_update(EVENT_MODULE, event_id, {"What_Id": {"id": deal_id}})
    except Exception as e:
        log.warning("Could not relate Event to Deal: %s", e)

    # Move deal to canceled stage if booking canceled
    if event_type == "booking.canceled":
        try:
            zoho_update("Deals", deal_id, {"Stage": CANCELED_DEAL_STAGE})
        except Exception as e:
            log.warning("Failed to move deal to canceled stage: %s", e)

    return {"ok": True, "event_id": event_id, "deal_id": deal_id, "contact_id": contact_id}
