import os
import hmac
import base64
import hashlib
import logging
import time
from typing import Optional, Tuple, Dict, Any

import requests
from fastapi import FastAPI, Request, Header, HTTPException

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("square-zoho-bridge")

# ------------------------ Config / Env ------------------------
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY", "")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ZOHO_CRM_BASE = os.getenv("ZOHO_CRM_BASE", "https://www.zohoapis.com")

EVENT_MODULE = os.getenv("EVENT_MODULE", "Events")
EVENT_EXT_ID_FIELD = os.getenv("EVENT_EXT_ID_FIELD", "Square_Meeting_ID")
SUBJECT_FIELD = os.getenv("SUBJECT_FIELD", "Event_Title")

DEAL_MODULE = "Deals"
DEAL_EXT_ID_FIELD = os.getenv("DEAL_EXT_ID_FIELD", "Square_Meeting_ID")  # add this field to Deals
DEAL_PHONE_FIELD = os.getenv("DEAL_PHONE_FIELD", "Phone")                # change if your Deals phone API differs

DEFAULT_PIPELINE = os.getenv("DEFAULT_PIPELINE", "Himplant")
DEFAULT_DEAL_STAGE = os.getenv("DEFAULT_DEAL_STAGE", "Consultation Scheduled")
CANCELED_DEAL_STAGE = os.getenv("CANCELED_DEAL_STAGE", "Closed Lost")

CREATE_CONTACT_IF_NOT_FOUND = os.getenv("CREATE_CONTACT_IF_NOT_FOUND", "true").lower() == "true"

# ------------------------ App ------------------------
app = FastAPI()

# ------------------------ Utilities ------------------------
def is_valid_webhook_event_signature(body: str, signature: str, signature_key: str, notification_url: str) -> bool:
    if not signature or not signature_key or not notification_url:
        return False
    message = (notification_url + body).encode("utf-8")
    digest = hmac.new(signature_key.encode("utf-8"), message, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(signature, expected)

def parse_square_booking_id(raw_id: str) -> Tuple[str, Optional[int]]:
    if not raw_id:
        return "", None
    parts = str(raw_id).split(":")
    return parts[0], (int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None)

def extract_booking_id_from_payload(payload: dict) -> str:
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
    last = None
    for attempt in range(4):
        r = requests.get(
            f"https://connect.squareup.com/v2/bookings/{base_booking_id}",
            headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
            timeout=30,
        )
        if r.status_code == 200:
            return r
        last = r
        if r.status_code == 404:
            time.sleep(0.7 + 0.5 * attempt)
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

# ------------------------ Zoho Auth ------------------------
_token_cache = {"access_token": None, "expiry": 0}

def zoho_token() -> str:
    now = int(time.time())
    if _token_cache["access_token"] and now < _token_cache["expiry"] - 30:
        return _token_cache["access_token"]

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
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expiry"] = int(time.time()) + int(data.get("expires_in", 3600))
    return _token_cache["access_token"]

def zoho_headers() -> Dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {zoho_token()}", "Content-Type": "application/json"}

# ------------------------ Zoho helpers ------------------------
def zoho_search(module: str, criteria: str) -> Dict[str, Any]:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/search"
    r = requests.get(url, headers=zoho_headers(), params={"criteria": criteria}, timeout=30)
    if r.status_code == 204:
        return {"data": []}
    return r.json()

def zoho_create(module: str, record: Dict[str, Any]) -> requests.Response:
    return requests.post(f"{ZOHO_CRM_BASE}/crm/v2/{module}", headers=zoho_headers(), json={"data": [record]}, timeout=30)

def zoho_update(module: str, rec_id: str, record: Dict[str, Any]) -> requests.Response:
    return requests.put(f"{ZOHO_CRM_BASE}/crm/v2/{module}/{rec_id}", headers=zoho_headers(), json={"data": [record]}, timeout=30)

def upsert_event(record: Dict[str, Any]) -> Tuple[str, bool]:
    payload = {"data": [record], "duplicate_check_fields": [EVENT_EXT_ID_FIELD]}
    r = requests.post(f"{ZOHO_CRM_BASE}/crm/v2/{EVENT_MODULE}", headers=zoho_headers(), json=payload, timeout=30)
    if r.status_code in (200, 201, 202):
        node = r.json()["data"][0]
        details = node.get("details", {})
        if node.get("code") == "DUPLICATE_DATA":
            return details.get("id"), False
        return details.get("id"), True
    log.error("Event upsert failed: %s %s", r.status_code, r.text)
    raise HTTPException(status_code=500, detail="Event upsert failed")

def find_contact_by_email(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None
    resp = zoho_search("Contacts", f"(Email:equals:{email})")
    return (resp.get("data") or [None])[0]

def find_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    if not phone:
        return None
    resp = zoho_search("Contacts", f"(Phone:equals:{phone})")
    if resp.get("data"):
        return resp["data"][0]
    resp = zoho_search("Contacts", f"(Mobile:equals:{phone})")
    return (resp.get("data") or [None])[0]

def create_contact(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = zoho_create("Contacts", payload)
    if r.status_code in (200, 201):
        cid = r.json()["data"][0]["details"]["id"]
        g = requests.get(f"{ZOHO_CRM_BASE}/crm/v2/Contacts/{cid}", headers=zoho_headers(), timeout=30)
        if g.status_code == 200:
            return (g.json().get("data") or [None])[0]
        return {"id": cid}
    log.warning("Contact create failed: %s %s", r.status_code, r.text)
    return None

def ensure_contact_fields(contact: Dict[str, Any], email_norm: str, phone_norm: str, first_name: str, last_name: str) -> Dict[str, Any]:
    """If matched contact is missing phone/email, backfill; else no-op."""
    updates: Dict[str, Any] = {}
    if email_norm and not (contact.get("Email") or "").strip():
        updates["Email"] = email_norm
    existing_phone = (contact.get("Phone") or "").strip()
    existing_mobile = (contact.get("Mobile") or "").strip()
    if phone_norm and (existing_phone != phone_norm or not existing_mobile):
        updates["Phone"] = phone_norm
        updates["Mobile"] = phone_norm
    if updates:
        zoho_update("Contacts", contact["id"], updates)
        contact = requests.get(f"{ZOHO_CRM_BASE}/crm/v2/Contacts/{contact['id']}", headers=zoho_headers(), timeout=30).json()["data"][0]
    return contact

def find_existing_deal_by_square_id(square_id: str) -> Optional[str]:
    if not square_id:
        return None
    resp = zoho_search(DEAL_MODULE, f"({DEAL_EXT_ID_FIELD}:equals:{square_id})")
    data = resp.get("data") or []
    return data[0]["id"] if data else None

def create_deal(payload: Dict[str, Any]) -> str:
    r = zoho_create(DEAL_MODULE, payload)
    if r.status_code in (200, 201):
        return r.json()["data"][0]["details"]["id"]
    log.error("Deal create failed: %s %s", r.status_code, r.text)
    raise HTTPException(status_code=500, detail="Deal create failed")

def create_task(record: Dict[str, Any]) -> None:
    r = zoho_create("Tasks", record)
    if r.status_code not in (200, 201):
        log.warning("Task create failed: %s %s", r.status_code, r.text)

# ------------------------ Routes ------------------------
@app.get("/", status_code=200)
def health():
    return {"status": "OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    body_bytes = await req.body()
    body = body_bytes.decode("utf-8", errors="ignore")

    if not x_square_signature or not SQUARE_WEBHOOK_KEY or not WEBHOOK_URL:
        raise HTTPException(status_code=401, detail="Signature/Config missing")
    if not is_valid_webhook_event_signature(body, x_square_signature, SQUARE_WEBHOOK_KEY, WEBHOOK_URL):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = await req.json()
    event_type = payload.get("type")
    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    raw_booking_id = extract_booking_id_from_payload(payload)
    base_booking_id, _ = parse_square_booking_id(raw_booking_id)
    log.info("Square event=%s booking_id=%s", event_type, raw_booking_id)

    # Fetch booking with small retry to avoid 404 race
    br = fetch_square_booking_or_retry(base_booking_id)
    if br.status_code != 200:
        log.error("Square booking fetch failed: %s %s", br.status_code, br.text)
        raise HTTPException(status_code=500, detail="Square booking fetch failed")
    booking = br.json().get("booking", {})

    # Customer basics
    customer_id = booking.get("customer_id")
    first_name = ""
    last_name = ""
    email_norm = ""
    phone_norm = ""

    if customer_id:
        cr = requests.get(f"https://connect.squareup.com/v2/customers/{customer_id}",
                          headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
                          timeout=30)
        if cr.status_code == 200:
            cust = cr.json().get("customer", {})
            first_name = cust.get("given_name") or ""
            last_name = cust.get("family_name") or ""
            email_norm = (cust.get("email_address") or "").strip().lower()
            pns = cust.get("phone_numbers") or []
            if pns:
                phone_norm = normalize_phone(pns[0].get("phone_number") or "")
        else:
            log.warning("Customer fetch failed: %s %s", cr.status_code, cr.text)

    if not (first_name and last_name):
        attendee = (booking.get("attendees") or [{}])[0]
        first_name = first_name or attendee.get("given_name") or ""
        last_name  = last_name  or attendee.get("family_name") or ""
        email_norm = email_norm or (attendee.get("email_address") or "").strip().lower()
        phone_norm = phone_norm or normalize_phone(attendee.get("phone_number") or "")

    start_at = booking.get("start_at")
    end_at = booking.get("end_at")
    # Build a human label from segments
    segs = booking.get("appointment_segments") or []
    service_names = [s.get("service_variation_name") or s.get("service_variation_id") or "Consultation" for s in segs]
    service_label = ", ".join([s for s in service_names if s]) or "Consultation"

    # ---------------- Contact: email -> phone; create if needed; backfill phone/email ----------------
    contact = None
    if email_norm:
        contact = find_contact_by_email(email_norm)
    if not contact and phone_norm:
        contact = find_contact_by_phone(phone_norm)

    created_contact = False
    if not contact and CREATE_CONTACT_IF_NOT_FOUND:
        c_payload = {"First_Name": first_name or "", "Last_Name": (last_name or "Patient")}
        if email_norm: c_payload["Email"] = email_norm
        if phone_norm:
            c_payload["Phone"]  = phone_norm
            c_payload["Mobile"] = phone_norm
        contact = create_contact(c_payload)
        created_contact = bool(contact)
        # Create triage task for possible duplicate
        create_task({
            "Subject": f"Review possible duplicate — {first_name} {last_name}".strip(),
            "Description": "\n".join([
                f"Square Booking ID: {base_booking_id}",
                f"Name: {first_name} {last_name}".strip(),
                f"Email: {email_norm or '—'}",
                f"Phone: {phone_norm or '—'}",
            ]),
            "Status": "Not Started",
        })
    elif contact:
        contact = ensure_contact_fields(contact, email_norm, phone_norm, first_name, last_name)
    else:
        # Creation disabled – still surface a task
        create_task({
            "Subject": f"Review possible duplicate — {first_name} {last_name}".strip(),
            "Description": "\n".join([
                f"Square Booking ID: {base_booking_id}",
                f"Name: {first_name} {last_name}".strip(),
                f"Email: {email_norm or '—'}",
                f"Phone: {phone_norm or '—'}",
            ]),
            "Status": "Not Started",
        })

    contact_id = (contact or {}).get("id")
    owner_id = ((contact or {}).get("Owner") or {}).get("id")

    # ---------------- Deal: dedupe/update by Square ID ----------------
    deal_name = f"{(first_name + ' ' + last_name).strip() or (email_norm or phone_norm or 'Consultation')} — {base_booking_id}"
    existing_deal_id = find_existing_deal_by_square_id(base_booking_id)

    deal_payload = {
        "Deal_Name": deal_name,
        "Pipeline": DEFAULT_PIPELINE,
        "Stage": DEFAULT_DEAL_STAGE,
        DEAL_EXT_ID_FIELD: base_booking_id,  # persist Square ID on the Deal for future updates
    }
    if contact_id:
        deal_payload["Contact_Name"] = {"id": contact_id}
    if owner_id:
        deal_payload["Owner"] = {"id": owner_id}
    if email_norm:
        deal_payload["Email"] = email_norm  # keep if your Deals module has an Email field
    if DEAL_PHONE_FIELD and phone_norm:
        deal_payload[DEAL_PHONE_FIELD] = phone_norm

    if existing_deal_id:
        zoho_update(DEAL_MODULE, existing_deal_id, deal_payload)
        deal_id = existing_deal_id
        log.info("Updated existing Deal %s for Square ID %s", deal_id, base_booking_id)
    else:
        deal_id = create_deal(deal_payload)
        log.info("Created Deal %s for Square ID %s", deal_id, base_booking_id)

    # If canceled, move stage
    if event_type == "booking.canceled":
        try:
            zoho_update(DEAL_MODULE, deal_id, {"Stage": CANCELED_DEAL_STAGE})
        except Exception as e:
            log.warning("Failed to move deal to canceled stage: %s", e)

    # ---------------- Meeting (Event) creation with exact title & links ----------------
    title_bits = ["Himplant Consultation Via Zoom"]
    name_part = (f"{first_name} {last_name}").strip()
    if name_part:  title_bits.append(name_part)
    if email_norm: title_bits.append(email_norm)
    if phone_norm: title_bits.append(phone_norm)
    event_subject = " — ".join(title_bits)

    event_record = {
        SUBJECT_FIELD: event_subject,
        EVENT_EXT_ID_FIELD: base_booking_id,   # Square ID on the Event
    }
    if start_at: event_record["Start_DateTime"] = start_at
    if end_at:   event_record["End_DateTime"] = end_at
    if contact_id: event_record["Who_Id"] = {"id": contact_id}

    event_id, _ = upsert_event(event_record)

    # Link Event to the Deal (What_Id)
    try:
        zoho_update(EVENT_MODULE, event_id, {"What_Id": {"id": deal_id}})
    except Exception as e:
        log.warning("Could not relate Event to Deal: %s", e)

    return {"ok": True, "contact_id": contact_id, "deal_id": deal_id, "event_id": event_id}
