import os
import hmac
import base64
import hashlib
import logging
import asyncio
from typing import Optional, Dict, Any, Tuple

import requests
from fastapi import FastAPI, Request, Header, HTTPException
from dotenv import load_dotenv

load_dotenv()

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("square-zoho-bridge")

# -------------------- FastAPI --------------------
app = FastAPI()

# -------------------- ENV --------------------
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()  # e.g. https://square-to-zoho-crm.onrender.com/square/webhook

ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com").rstrip("/")
ZOHO_CRM_BASE = os.getenv("ZOHO_CRM_BASE", "https://www.zohoapis.com").rstrip("/")

EVENT_MODULE = os.getenv("EVENT_MODULE", "Events").strip()
EVENT_EXT_ID_FIELD = os.getenv("EVENT_EXT_ID_FIELD", "Square_Meeting_ID").strip()  # unique in Events
SUBJECT_FIELD = os.getenv("SUBJECT_FIELD", "Event_Title").strip()

DEFAULT_PIPELINE = os.getenv("DEFAULT_PIPELINE", "Himplant").strip()
DEFAULT_DEAL_STAGE = os.getenv("DEFAULT_DEAL_STAGE", "Consultation Scheduled").strip()
CANCELED_DEAL_STAGE = os.getenv("CANCELED_DEAL_STAGE", "Closed Lost").strip()

# Prefer to upsert Deals using a unique Square field on Deals as well
DEAL_EXT_ID_FIELD = os.getenv("DEAL_EXT_ID_FIELD", "").strip()   # e.g. "Square_Meeting_ID" if you created it on Deals
DEAL_PHONE_FIELD = os.getenv("DEAL_PHONE_FIELD", "Phone").strip()  # your Deals phone field API name

CREATE_CONTACT_IF_NOT_FOUND = os.getenv("CREATE_CONTACT_IF_NOT_FOUND", "true").lower() == "true"

# -------------------- Helpers --------------------
def normalize_phone(phone: Optional[str]) -> str:
    """Return best-effort E.164 like +15551234567 (no spaces)."""
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit())
    if not digits:
        return ""
    if phone.strip().startswith("+"):
        return "+" + digits.lstrip("0")
    if len(digits) == 10:
        return "+1" + digits
    return "+" + digits

def phones_equal(a: str, b: str) -> bool:
    return "".join(c for c in a if c.isdigit()) == "".join(c for c in b if c.isdigit())

def split_name(first: Optional[str], last: Optional[str]) -> Tuple[str, str]:
    return (first or "").strip(), (last or "").strip()

def is_valid_webhook_event_signature(body: str, signature: str, signature_key: str, notification_url: str) -> bool:
    """
    Square signature = base64(HMAC_SHA1(key, notification_url + body))
    Keep parity with your earlier working build.
    """
    if not (signature and signature_key and notification_url):
        return False
    try:
        message = (notification_url + body).encode("utf-8")
        digest = hmac.new(signature_key.encode("utf-8"), message, hashlib.sha1).digest()
        expected = base64.b64encode(digest).decode("utf-8").strip()
        return hmac.compare_digest(expected, signature.strip())
    except Exception:
        return False

# -------------------- Square --------------------
def square_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"}

async def square_get_booking(booking_id_raw: str) -> Optional[Dict[str, Any]]:
    """
    Strip ':version' suffix and retry small backoff for eventual consistency (404s right after event).
    """
    booking_id = (booking_id_raw or "").split(":")[0]
    url = f"https://connect.squareup.com/v2/bookings/{booking_id}"
    last_text = ""
    for attempt in range(4):
        resp = requests.get(url, headers=square_headers(), timeout=20)
        if resp.status_code == 200:
            return resp.json().get("booking", {})
        last_text = resp.text
        if resp.status_code == 404:
            await asyncio.sleep(0.5 + 0.4 * attempt)
            continue
        log.error("Square booking fetch failed (%s): %s", resp.status_code, resp.text)
        break
    log.error("Square booking fetch failed final: %s", last_text)
    return None

def square_get_customer(customer_id: str) -> Optional[Dict[str, Any]]:
    if not customer_id:
        return None
    url = f"https://connect.squareup.com/v2/customers/{customer_id}"
    resp = requests.get(url, headers=square_headers(), timeout=20)
    if resp.status_code == 200:
        return resp.json().get("customer", {})
    return None

# -------------------- Zoho --------------------
_token_cache: Dict[str, Any] = {"token": None}

def zoho_access_token() -> str:
    if _token_cache.get("token"):
        return _token_cache["token"]
    url = f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    resp = requests.post(url, data=data, timeout=25)
    if resp.status_code != 200:
        log.error("Zoho token refresh failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=500, detail="Zoho auth failed")
    tok = resp.json()["access_token"]
    _token_cache["token"] = tok
    return tok

def zoho_headers() -> Dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}", "Content-Type": "application/json"}

def zoho_search(module: str, criteria: str) -> list[dict]:
    """
    Safe search: returns [] on 204 or 400 (invalid criteria)
    """
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/search"
    params = {"criteria": criteria}
    resp = requests.get(url, headers=zoho_headers(), params=params, timeout=25)
    if resp.status_code in (204, 400):
        if resp.status_code == 400:
            log.warning("Zoho search 400 (%s): %s", module, resp.text)
        return []
    resp.raise_for_status()
    return resp.json().get("data", []) or []

def zoho_get_by_id(module: str, rec_id: str) -> Optional[dict]:
    resp = requests.get(f"{ZOHO_CRM_BASE}/crm/v2/{module}/{rec_id}", headers=zoho_headers(), timeout=25)
    if resp.status_code == 200:
        data = resp.json().get("data", [])
        return data[0] if data else None
    return None

def zoho_create(module: str, data: dict, trigger: Optional[list[str]] = None) -> dict:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}"
    payload = {"data": [data]}
    if trigger:
        url += "?" + "&".join([f"trigger%5B%5D={t}" for t in trigger])
    resp = requests.post(url, headers=zoho_headers(), json=payload, timeout=25)
    log.info("Zoho %s create HTTP %s: %s", module, resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["data"][0]

def zoho_update(module: str, rec_id: str, data: dict) -> dict:
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}/{rec_id}"
    payload = {"data": [data]}
    resp = requests.put(url, headers=zoho_headers(), json=payload, timeout=25)
    log.info("Zoho %s update HTTP %s: %s", module, resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["data"][0]

def zoho_upsert_with_unique(module: str, data: dict, duplicate_key: str) -> dict:
    """
    Upsert using Zoho's duplicate_check_fields (field must be unique in that module).
    """
    url = f"{ZOHO_CRM_BASE}/crm/v2/{module}"
    payload = {"data": [data], "duplicate_check_fields": [duplicate_key]}
    resp = requests.post(url, headers=zoho_headers(), json=payload, timeout=25)
    log.info("Zoho %s upsert HTTP %s: %s", module, resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["data"][0]

def create_task(subject: str, desc: str, who_id: Optional[str] = None) -> None:
    try:
        payload = {"Subject": subject, "Description": desc, "Status": "Not Started", "Priority": "High"}
        if who_id:
            payload["Who_Id"] = {"id": who_id} if isinstance(who_id, str) else who_id
        zoho_create("Tasks", payload, trigger=["workflow"])
    except Exception as e:
        log.warning("Task create failed: %s", e)

# -------------------- Contact Logic --------------------
def contact_search_by_email(email: str) -> Optional[dict]:
    if not email:
        return None
    res = zoho_search("Contacts", f"(Email:equals:{email.strip()})")
    return res[0] if res else None

def contact_search_by_phone(phone: str) -> Optional[dict]:
    p = normalize_phone(phone)
    if not p:
        return None
    res = zoho_search("Contacts", f"(Phone:equals:{p})")
    if res:
        return res[0]
    res = zoho_search("Contacts", f"(Mobile:equals:{p})")
    return res[0] if res else None

def ensure_contact(first: str, last: str, email: str, phone: str) -> Tuple[str, bool]:
    """
    Find by email, then phone. Create if not found (and create a Task to review possible duplicate).
    Always backfill missing phone/mobile and email.
    Returns (contact_id, created_flag).
    """
    # 1) email
    c = contact_search_by_email(email) if email else None
    found_by = "email" if c else None
    # 2) phone
    if not c and phone:
        c = contact_search_by_phone(phone)
        found_by = "phone" if c else None

    normalized_phone = normalize_phone(phone)

    if c:
        cid = c.get("id")
        updates = {}
        existing_phone = normalize_phone(c.get("Phone", ""))
        existing_mobile = normalize_phone(c.get("Mobile", ""))
        if normalized_phone and not phones_equal(normalized_phone, existing_phone):
            updates["Phone"] = normalized_phone
        if normalized_phone and not phones_equal(normalized_phone, existing_mobile):
            updates["Mobile"] = normalized_phone
        if email and not (c.get("Email") or "").strip():
            updates["Email"] = email.strip()
        if updates:
            try:
                zoho_update("Contacts", cid, updates)
            except Exception as e:
                log.warning("Contact update failed: %s", e)
        log.info("Matched Contacts id=%s (by %s)", cid, found_by)
        return cid, False

    # Create (let assignment rules/workflows run)
    if not CREATE_CONTACT_IF_NOT_FOUND:
        # still surface a task so someone can merge later
        create_task(
            "Review possible duplicate — new Square booking contact",
            f"Name: {first} {last}\nEmail: {email or '(none)'}\nPhone: {normalized_phone or '(none)'}",
            who_id=None,
        )
        raise HTTPException(status_code=409, detail="Contact not found and auto-create disabled")

    payload = {
        "First_Name": first or "",
        "Last_Name": last or "Patient",
    }
    if email:
        payload["Email"] = email.strip()
    if normalized_phone:
        payload["Phone"] = normalized_phone
        payload["Mobile"] = normalized_phone

    res = zoho_create("Contacts", payload, trigger=["workflow"])
    cid = res.get("details", {}).get("id") or res.get("id")
    # Create task to flag potential duplicates for human review
    create_task(
        "Review possible duplicate — new Square booking contact",
        f"Name: {first} {last}\nEmail: {email or '(none)'}\nPhone: {normalized_phone or '(none)'}",
        who_id=cid,
    )
    log.info("Created Contacts id=%s (new)", cid)
    return cid, True

def get_contact_owner_id(contact_id: str) -> Optional[str]:
    c = zoho_get_by_id("Contacts", contact_id)
    if not c:
        return None
    owner = c.get("Owner") or {}
    return owner.get("id")

# -------------------- Deal Logic --------------------
def build_deal_name(first: str, last: str, booking_id: str) -> str:
    return f"{(first or '').strip()} {(last or '').strip()} {booking_id}".strip()

def find_existing_deal(booking_id: str, deal_name: str) -> Optional[dict]:
    if DEAL_EXT_ID_FIELD:
        res = zoho_search("Deals", f"({DEAL_EXT_ID_FIELD}:equals:{booking_id})")
        if res:
            return res[0]
    res = zoho_search("Deals", f"(Deal_Name:equals:{deal_name})")
    return res[0] if res else None

def upsert_deal(contact_id: str, first: str, last: str, email: str, phone: str, booking_id: str) -> str:
    deal_name = build_deal_name(first, last, booking_id)
    existing = find_existing_deal(booking_id, deal_name)

    data = {
        "Deal_Name": deal_name,
        "Stage": DEFAULT_DEAL_STAGE,
        "Pipeline": DEFAULT_PIPELINE,
        "Contact_Name": {"id": contact_id},
    }
    if email:
        data["Email"] = email.strip()
    phone_norm = normalize_phone(phone)
    if phone_norm and DEAL_PHONE_FIELD:
        data[DEAL_PHONE_FIELD] = phone_norm
    if DEAL_EXT_ID_FIELD:
        data[DEAL_EXT_ID_FIELD] = booking_id

    if existing:
        deal_id = existing["id"]
        try:
            zoho_update("Deals", deal_id, data)
        except Exception as e:
            log.warning("Deal update failed: %s", e)
        log.info("Updated Deals id=%s (Square=%s)", deal_id, booking_id)
        return deal_id

    # New deal → align owner with Contact owner if available
    owner_id = get_contact_owner_id(contact_id)
    if owner_id:
        data["Owner"] = {"id": owner_id}

    res = zoho_create("Deals", data, trigger=["workflow"])
    deal_id = res.get("details", {}).get("id") or res.get("id")
    log.info("Created Deals id=%s (Square=%s)", deal_id, booking_id)
    return deal_id

# -------------------- Event (Meeting) Logic --------------------
def find_event_by_square(booking_id: str) -> Optional[dict]:
    try:
        res = zoho_search(EVENT_MODULE, f"({EVENT_EXT_ID_FIELD}:equals:{booking_id})")
        return res[0] if res else None
    except Exception as e:
        log.warning("Event search by Square key failed: %s", e)
        return None

def find_event_by_deal_and_time(deal_id: str, start_at: Optional[str]) -> Optional[dict]:
    try:
        if start_at:
            crit = f"(What_Id:equals:{deal_id}) and (Start_DateTime:equals:{start_at})"
            res = zoho_search(EVENT_MODULE, crit)
            if res:
                return res[0]
        res = zoho_search(EVENT_MODULE, f"(What_Id:equals:{deal_id})")
        return res[0] if res else None
    except Exception as e:
        log.warning("Event fallback search failed: %s", e)
        return None

def upsert_event(contact_id: str, deal_id: str, booking: dict, booking_id: str,
                 first: str, last: str, email: str, phone: str) -> str:
    """
    Ensure exactly one Event is present:
      1) by Square key, else
      2) by Deal + Start (or Deal only), else
      3) create new
    In all cases write the Square key, title, start/end, Who_Id, What_Id.
    """
    start_at = booking.get("start_at")
    end_at = booking.get("end_at")

    title_bits = ["Himplant Consultation Via Zoom"]
    name_part = f"{first} {last}".strip()
    if name_part:
        title_bits.append(name_part)
    if email:
        title_bits.append(email.strip())
    phone_norm = normalize_phone(phone)
    if phone_norm:
        title_bits.append(phone_norm)
    subject = " — ".join(title_bits)

    existing = find_event_by_square(booking_id) or find_event_by_deal_and_time(deal_id, start_at)

    payload = {
        SUBJECT_FIELD: subject,
        "What_Id": {"id": deal_id},
        "Who_Id": {"id": contact_id},
        "Start_DateTime": start_at,
        "End_DateTime": end_at,
        EVENT_EXT_ID_FIELD: booking_id,
    }

    if existing:
        ev_id = existing["id"]
        try:
            zoho_update(EVENT_MODULE, ev_id, payload)
            log.info("Updated %s id=%s (meeting linked)", EVENT_MODULE, ev_id)
            return ev_id
        except Exception as e:
            log.error("Event update failed (will create new): %s", e)

    res = zoho_create(EVENT_MODULE, payload, trigger=["workflow"])
    ev_id = res.get("details", {}).get("id") or res.get("id")
    log.info("Created %s id=%s (new meeting)", EVENT_MODULE, ev_id)
    return ev_id

def cancel_event_and_deal(deal_id: str, booking_id: str, first: str, last: str) -> None:
    # Move deal to canceled stage
    try:
        zoho_update("Deals", deal_id, {"Stage": CANCELED_DEAL_STAGE})
    except Exception as e:
        log.warning("Deal cancel stage update failed: %s", e)
    # Mark event title as canceled if present
    ev = find_event_by_square(booking_id)
    if ev:
        ev_id = ev["id"]
        try:
            zoho_update(EVENT_MODULE, ev_id, {SUBJECT_FIELD: f"Canceled — Himplant Consultation — {first} {last}".strip()})
        except Exception as e:
            log.warning("Event cancel title update failed: %s", e)

# -------------------- FastAPI Routes --------------------
@app.get("/", status_code=200)
def health():
    return {"status": "OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    body_bytes = await req.body()
    body_str = body_bytes.decode("utf-8", errors="ignore")

    if not x_square_signature:
        raise HTTPException(status_code=401, detail="Missing signature")
    if not is_valid_webhook_event_signature(body_str, x_square_signature, SQUARE_WEBHOOK_KEY, WEBHOOK_URL):
        raise HTTPException(status_code=401, detail="Invalid Square signature")

    try:
        payload = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_type = payload.get("type") or payload.get("event_type") or ""
    # Ignore non-booking webhooks (we still return 200)
    if not event_type.startswith("booking."):
        return {"ignored": True}

    # Square webhooks sometimes put booking id as data.id, sometimes object.id
    booking_id_raw = (
        payload.get("data", {}).get("id")
        or payload.get("data", {}).get("object", {}).get("id")
        or ""
    )
    log.info("Square event=%s booking_id=%s", event_type, booking_id_raw)

    booking = await square_get_booking(booking_id_raw)
    if not booking:
        # Acknowledge to avoid retries storm; we'll get subsequent .updated webhooks
        return {"status": "booking not available yet"}

    # Customer
    customer_id = booking.get("customer_id")
    sq_customer = square_get_customer(customer_id) if customer_id else {}

    first, last = split_name(sq_customer.get("given_name"), sq_customer.get("family_name"))
    # If Square didn't have names, try attendees (some bookings do this)
    if not (first and last):
        attendees = booking.get("attendees") or []
        if attendees:
            first = first or (attendees[0].get("given_name") or "")
            last = last or (attendees[0].get("family_name") or "")

    email = (sq_customer.get("email_address") or "").strip()
    phone = sq_customer.get("phone_number") or ""
    # Some orgs capture booking-specific phone/email under attendees too
    if not email or not phone:
        attendees = booking.get("attendees") or []
        if attendees:
            email = email or (attendees[0].get("email_address") or "").strip()
            phone = phone or (attendees[0].get("phone_number") or "")

    stable_booking_id = (booking.get("id") or booking_id_raw or "").split(":")[0]

    # Ensure Contact
    contact_id, _created = ensure_contact(first, last, email, phone)

    # Ensure Deal (one per booking)
    deal_id = upsert_deal(contact_id, first, last, email, phone, stable_booking_id)

    # Handle cancel vs upsert meeting
    if event_type == "booking.canceled":
        cancel_event_and_deal(deal_id, stable_booking_id, first, last)
        return {"status": "canceled processed"}

    # Ensure Event exists (create if missing, repair legacy)
    upsert_event(contact_id, deal_id, booking, stable_booking_id, first, last, email, phone)

    return {"status": "ok", "contact_id": contact_id, "deal_id": deal_id}
