import os
import hmac
import base64
import hashlib
import logging
import requests
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, Header, HTTPException
from dotenv import load_dotenv

# -------------------- setup --------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("square-zoho-bridge")

app = FastAPI()

# Square / Zoho env
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY", "")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://square-to-zoho-crm.onrender.com/square/webhook")

ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ZOHO_CRM_BASE      = os.getenv("ZOHO_CRM_BASE", "https://www.zohoapis.com")
ZOHO_CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")

# Events config
EVENT_MODULE       = os.getenv("EVENT_MODULE", "Events")
EVENT_EXT_ID_FIELD = os.getenv("EVENT_EXT_ID_FIELD", "Square_Meeting_ID")
SUBJECT_FIELD      = os.getenv("SUBJECT_FIELD", "Event_Title")

# Deals config
DEFAULT_PIPELINE   = os.getenv("DEFAULT_PIPELINE", "Default")
CONSULT_STAGE      = os.getenv("CONSULT_STAGE", "Consultation Scheduled")
DEAL_EXT_ID_FIELD  = os.getenv("DEAL_EXT_ID_FIELD", "Square_Meeting_ID")  # Create this field in Deals

# Optional assignment rules (IDs)
ASSIGNMENT_RULE_ID_LEADS = os.getenv("ASSIGNMENT_RULE_ID_LEADS", "").strip()
ASSIGNMENT_RULE_ID_DEALS = os.getenv("ASSIGNMENT_RULE_ID_DEALS", "").strip()

# -------------------- helpers --------------------
def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = "".join(ch for ch in phone if ch.isdigit() or ch == "+")
    if not digits:
        return ""
    if not digits.startswith("+"):
        digits = "+" + digits
    return digits

def is_valid_webhook_event_signature(body: str, signature: str, secret: str, callback_url: str) -> bool:
    # Square HMAC(base64(sha1(key, url+body))) — this matches what you already validated successfully.
    mac = hmac.new(secret.encode("utf-8"), (callback_url + body).encode("utf-8"), hashlib.sha1)
    expected = base64.b64encode(mac.digest()).decode("utf-8")
    return hmac.compare_digest(signature, expected)

# ---- Zoho OAuth ----
_access_token_cache = {}

def zoho_access_token() -> str:
    tok = _access_token_cache.get("token")
    if tok:
        return tok
    resp = requests.post(
        f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "refresh_token": ZOHO_REFRESH_TOKEN,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        log.error("Zoho token error: %s %s", resp.status_code, resp.text)
        raise HTTPException(500, "Zoho auth failed")
    tok = resp.json().get("access_token")
    if not tok:
        raise HTTPException(500, "Zoho auth failed (no access_token)")
    _access_token_cache["token"] = tok
    return tok

def zheaders(extra: dict | None = None):
    h = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    if extra:
        h.update(extra)
    return h

def zget(path, params=None):
    return requests.get(f"{ZOHO_CRM_BASE}/crm/v5/{path}", headers=zheaders(), params=params, timeout=30)

def zpost(path, payload, params=None, extra_headers=None):
    h = {"Content-Type": "application/json"}
    if extra_headers:
        h.update(extra_headers)
    return requests.post(f"{ZOHO_CRM_BASE}/crm/v5/{path}", headers=zheaders(h), json=payload, params=params, timeout=30)

def zput(path, payload, params=None):
    return requests.put(f"{ZOHO_CRM_BASE}/crm/v5/{path}", headers=zheaders({"Content-Type":"application/json"}), json=payload, params=params, timeout=30)

def pick_first_id(resp_json) -> str | None:
    try:
        data = resp_json.get("data") or []
        if data:
            return data[0]["details"]["id"] if "details" in data[0] else data[0]["id"]
    except Exception:
        pass
    return None

# -------------------- search helpers --------------------
def find_contact_by_email_then_phone(email: str, phone: str) -> dict | None:
    # 1) email exact
    if email:
        r = zget("Contacts/search", params={"criteria": f"(Email:equals:{email})"})
        if r.status_code == 200 and r.json().get("data"):
            return r.json()["data"][0]
    # 2) phone (Phone or Mobile)
    if phone:
        r = zget("Contacts/search", params={"criteria": f"(Phone:equals:{phone})or(Mobile:equals:{phone})"})
        if r.status_code == 200 and r.json().get("data"):
            return r.json()["data"][0]
    return None

def find_deal_by_square_id(meeting_id: str) -> dict | None:
    r = zget("Deals/search", params={"criteria": f"({DEAL_EXT_ID_FIELD}:equals:{meeting_id})"})
    if r.status_code == 200 and r.json().get("data"):
        return r.json()["data"][0]
    return None

def upsert_event(meeting_id: str, subject: str, start_iso: str, end_iso: str, contact_id: str | None):
    payload = {
        "data": [{
            SUBJECT_FIELD: subject,
            EVENT_EXT_ID_FIELD: meeting_id,
            "Start_DateTime": start_iso,
            "End_DateTime": end_iso,
        }],
        "duplicate_check_fields": [EVENT_EXT_ID_FIELD],
    }
    if contact_id:
        payload["data"][0]["Who_Id"] = {"id": contact_id}
    r = zpost(EVENT_MODULE, payload)
    log.info("Zoho %s upsert HTTP %s: %s", EVENT_MODULE, r.status_code, r.text)
    if r.status_code in (200, 201, 202):
        # when 202 DUPLICATE_DATA we still return the found id
        body = r.json()
        det = (body.get("data") or [{}])[0].get("details") or {}
        return det.get("id") or pick_first_id(body)
    return None

def create_or_update_deal_by_square_id(first: str, last: str, meeting_id: str, contact: dict | None) -> str | None:
    # Try to find existing Deal by Square ID
    existing = find_deal_by_square_id(meeting_id)
    deal_name = f"{(first or '').strip()} {(last or '').strip()} {meeting_id}".strip()
    data = {
        "Deal_Name": deal_name or f"Consultation {meeting_id}",
        "Stage": CONSULT_STAGE,
        "Pipeline": DEFAULT_PIPELINE,
        DEAL_EXT_ID_FIELD: meeting_id,
    }
    params = {}
    if contact:
        data["Contact_Name"] = {"id": contact["id"]}
        owner = contact.get("Owner", {}).get("id")
        if owner:
            data["Owner"] = {"id": owner}
    else:
        if ASSIGNMENT_RULE_ID_DEALS:
            params["assignment_rule_id"] = ASSIGNMENT_RULE_ID_DEALS

    if existing:
        data["id"] = existing["id"]
        r = zput("Deals", {"data": [data]})
        log.info("Zoho Deals update HTTP %s: %s", r.status_code, r.text)
        if r.status_code in (200, 202):
            return existing["id"]
        return None

    # Create
    r = zpost("Deals", {"data": [data]}, params=params)
    log.info("Zoho Deals create HTTP %s: %s", r.status_code, r.text)
    if r.status_code in (200, 201):
        return pick_first_id(r.json())
    return None

def create_lead(first: str, last: str, email: str, phone: str, mobile: str) -> str | None:
    params = {}
    if ASSIGNMENT_RULE_ID_LEADS:
        params["assignment_rule_id"] = ASSIGNMENT_RULE_ID_LEADS
    payload = {
        "data": [{
            "First_Name": first or "",
            "Last_Name": last or "Unknown",
            "Email": (email or "").lower(),
            "Phone": phone or "",
            "Mobile": mobile or ""
        }]
    }
    r = zpost("Leads", payload, params=params)
    log.info("Zoho Leads create HTTP %s: %s", r.status_code, r.text)
    if r.status_code in (200, 201):
        return pick_first_id(r.json())
    return None

def convert_lead_get_contact_id(lead_id: str) -> str | None:
    # Convert uses v2 action endpoint; response contains created/linked Contact ID
    url = f"{ZOHO_CRM_BASE}/crm/v2/Leads/{lead_id}/actions/convert"
    payload = {"data": [{"overwrite": True, "notify_lead_owner": False, "notify_new_entity_owner": False}]}
    r = requests.post(url, headers=zheaders({"Content-Type":"application/json"}), json=payload, timeout=30)
    log.info("Zoho Lead convert HTTP %s: %s", r.status_code, r.text)
    if r.status_code not in (200, 201):
        return None
    try:
        conv = r.json()["data"][0]["details"]
        # contact id is in "contacts": [{"id": "..."}] or "Contact" key depending on org
        if "contacts" in conv and conv["contacts"]:
            return conv["contacts"][0]["id"]
        return conv.get("Contact")
    except Exception:
        return None

def create_dedupe_task_on_lead(lead_id: str, first: str, last: str, email: str, phone: str, meeting_id: str):
    subject = "Review: Possible duplicate from Square booking"
    desc = f"""Potential duplicate. Please review details from Square:
Full Name: {first} {last}
Email: {email or '(none)'}
Phone/Mobile: {phone or '(none)'}
Square Meeting ID: {meeting_id}
"""
    due_date = (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat()  # YYYY-MM-DD
    payload = {"data": [{
        "Subject": subject,
        "Description": desc,
        "Due_Date": due_date,
        "What_Id": {"id": lead_id},  # relate to the Lead
        "Status": "Not Started",
        "Priority": "High",
    }]}
    r = zpost("Tasks", payload)
    log.info("Zoho Tasks create HTTP %s: %s", r.status_code, r.text)

# -------------------- FastAPI endpoints --------------------
@app.get("/", status_code=200)
def root():
    return {"status": "OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    body_bytes = await req.body()
    body_str = body_bytes.decode("utf-8")

    if not x_square_signature:
        raise HTTPException(status_code=401, detail="No Square signature provided")
    if not SQUARE_WEBHOOK_KEY:
        raise HTTPException(status_code=500, detail="Webhook key not configured")

    if not is_valid_webhook_event_signature(body_str, x_square_signature, SQUARE_WEBHOOK_KEY, WEBHOOK_URL):
        log.warning("Invalid Square webhook signature")
        raise HTTPException(status_code=401, detail="Invalid Square Webhook Key")

    payload = await req.json()
    event_type = payload.get("type")
    booking_id = payload.get("data", {}).get("id")
    log.info("Square event=%s booking_id=%s", event_type, booking_id)

    if event_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": True}

    # fetch booking from Square
    br = requests.get(
        f"https://connect.squareup.com/v2/bookings/{booking_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
        timeout=30,
    )
    if br.status_code != 200:
        log.error("Square booking fetch failed: %s %s", br.status_code, br.text)
        raise HTTPException(status_code=500, detail="Square booking fetch failed")
    booking = br.json().get("booking", {})
    customer_id = booking.get("customer_id")

    # customer details
    first_name = last_name = email = ""
    phone_raw = ""
    if customer_id:
        cr = requests.get(
            f"https://connect.squareup.com/v2/customers/{customer_id}",
            headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"},
            timeout=30,
        )
        if cr.status_code == 200:
            c = cr.json().get("customer", {})
            first_name = (c.get("given_name") or "").strip()
            last_name  = (c.get("family_name") or "").strip()
            email = (c.get("email_address") or "").strip().lower()
            phone_raw = c.get("phone_number") or ""
        else:
            log.warning("Square customer fetch failed: %s %s", cr.status_code, cr.text)

    # normalize phones
    phone = normalize_phone(phone_raw)
    mobile = phone

    # timing fields
    start_at = booking.get("start_at") or datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    duration_min = (booking.get("appointment_segments") or [{}])[0].get("duration_minutes") or 15
    try:
        sdt = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
    except Exception:
        sdt = datetime.now(timezone.utc)
    edt = sdt + timedelta(minutes=int(duration_min))
    start_iso = sdt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    end_iso   = edt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

    # ---------- always connect Deal to a Contact (create/convert if needed) ----------
    contact = None

    # If a Deal with this Square ID already exists and has a Contact, reuse that Contact
    existing_deal = find_deal_by_square_id(booking_id)
    if existing_deal and existing_deal.get("Contact_Name", {}).get("id"):
        contact_id = existing_deal["Contact_Name"]["id"]
        # fetch that contact to get owner for deal sync
        rc = zget(f"Contacts/{contact_id}")
        if rc.status_code == 200 and rc.json().get("data"):
            contact = rc.json()["data"][0]

    # Otherwise: try Contact by email first, then phone
    if not contact:
        contact = find_contact_by_email_then_phone(email, phone)

    created_lead = None
    created_task = False
    # If still no Contact, create a Lead (runs assignment rule), then convert → get Contact
    if not contact:
        created_lead = create_lead(first_name, last_name, email, phone, mobile)
        if created_lead:
            # Create review task only when there was no match
            create_dedupe_task_on_lead(created_lead, first_name, last_name, email, phone, booking_id)
            created_task = True
            contact_id = convert_lead_get_contact_id(created_lead)
            if contact_id:
                rc = zget(f"Contacts/{contact_id}")
                if rc.status_code == 200 and rc.json().get("data"):
                    contact = rc.json()["data"][0]
        # If convert failed for some reason, we still proceed with no contact (but we'll try to avoid this path)
        if not contact:
            log.warning("Lead created but conversion to Contact failed; proceeding without Contact link temporarily.")

    # Upsert Event (always), link to Contact if we have it
    subject = f"Consultation — Square Booking {booking_id}"
    event_id = upsert_event(booking_id, subject, start_iso, end_iso, contact["id"] if contact else None)

    # Create or Update Deal by Square ID, always; link to Contact if we have it (owner copied)
    deal_id = create_or_update_deal_by_square_id(first_name, last_name, booking_id, contact)

    # Done
    return {
        "status": "ok",
        "booking_id": booking_id,
        "event_id": event_id,
        "deal_id": deal_id,
        "matched_contact": bool(contact and not created_lead),
        "made_lead": bool(created_lead),
        "made_task": created_task
    }
