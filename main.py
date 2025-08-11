import os
import hmac
import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests
from fastapi import FastAPI, Request, HTTPException

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("square-zoho-bridge")
DEBUG_SIGNATURE = os.getenv("DEBUG_SIGNATURE", "0") == "1"

# -------------------------
# FastAPI
# -------------------------
app = FastAPI()

# -------------------------
# Env
# -------------------------
SQUARE_WEBHOOK_KEY   = (os.getenv("SQUARE_WEBHOOK_KEY") or "").strip()
SQUARE_ACCESS_TOKEN  = (os.getenv("SQUARE_ACCESS_TOKEN") or "").strip()
WEBHOOK_URL          = (os.getenv("WEBHOOK_URL") or "").strip()  # MUST exactly match URL set in Square

ZOHO_CLIENT_ID       = (os.getenv("ZOHO_CLIENT_ID") or "").strip()
ZOHO_CLIENT_SECRET   = (os.getenv("ZOHO_CLIENT_SECRET") or "").strip()
ZOHO_REFRESH_TOKEN   = (os.getenv("ZOHO_REFRESH_TOKEN") or "").strip()
ZOHO_ACCOUNTS_BASE   = (os.getenv("ZOHO_ACCOUNTS_BASE") or "https://accounts.zoho.com").strip()
ZOHO_CRM_BASE        = (os.getenv("ZOHO_CRM_BASE") or "https://www.zohoapis.com").strip()

DEFAULT_PIPELINE     = (os.getenv("DEFAULT_PIPELINE") or "Default").strip()
DEFAULT_DEAL_STAGE   = (os.getenv("DEFAULT_DEAL_STAGE") or "Qualification").strip()
CANCELED_DEAL_STAGE  = (os.getenv("CANCELED_DEAL_STAGE") or "Closed Lost").strip()

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
        dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = dt + timedelta(minutes=15)
        return end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception as e:
        logger.warning(f"ensure_end_15min failed: {e}")
        return None

def _safe_tail(s: str, n: int = 6) -> str:
    return s[:6] + "..." + s[-6:] if len(s) > 12 else s

def get_square_signature_header(req: Request) -> Optional[str]:
    """
    Square currently sends HMAC-SHA256 signature in header:
      x-square-hmacsha256-signature
    Some older examples show x-square-signature. Support both.
    """
    # FastAPI lower-cases header names
    h = req.headers.get("x-square-hmacsha256-signature") or req.headers.get("x-square-signature")
    return h

def verify_square_signature(raw_body: bytes, provided_sig: str) -> bool:
    """
    Signature = Base64( HMAC_SHA256( WEBHOOK_SIGNATURE_KEY, WEBHOOK_URL + rawBody ) )
    Both WEBHOOK_URL and rawBody must match exactly what Square used.
    """
    if not SQUARE_WEBHOOK_KEY or not WEBHOOK_URL or not provided_sig:
        if DEBUG_SIGNATURE:
            logger.error("Signature precheck failed. Have key=%s url=%s header=%s",
                         bool(SQUARE_WEBHOOK_KEY), bool(WEBHOOK_URL), bool(provided_sig))
        return False

    message = (WEBHOOK_URL + raw_body.decode("utf-8")).encode("utf-8")
    digest = hmac.new(SQUARE_WEBHOOK_KEY.encode("utf-8"), message, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")

    ok = hmac.compare_digest(expected, provided_sig)

    if DEBUG_SIGNATURE:
        logger.info("Sig debug → URL: %s", WEBHOOK_URL)
        logger.info("Sig debug → Key (masked): %s", _safe_tail(SQUARE_WEBHOOK_KEY))
        logger.info("Sig debug → Provided: %s", provided_sig)
        logger.info("Sig debug → Expected: %s", expected)
        logger.info("Sig debug → Match: %s", ok)

    return ok

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

def zoho_search_contact_or_lead(access_token: str, email: Optional[str], phone: Optional[str]) -> Tuple[Optional[str], Optional[dict]]:
    email = (email or "").strip()
    phone = normalize_phone(phone)

    # Contacts
    if email:
        rec = zoho_search_module(access_token, "Contacts", f"(Email:equals:{email})")
        if rec: return "Contacts", rec
    if phone:
        for field in ("Phone", "Mobile"):
            rec = zoho_search_module(access_token, "Contacts", f"({field}:equals:{phone})")
            if rec: return "Contacts", rec

    # Leads
    if email:
        rec = zoho_search_module(access_token, "Leads", f"(Email:equals:{email})")
        if rec: return "Leads", rec
    if phone:
        for field in ("Phone", "Mobile"):
            rec = zoho_search_module(access_token, "Leads", f"({field}:equals:{phone})")
            if rec: return "Leads", rec

    return None, None

# -------------------------
# Zoho CRUD
# -------------------------
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
    ev = zoho_sea_
