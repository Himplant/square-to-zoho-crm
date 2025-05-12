# ──────────────────────────────────────────────────────────────
#  main.py  –  Square → Zoho CRM bridge
# ──────────────────────────────────────────────────────────────
import os, time, hashlib, hmac, json, typing as t
from datetime import datetime, timedelta

import requests
from fastapi import FastAPI, Request, HTTPException

# load .env locally if the library exists; ignore on Render
try:
    from dotenv import load_dotenv; load_dotenv()
except ModuleNotFoundError:
    pass

# ─── FastAPI instance ─────────────────────────────────────────
app = FastAPI()

# ─── Config pulled from Render / .env ─────────────────────────
SQUARE_TOKEN        = os.environ["SQUARE_ACCESS_TOKEN"].strip()
SQUARE_WEBHOOK_KEY  = os.environ["SQUARE_WEBHOOK_KEY"].strip()
ZOHO_CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"].strip()
ZOHO_API_DOMAIN     = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")

# in-memory cache for Zoho access token
_ZOHO_TOKEN: tuple[str, float] | None = None  # (token, epoch-expiry)

# ─── Utility helpers ──────────────────────────────────────────
def zoho_access_token() -> str:
    """Return a fresh Zoho access token (refresh when needed)."""
    global _ZOHO_TOKEN
    if _ZOHO_TOKEN and _ZOHO_TOKEN[1] > time.time() + 60:
        return _ZOHO_TOKEN[0]

    r = requests.post(
        f"{ZOHO_API_DOMAIN}/oauth/v2/token",
        params=dict(
            refresh_token = ZOHO_REFRESH_TOKEN,
            client_id     = ZOHO_CLIENT_ID,
            client_secret = ZOHO_CLIENT_SECRET,
            grant_type    = "refresh_token",
        ),
        timeout=15,
    ).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho token error: {r}")
    _ZOHO_TOKEN = (r["access_token"], time.time() + int(r["expires_in"]) - 30)
    return _ZOHO_TOKEN[0]

def square_get(path: str) -> dict:
    """GET request to Square; returns {} on any error."""
    try:
        r = requests.get(
            f"https://connect.squareup.com{path}",
            headers={"Authorization": f"Bearer {SQUARE_TOKEN}",
                     "Accept": "application/json"},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}

def verify_square_sig(body: bytes, sig: str) -> bool:
    h = hmac.new(SQUARE_WEBHOOK_KEY.encode(), body, hashlib.sha1).hexdigest()
    return h == sig

def iso_to_zoho(start_iso: str, minutes: int) -> tuple[str, str]:
    """Convert Square ISO start + duration → Zoho UTC start/end strings."""
    st = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    en = st + timedelta(minutes=minutes)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return st.strftime(fmt), en.strftime(fmt)

def zoho_find_record(email: str) -> tuple[str | None, str]:
    """Return (record_id, module) if Lead/Contact exists, else (None, 'Leads')."""
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    cri = f"(Email:equals:{email})"
    for mod in ("Leads", "Contacts"):
        r = requests.get(f"{ZOHO_API_DOMAIN}/crm/v4/{mod}/search",
                         params={"criteria": cri}, headers=hdr, timeout=15).json()
        if r.get("data"):
            return r["data"][0]["id"], mod
    return None, "Leads"

def zoho_create_lead(email: str, first: str, last: str, phone: str) -> str:
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    body = {"data": [{
        "First_Name": first,
        "Last_Name" : last or "Square",
        "Email"     : email,
        "Phone"     : phone,
        "Lead_Source": "Square"
    }]}
    r = requests.post(f"{ZOHO_API_DOMAIN}/crm/v4/Leads",
                      headers=hdr, json=body, timeout=15).json()
    return r["data"][0]["details"]["id"]

def zoho_event_exists(square_id: str) -> bool:
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    q = f"select id from Events where Square_Meeting_ID = '{square_id}'"
    r = requests.post(f"{ZOHO_API_DOMAIN}/crm/v4/coql",
                      headers=hdr, json={"select_query": q}, timeout=15).json()
    return bool(r.get("data"))

def zoho_create_event(rec_id: str, mod: str, title: str,
                      start: str, end: str, square_id: str):
    hdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}"}
    body = {"data": [{
        "Event_Title"      : title,
        "Start_DateTime"   : start,
        "End_DateTime"     : end,
        "All_day"          : False,
        "Meeting_Status"   : "Scheduled",
        "Square_Meeting_ID": square_id,
        "$se_module"       : mod,
        "What_Id"          : {"id": rec_id},
    }]}
    requests.post(f"{ZOHO_API_DOMAIN}/crm/v4/Events",
                  headers=hdr, json=body, timeout=15)

# ─── Webhook endpoint ─────────────────────────────────────────
@app.post("/square/webhook")
async def square_webhook(req: Request):
    raw = await req.body()
    sig = req.headers.get("x-square-hmacsha256-signature", "")
    if not verify_square_sig(raw, sig):
        raise HTTPException(401, "bad signature")

    pld = json.loads(raw)
    if not pld.get("type", "").startswith("booking."):
        return {"ignored": pld.get("type")}

    bok      = pld["data"]["object"]["booking"]
    square_id = bok["id"]
    if zoho_event_exists(square_id):
        return {"status": "duplicate skipped"}

    # customer details
    cust = square_get(f"/v2/customers/{bok.get('customer_id')}")
    info = cust.get("customer", {})
    email = info.get("email_address", "")
    first = info.get("given_name", "") or "Square"
    last  = info.get("family_name", "")
    phone = info.get("phone_number", "")

    rec_id, module = zoho_find_record(email)
    if not rec_id:
        rec_id = zoho_create_lead(email, first, last, phone)
        module = "Leads"

    # meeting title & time
    loc = square_get(f"/v2/locations/{bok.get('location_id')}").get("location", {})
    title = f"Himplant Consultation – {first} {last} ({loc.get('name','Square')})"
    dur = bok["appointment_segments"][0]["duration_minutes"]
    start, end = iso_to_zoho(bok["start_at"], dur)

    zoho_create_event(rec_id, module, title, start, end, square_id)
    return {"status": "created", "record": rec_id}
