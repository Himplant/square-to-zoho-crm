# ---------------------  main.py  ---------------------
import os, time, random, re, hmac, hashlib, base64, json, requests
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, HTTPException

# ──────────── REQUIRED ENV VARS (Render → Environment) ────────────
SQUARE_ACCESS_TOKEN = os.environ["SQUARE_ACCESS_TOKEN"].strip()
SQUARE_WEBHOOK_KEY  = os.getenv("SQUARE_WEBHOOK_KEY", "").strip()   # optional

ZOHO_CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"].strip()

# Zoho custom-field API name that stores the Square booking-id
ZOHO_FIELD_SQUARE_ID = "Square_Meeting_ID"
# ──────────────────────────────────────────────────────────────────

SQUARE_API  = "https://connect.squareup.com/v2"
SQUARE_VER  = "2024-05-15"
ZOHO_API    = "https://www.zohoapis.com/crm/v5"
ZOHO_OAUTH  = "https://accounts.zoho.com/oauth/v2/token"

app = FastAPI()

# ───────── Render health-check endpoints ─────────
@app.get("/",  status_code=200)
def root_get():  return {"ok": True}

@app.head("/", status_code=200)
def root_head(): return
# ────────────────────────────────────────────────

# ───────── Zoho access-token cache ─────────
_ZOHO_TOKEN, _ZOHO_EXP = "", 0
def zoho_token() -> str:
    global _ZOHO_TOKEN, _ZOHO_EXP
    if _ZOHO_TOKEN and _ZOHO_EXP > time.time() + 60:
        return _ZOHO_TOKEN
    rsp = requests.post(
        ZOHO_OAUTH,
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id":     ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type":    "refresh_token",
        },
        timeout=15,
    ).json()
    if "access_token" not in rsp:
        raise RuntimeError(f"Zoho OAuth error: {rsp}")
    _ZOHO_TOKEN = rsp["access_token"]
    _ZOHO_EXP   = time.time() + int(rsp["expires_in"]) - 30
    return _ZOHO_TOKEN

def zh() -> dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {zoho_token()}"}
# ──────────────────────────────────────────

# ───────── misc helpers ─────────
PHONE_RE = re.compile(r"[^\d]+")
def clean_phone(num: str | None) -> str | None:
    return f"+{PHONE_RE.sub('', num)}" if num else None

def sq_hdr() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Square-Version": SQUARE_VER,
    }

def backoff(req_fn, *a, **k):
    for i in range(5):
        r = req_fn(*a, **k)
        if r.status_code != 429:
            return r
        time.sleep((2 ** i) + random.random())
    return r

def verify_square(body: bytes, header_sig: str) -> bool:
    """
    Square → x-square-hmacsha256-signature = base64(HMAC-SHA256(body, key))
    """
    if not SQUARE_WEBHOOK_KEY:
        return True          # no key configured → skip check
    digest = hmac.new(SQUARE_WEBHOOK_KEY.encode(), body, hashlib.sha256).digest()
    calc   = base64.b64encode(digest).decode()
    return hmac.compare_digest(calc, header_sig)
# ────────────────────────────────

# ───────── Zoho person upsert ─────────
def search(module: str, criteria: str):
    r = backoff(
        requests.get,
        f"{ZOHO_API}/{module}/search",
        headers=zh(),
        params={"criteria": criteria},
        timeout=15,
    ).json()
    return r.get("data", [])

def upsert_person(email: str, phone: str | None,
                  first: str, last: str, addr: dict):
    crit_e = f"(Email:equals:{email})" if email else ""
    crit_p = f"(Phone:equals:{PHONE_RE.sub('', phone or '')})" if phone else ""
    criteria = f"({crit_e}or{crit_p})" if crit_e and crit_p else (crit_e or crit_p)

    for mod in ("Leads", "Contacts"):
        found = search(mod, criteria)
        if found:
            rid = found[0]["id"]
            if addr and not found[0].get("Mailing_Street"):
                backoff(requests.put, f"{ZOHO_API}/{mod}",
                        headers=zh(), json={"data": [{"id": rid, **addr}]},
                        timeout=15)
            return rid, mod

    lead = {"First_Name": first, "Last_Name": last or "(Square)",
            "Email": email, "Phone": phone or "",
            "Lead_Source": "Square", **addr}
    rsp  = backoff(requests.post, f"{ZOHO_API}/Leads",
                   headers=zh(), json={"data": [lead]}, timeout=15).json()
    return rsp["data"][0]["details"]["id"], "Leads"
# ──────────────────────────────────────

def create_event(rec_id, module, sq_id, title, start, end, status, desc):
    evt = {"Event_Title": title, "Start_DateTime": start, "End_DateTime": end,
           "Meeting_Status": status, "Description": desc,
           "$se_module": module, "What_Id": rec_id,
           ZOHO_FIELD_SQUARE_ID: sq_id}
    backoff(requests.post, f"{ZOHO_API}/Events",
            headers=zh(), json={"data": [evt]}, timeout=15)

def update_event(evt_id, title, start, end, status, desc):
    patch = {"id": evt_id, "Event_Title": title, "Start_DateTime": start,
             "End_DateTime": end, "Meeting_Status": status,
             "Description": desc}
    backoff(requests.put, f"{ZOHO_API}/Events",
            headers=zh(), json={"data": [patch]}, timeout=15)

# ───────── main webhook ─────────
@app.post("/square/webhook")
async def square_webhook(request: Request):
    body = await request.body()
    if not verify_square(body, request.headers.get("x-square-hmacsha256-signature", "")):
        raise HTTPException(401, "Invalid Square signature")

    pl = json.loads(body)
    ev = pl.get("type", "")
    if not ev.startswith("booking."):
        return {"ignored": ev}

    booking = pl["data"]["object"]["booking"]
    sq_id   = booking["id"]

    # ── Customer details
    cust = requests.get(f"{SQUARE_API}/customers/{booking['customer_id']}",
                        headers=sq_hdr(), timeout=10).json()["customer"]
    email = (cust.get("email_address") or f"{sq_id}@square.local").lower()
    phone = clean_phone(cust.get("phone_number"))
    first, last = cust.get("given_name", ""), cust.get("family_name", "")

    a = cust.get("address") or {}
    addr = dict(Mailing_Street=a.get("address_line_1", ""),
                Mailing_City  =a.get("locality", ""),
                Mailing_State =a.get("administrative_district_level_1", ""),
                Mailing_Zip   =a.get("postal_code", ""),
                Country       =a.get("country", "US"))

    rec_id, module = upsert_person(email, phone, first, last, addr)

    # ── Service + location for title
    service_id = booking["appointment_segments"][0]["service_variation_id"]
    service = requests.get(f"{SQUARE_API}/catalog/object/{service_id}",
                           headers=sq_hdr(), timeout=10).json()
    service_name = service.get("object", {}).get("item_variation", {}).get("name", "Service")

    loc = requests.get(f"{SQUARE_API}/locations/{booking['location_id']}",
                       headers=sq_hdr(), timeout=10).json()["location"]
    loc_name = loc.get("name", "Location")

    title = (f"Himplant virtual consultation – {service_name} – "
             f"{loc_name} – {first} {last}").strip()

    # ── Start & End (UTC)
    start_iso = booking["start_at"]
    dur_min   = booking["appointment_segments"][0]["duration_minutes"]
    start_dt  = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_iso   = (start_dt + timedelta(minutes=dur_min)).astimezone(
                  timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Meeting status
    status_map = {"booking.created": "Scheduled",
                  "booking.updated": "Rescheduled",
                  "booking.canceled": "Canceled"}
    meeting_status = status_map.get(ev, "Scheduled")

    booking_url = f"https://squareup.com/dashboard/appointments/bookings/{sq_id}"
    desc = (f"Square booking status: {booking['status']}\n"
            f"Booking URL: {booking_url}")

    existing = search("Events", f"({ZOHO_FIELD_SQUARE_ID}:equals:{sq_id})")
    if existing and ev in {"booking.updated", "booking.canceled"}:
        update_event(existing[0]["id"], title, start_iso, end_iso,
                     meeting_status, desc)
    elif not existing and ev == "booking.created":
        create_event(rec_id, module, sq_id, title, start_iso, end_iso,
                     meeting_status, desc)

    return {"ok": True}
# -------------------------------------------------------------
