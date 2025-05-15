# ---------------- main.py ----------------
import os, time, random, re, hmac, hashlib, json, requests
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import FastAPI, Request, HTTPException

# ─────── Environment (set these in Render → Environment) ───────
SQUARE_ACCESS_TOKEN  = os.environ["SQUARE_ACCESS_TOKEN"].strip()
# Optional: if you set a Square “signature key”, we’ll verify it
SQUARE_WEBHOOK_KEY   = os.getenv("SQUARE_WEBHOOK_KEY", "").strip()

ZOHO_CLIENT_ID       = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET   = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN   = os.environ["ZOHO_REFRESH_TOKEN"].strip()

# Zoho custom-field API name that stores the Square booking-id
ZOHO_FIELD_SQUARE_ID = "Square_Meeting_ID"          # ← change if yours differs
# ────────────────────────────────────────────────────────────────

SQUARE_API = "https://connect.squareup.com/v2"
ZOHO_API   = "https://www.zohoapis.com/crm/v5"
ZOHO_OAUTH = "https://accounts.zoho.com/oauth/v2/token"

app = FastAPI()

# ---------- Health-check for Render ----------
@app.get("/", status_code=200)
def root_get():
    return {"ok": True}

@app.head("/", status_code=200)
def root_head():
    return  # no body

# ---------- Zoho token cache ----------
_zoho: dict[str, float] = {"token": "", "exp": 0}

def zoho_token() -> str:
    if _zoho["token"] and _zoho["exp"] > time.time() + 60:
        return _zoho["token"]

    r = requests.post(
        ZOHO_OAUTH,
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id":     ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type":    "refresh_token",
        },
        timeout=15,
    ).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho refresh failed → {r}")

    _zoho["token"] = r["access_token"]
    _zoho["exp"]   = time.time() + int(r["expires_in"]) - 30
    return _zoho["token"]

def zh() -> dict[str,str]:
    return {"Authorization": f"Zoho-oauthtoken {zoho_token()}"}

# ---------- misc helpers ----------
def square_headers() -> dict[str,str]:
    return {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Square-Version": "2024-05-15",
    }

PHONE_RE = re.compile(r"[^\d]+")
def clean_phone(num: Optional[str]) -> Optional[str]:
    if not num:
        return None
    digits = PHONE_RE.sub("", num)
    return f"+{digits}" if digits else None

def backoff(req_fn, *a, **k):
    for i in range(5):
        r = req_fn(*a, **k)
        if r.status_code != 429:
            return r
        time.sleep((2**i) + random.random())
    return r

def verify_square(body: bytes, header_sig: str) -> bool:
    if not SQUARE_WEBHOOK_KEY:
        return True  # signature disabled
    mac = hmac.new(SQUARE_WEBHOOK_KEY.encode(), body, hashlib.sha1).hexdigest()
    return hmac.compare_digest(mac, header_sig)

# ---------- Zoho Lead / Contact upsert ----------
def search_module(module: str, criteria: str):
    r = backoff(
        requests.get,
        f"{ZOHO_API}/{module}/search",
        headers=zh(),
        params={"criteria": criteria},
        timeout=15,
    ).json()
    return r.get("data", [])

def upsert_person(email, phone, first, last, address):
    phone_digits = PHONE_RE.sub("", phone or "")
    crit_e = f"(Email:equals:{email})" if email else ""
    crit_p = f"(Phone:equals:{phone_digits})" if phone_digits else ""
    criteria = f"({crit_e}or{crit_p})" if crit_e and crit_p else (crit_e or crit_p)

    for module in ("Leads", "Contacts"):
        res = search_module(module, criteria)
        if res:
            rec = res[0]
            rec_id = rec["id"]
            # patch empty address once
            if address and not rec.get("Mailing_Street"):
                backoff(
                    requests.put,
                    f"{ZOHO_API}/{module}",
                    headers=zh(),
                    json={"data": [{"id": rec_id, **address}]},
                    timeout=15,
                )
            return rec_id, module

    # create Lead
    lead = {
        "First_Name": first,
        "Last_Name":  last or "(Square)",
        "Email":      email,
        "Phone":      phone or "",
        "Lead_Source": "Square",
        **address,
    }
    r = backoff(
        requests.post,
        f"{ZOHO_API}/Leads",
        headers=zh(),
        json={"data": [lead]},
        timeout=15,
    ).json()
    return r["data"][0]["details"]["id"], "Leads"

# ---------- ensure single Event ----------
def ensure_meeting(rec_id, module, sq_id, title, start_iso, end_iso, status):
    if search_module("Events", f"({ZOHO_FIELD_SQUARE_ID}:equals:{sq_id})"):
        return

    event = {
        "Event_Title":     title,
        "Start_DateTime":  start_iso,
        "End_DateTime":    end_iso,
        "Meeting_Status":  status.capitalize(),
        ZOHO_FIELD_SQUARE_ID: sq_id,
        "$se_module":      module,
        "What_Id":         rec_id,
    }
    backoff(
        requests.post,
        f"{ZOHO_API}/Events",
        headers=zh(),
        json={"data": [event]},
        timeout=15,
    )

# ---------- Webhook endpoint ----------
@app.post("/square/webhook")
async def webhook(request: Request):
    raw = await request.body()
    if not verify_square(raw, request.headers.get("x-square-signature", "")):
        raise HTTPException(401, "Bad signature")

    payload = json.loads(raw)
    if payload.get("type") != "booking.created":
        return {"ignored": payload.get("type")}

    booking = payload["data"]["object"]["booking"]
    square_id = booking["id"]

    # customer from Square
    cust = requests.get(
        f"{SQUARE_API}/customers/{booking['customer_id']}",
        headers=square_headers(),
        timeout=10,
    ).json()["customer"]

    email = (cust.get("email_address") or f"{square_id}@square.local").lower()
    phone = clean_phone(cust.get("phone_number"))
    first = cust.get("given_name", "")
    last  = cust.get("family_name", "")

    addr_raw = cust.get("address") or {}
    address = dict(
        Mailing_Street = addr_raw.get("address_line_1",""),
        Mailing_City   = addr_raw.get("locality",""),
        Mailing_State  = addr_raw.get("administrative_district_level_1",""),
        Mailing_Zip    = addr_raw.get("postal_code",""),
        Country        = addr_raw.get("country","US"),
    )

    rec_id, module = upsert_person(email, phone, first, last, address)

    # location for title
    loc = requests.get(
        f"{SQUARE_API}/locations/{booking['location_id']}",
        headers=square_headers(),
        timeout=10,
    ).json()["location"]
    loc_name = loc.get("name", "Location")

    title = f"Himplant virtual consultation – {loc_name} – {first} {last}".strip()

    start_iso = booking["start_at"]                       # UTC
    dur       = booking["appointment_segments"][0]["duration_minutes"]
    start_dt  = datetime.fromisoformat(start_iso.replace("Z","+00:00"))
    end_dt    = start_dt + timedelta(minutes=dur)
    end_iso   = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    ensure_meeting(rec_id, module, square_id, title, start_iso, end_iso, booking["status"])

    return {"ok": True}
