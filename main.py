# main.py  – drop-in version
import os, time, logging, re, random, requests
from typing import Optional
from fastapi import FastAPI, Request

# ---------- ENV ----------
SQUARE_ACCESS_TOKEN  = os.environ["SQUARE_ACCESS_TOKEN"].strip()
ZOHO_CLIENT_ID       = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET   = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN   = os.environ["ZOHO_REFRESH_TOKEN"].strip()
ZOHO_FIELD_SQUARE_ID = "Square_Meeting_ID"          # <- change if API-name differs
# --------------------------

SQUARE_API  = "https://connect.squareup.com/v2"
ZOHO_OAUTH  = "https://accounts.zoho.com/oauth/v2/token"
ZOHO_API    = "https://www.zohoapis.com/crm/v5"

log = logging.getLogger("uvicorn.error")
app = FastAPI()

@app.get("/")
@app.head("/")
def health():
    return {"ok": True}

# ---------- helpers ----------
_token: tuple[str, float] | None = None
def zoho_token() -> str:
    global _token
    if _token and _token[1] > time.time() + 120:
        return _token[0]
    r = requests.post(
        ZOHO_OAUTH,
        data=dict(
            refresh_token = ZOHO_REFRESH_TOKEN,
            client_id     = ZOHO_CLIENT_ID,
            client_secret = ZOHO_CLIENT_SECRET,
            grant_type    = "refresh_token",
        ),
        timeout=10,
    ).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho refresh failed → {r}")
    _token = (r["access_token"], time.time() + int(r["expires_in"]))
    return _token[0]

def zh() -> dict[str,str]:
    return {"Authorization": f"Zoho-oauthtoken {zoho_token()}"}

def square_headers() -> dict[str,str]:
    return {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Square-Version": "2024-05-15",
        "Content-Type": "application/json"
    }

PHONE_RE = re.compile(r"[^\d]+")
def fmt_phone(num: Optional[str]) -> Optional[str]:
    if not num:
        return None
    digits = PHONE_RE.sub("", num)
    return f"+{digits}"

def retry_zoho(req_fn,*a,**k):
    for attempt in range(5):
        r = req_fn(*a,**k)
        if r.status_code != 429:
            return r
        delay = (2**attempt) + random.random()
        log.warning("Zoho 429 – sleeping %.1fs", delay)
        time.sleep(delay)
    return r

def search_module(module:str, criteria:str):
    r = retry_zoho(requests.get, f"{ZOHO_API}/{module}/search",
                   headers=zh(), params={"criteria":criteria}, timeout=15)
    r.raise_for_status()
    return r.json().get("data",[])

def upsert_person(email, phone, first, last, addr):
    crit_e = f"(Email:equals:{email.lower()})"
    crit_p = f"(Phone:equals:{PHONE_RE.sub('' , phone or '')})"
    criteria = f"({crit_e}or{crit_p})" if phone else crit_e

    for mod in ("Leads","Contacts"):
        res = search_module(mod, criteria)
        if res:
            rec = res[0]; rec_id = rec["id"]
            if addr and not rec.get("Mailing_Street"):
                patch = {"id": rec_id, **addr}
                retry_zoho(requests.put, f"{ZOHO_API}/{mod}",
                           headers=zh(), json={"data":[patch]}, timeout=15)
            return rec_id, mod

    payload = {"First_Name":first,"Last_Name":last or "(unknown)",
               "Email":email,"Phone":phone or "",
               "Lead_Source":"Square", **addr}
    res = retry_zoho(requests.post, f"{ZOHO_API}/Leads",
                     headers=zh(), json={"data":[payload]}, timeout=15).json()
    return res["data"][0]["details"]["id"], "Leads"

def ensure_meeting(rec_id, module, sq_id, title, start_iso, end_iso, status):
    if search_module("Events", f"({ZOHO_FIELD_SQUARE_ID}:equals:{sq_id})"):
        return
    evt = {
        "Event_Title":     title,
        "Start_DateTime":  start_iso,
        "End_DateTime":    end_iso,
        "Meeting_Status":  status.capitalize(),
        ZOHO_FIELD_SQUARE_ID: sq_id,
        "$se_module":      module,
        "What_Id":         rec_id,
    }
    retry_zoho(requests.post, f"{ZOHO_API}/Events",
               headers=zh(), json={"data":[evt]}, timeout=15)
# -------------------------------------------------

@app.post("/square/webhook")
async def square_webhook(request: Request):
    body = await request.json()
    if not body.get("type","").startswith("booking."):
        return {"ignored": body.get("type")}

    b   = body["data"]["object"]["booking"]
    sq_id   = b["id"]
    status  = b["status"]
    start   = b["start_at"]                          # UTC ISO
    dur     = b["appointment_segments"][0]["duration_minutes"]
    end_ts  = time.mktime(time.strptime(start,"%Y-%m-%dT%H:%M:%SZ")) + dur*60
    end_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_ts))

    cust = requests.get(f"{SQUARE_API}/customers/{b['customer_id']}",
                        headers=square_headers(), timeout=10).json()["customer"]
    email = (cust.get("email_address") or f"{sq_id}@square.noemail").lower()
    first = cust.get("given_name","")
    last  = cust.get("family_name","")
    phone = fmt_phone(cust.get("phone_number"))

    addr_sq = cust.get("address") or {}
    addr = {}
    if addr_sq:
        addr.update(Mailing_Street=addr_sq.get("address_line_1",""),
                    Mailing_City  =addr_sq.get("locality",""),
                    Mailing_State =addr_sq.get("administrative_district_level_1",""),
                    Mailing_Zip   =addr_sq.get("postal_code",""),
                    Country       =addr_sq.get("country","US"))

    loc = requests.get(f"{SQUARE_API}/locations/{b['location_id']}",
                       headers=square_headers(), timeout=10).json()["location"]
    loc_name = loc.get("name","Location")
    title = f"Himplant virtual consultation – {loc_name} – {first} {last}".strip()

    rec_id, module = upsert_person(email, phone, first, last, addr)
    ensure_meeting(rec_id, module, sq_id, title, start, end_iso, status)

    return {"done": True}
