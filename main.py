# main.py  – copy / commit / push
import os, time, logging, re, random, asyncio, requests
from typing import Optional
from fastapi import FastAPI, Request, Header

# ---------------  ENV VARS  ----------------------------------
SQUARE_ACCESS_TOKEN  = os.environ["SQUARE_ACCESS_TOKEN"].strip()
ZOHO_CLIENT_ID       = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET   = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN   = os.environ["ZOHO_REFRESH_TOKEN"].strip()
ZOHO_FIELD_SQUARE_ID = "Square_Meeting_ID"            # <-- change if field API-name differs
# -------------------------------------------------------------

SQUARE_API  = "https://connect.squareup.com/v2"
ZOHO_OAUTH  = "https://accounts.zoho.com/oauth/v2/token"
ZOHO_API    = "https://www.zohoapis.com/crm/v5"

log = logging.getLogger("uvicorn.error")
app = FastAPI()

@app.get("/")
@app.head("/")
def health():  # Render’s health-check
    return {"ok": True}

# ---------------- Zoho token cache ----------------
_token: tuple[str, float] | None = None        # (token, expiryEpoch)

def zoho_token() -> str:
    global _token
    if _token and _token[1] > time.time() + 120:
        return _token[0]
    r = requests.post(
        ZOHO_OAUTH,
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id":     ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type":    "refresh_token",
        },
        timeout=10,
    ).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho refresh failed → {r}")
    _token = (r["access_token"], time.time() + int(r["expires_in"]))
    return _token[0]

def zh() -> dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {zoho_token()}"}

# ---------------- utilities ----------------
PHONE_RE = re.compile(r"[^\d]+")

def fmt_phone(num: Optional[str]) -> Optional[str]:
    """to +<digits> (no spaces). Returns None if nothing."""
    if not num:
        return None
    num = num.strip()
    lead_plus = num.startswith("+")
    digits = PHONE_RE.sub("", num)
    return f"+{digits}" if lead_plus else f"+{digits}"  # always include '+'

def retry_zoho(req_fn, *a, **k):
    """simple back-off retry wrapper for Zoho 429s."""
    for attempt in range(5):
        r = req_fn(*a, **k)
        if r.status_code != 429:
            return r
        delay = (2 ** attempt) + random.random()
        log.warning("Zoho 429 – sleeping %.1fs", delay)
        time.sleep(delay)
    return r  # last response


def search_in_module(module: str, criteria: str):
    r = retry_zoho(
        requests.get,
        f"{ZOHO_API}/{module}/search",
        headers=zh(),
        params={"criteria": criteria},
        timeout=15,
    )
    r.raise_for_status()
    return r.json().get("data", [])


def upsert_person(email: str, phone: Optional[str], first: str, last: str,
                  addr: dict) -> tuple[str, str]:
    """returns (recordId, module)"""
    # build criteria fragments
    crit_email = f"(Email:equals:{email.lower()})"
    crit_phone = f"(Phone:equals:{PHONE_RE.sub('', phone or '')})"
    combined   = f"({crit_email}or{crit_phone})" if phone else crit_email

    for module in ("Leads", "Contacts"):
        res = search_in_module(module, combined)
        if res:
            rec = res[0]
            rec_id = rec["id"]
            # optional address patch
            if addr and not rec.get("Mailing_Street"):
                patch = {"id": rec_id, **addr}
                retry_zoho(requests.put, f"{ZOHO_API}/{module}",
                           headers=zh(), json={"data": [patch]}, timeout=15)
            return rec_id, module

    # create Lead
    payload = {
        "First_Name": first,
        "Last_Name":  last or "(unknown)",
        "Email":      email,
        "Phone":      phone or "",
        "Lead_Source":"Square",
        **addr,
    }
    res = retry_zoho(requests.post, f"{ZOHO_API}/Leads",
                     headers=zh(), json={"data": [payload]}, timeout=15).json()
    rec_id = res["data"][0]["details"]["id"]
    return rec_id, "Leads"


def ensure_meeting(rec_id: str, module: str, sq_id: str,
                   title: str, start_iso: str, end_iso: str, status: str):
    """create one Event per Square booking"""
    criteria = f"({ZOHO_FIELD_SQUARE_ID}:equals:{sq_id})"
    if search_in_module("Events", criteria):
        return
    evt = {
        "Event_Title":     title,
        "Start_DateTime":  start_iso,
        "End_DateTime":    end_iso,
       
