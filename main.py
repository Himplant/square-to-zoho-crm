# ------------------------- main.py -------------------------
import os, time, random, re, hmac, hashlib, json, requests
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, HTTPException

# ─────────── Environment ───────────
SQUARE_ACCESS_TOKEN  = os.environ["SQUARE_ACCESS_TOKEN"].strip()
SQUARE_WEBHOOK_KEY   = os.getenv("SQUARE_WEBHOOK_KEY", "").strip()

ZOHO_CLIENT_ID       = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET   = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN   = os.environ["ZOHO_REFRESH_TOKEN"].strip()

ZOHO_FIELD_SQUARE_ID = "Square_Meeting_ID"          # change if your API name differs
# ────────────────────────────────────

SQUARE_API  = "https://connect.squareup.com/v2"
ZOHO_API    = "https://www.zohoapis.com/crm/v5"
ZOHO_OAUTH  = "https://accounts.zoho.com/oauth/v2/token"
SQUARE_VER  = "2024-05-15"

app = FastAPI()

# ---------- health for Render ----------
@app.get("/", status_code=200)
def ping_get():  return {"ok": True}

@app.head("/", status_code=200)
def ping_head(): return

# ---------- Zoho token cache ----------
_tok, _exp = "", 0
def zoho_token() -> str:
    global _tok, _exp
    if _tok and _exp > time.time()+60:
        return _tok
    r = requests.post(ZOHO_OAUTH, data={
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id":     ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type":    "refresh_token",
    }, timeout=15).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho OAuth error {r}")
    _tok, _exp = r["access_token"], time.time()+int(r["expires_in"])-30
    return _tok

def zh(): return {"Authorization": f"Zoho-oauthtoken {zoho_token()}"}

# ---------- helpers ----------
PHONE_RE = re.compile(r"[^\d]+")
def clean_phone(num): return f"+{PHONE_RE.sub('',num)}" if num else None

def sq_hdr(): return {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
                      "Square-Version": SQUARE_VER}

def backoff(req,*a,**k):
    for i in range(5):
        r=req(*a,**k)
        if r.status_code!=429: return r
        time.sleep((2**i)+random.random())
    return r

def verify_square(body:bytes, sig:str)->bool:
    if not SQUARE_WEBHOOK_KEY: return True
    mac=hmac.new(SQUARE_WEBHOOK_KEY.encode(),body,hashlib.sha1).hexdigest()
    return hmac.compare_digest(mac,sig)

def search(module, crit):
    r=backoff(requests.get,f"{ZOHO_API}/{module}/search",
              headers=zh(),params={"criteria":crit},timeout=15).json()
    return r.get("data",[])

def upsert_person(email, phone, first, last, addr):
    ecrit=f"(Email:equals:{email})" if email else ""
    pcrit=f"(Phone:equals:{PHONE_RE.sub('',phone or '')})" if phone else ""
    crit=f"({ecrit}or{pcrit})" if ecrit and pcrit else (ecrit or pcrit)

    for mod in ("Leads","Contacts"):
        res=search(mod,crit)
        if res:
            rid=res[0]["id"]
            if addr and not res[0].get("Mailing_Street"):
                backoff(requests.put,f"{ZOHO_API}/{mod}",
                        headers=zh(),json={"data":[{"id":rid,**addr}]},timeout=15)
            return rid,mod

    lead={"First_Name":first,"Last_Name":last or "(Square)","Email":email,
          "Phone":phone or "","Lead_Source":"Square",**addr}
    res=backoff(requests.post,f"{ZOHO_API}/Leads",
                headers=zh(),json={"data":[lead]},timeout=15).json()
    return res["data"][0]["details"]["id"],"Leads"

def create_evt(rec_id,module,sq_id,title,start,end,status,desc):
    evt={"Event_Title":title,"Start_DateTime":start,"End_DateTime":end,
         "Meeting_Status":status,"Description":desc,"$se_module":module,
         "What_Id":rec_id,ZOHO_FIELD_SQUARE_ID:sq_id}
    backoff(requests.post,f"{ZOHO_API}/Events",
            headers=zh(),json={"data":[evt]},timeout=15)

def update_evt(eid,title,start,end,status,desc):
    patch={"id":eid,"Event_Title":title,"Start_DateTime":start,
           "End_DateTime":end,"Meeting_Status":status,"Description":desc}
    backoff(requests.put,f"{ZOHO_API}/Events",
            headers=zh(),json={"data":[patch]},timeout=15)

# ---------- webhook ----------
@app.post("/square/webhook")
async def webhook(req: Request):
    raw=await req.body()
    if not verify_square(raw, req.headers.get("x-square-hmacsha256-signature","")):
        raise HTTPException(401,"bad signature")

    p=json.loads(raw); ev=p.get("type","")
    if not ev.startswith("booking."): return {"ignored":ev}

    b=p["data"]["object"]["booking"]; sq_id=b["id"]
    cust=requests.get(f"{SQUARE_API}/customers/{b['customer_id']}",
                      headers=sq_hdr(),timeout=10).json()["customer"]
    email=(cust.get("email_address") or f"{sq_id}@square.local").lower()
    phone=clean_phone(cust.get("phone_number"))
    first,last=cust.get("given_name",""),cust.get("family_name","")
    a=cust.get("address") or {}
    addr=dict(Mailing_Street=a.get("address_line_1",""),Mailing_City=a.get("locality",""),
              Mailing_State=a.get("administrative_district_level_1",""),
              Mailing_Zip=a.get("postal_code",""),Country=a.get("country","US"))
    rec_id,mod=upsert_person(email,phone,first,last,addr)

    loc=requests.get(f"{SQUARE_API}/locations/{b['location_id']}",
                     headers=sq_hdr(),timeout=10).json()["location"]
    service_id=b["appointment_segments"][0]["service_variation_id"]
    service=requests.get(f"{SQUARE_API}/catalog/object/{service_id}",
                         headers=sq_hdr(),timeout=10).json()
    service_name=service.get("object",{}).get("item_variation",{}).get("name","Service")

    title=f"Himplant virtual consultation – {service_name} – {loc.get('name','Loc')} – {first} {last}".strip()

    start=b["start_at"]; dur=b["appointment_segments"][0]["duration_minutes"]
    end=(datetime.fromisoformat(start.replace("Z","+00:00"))
         +timedelta(minutes=dur)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    status_map={"booking.created":"Scheduled",
                "booking.updated":"Rescheduled",
                "booking.canceled":"Canceled"}
    meeting_status=status_map.get(ev,"Scheduled")

    booking_url=f"https://squareup.com/dashboard/appointments/bookings/{sq_id}"
    desc=f"Square booking status: {b['status']}\nBooking URL: {booking_url}"

    existing=search("Events",f"({ZOHO_FIELD_SQUARE_ID}:equals:{sq_id})")
    if existing:
        update_evt(existing[0]["id"],title,start,end,meeting_status,desc)
    else:
        create_evt(rec_id,mod,sq_id,title,start,end,meeting_status,desc)

    return {"ok":True}
