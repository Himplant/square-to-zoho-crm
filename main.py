# ------------- main.py -------------
import os, time, random, re, hmac, hashlib, base64, json, requests
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, HTTPException

# ───── env ─────
SQUARE_ACCESS_TOKEN   = os.environ["SQUARE_ACCESS_TOKEN"].strip()
VERIFY_SIG            = os.getenv("VERIFY_SQUARE_SIGNATURE", "false").lower()=="true"
SQUARE_WEBHOOK_KEY    = os.getenv("SQUARE_WEBHOOK_KEY", "").strip()
SQUARE_NOTIFICATION_URL = os.getenv("SQUARE_NOTIFICATION_URL", "").strip()

ZOHO_CLIENT_ID        = os.environ["ZOHO_CLIENT_ID"].strip()
ZOHO_CLIENT_SECRET    = os.environ["ZOHO_CLIENT_SECRET"].strip()
ZOHO_REFRESH_TOKEN    = os.environ["ZOHO_REFRESH_TOKEN"].strip()

ZOHO_FIELD_SQUARE_ID  = "Square_Meeting_ID"
# ─────────────────

SQUARE_API="https://connect.squareup.com/v2"; SQUARE_VER="2024-05-15"
ZOHO_API  ="https://www.zohoapis.com/crm/v5";  ZOHO_OAUTH="https://accounts.zoho.com/oauth/v2/token"

app=FastAPI()
@app.get("/",status_code=200)  def g(): return {"ok":True}
@app.head("/",status_code=200) def h(): return

# ─── Zoho token cache ───
_tok,_exp=" ",0
def zoho_tok():
    global _tok,_exp
    if _tok and _exp>time.time()+60: return _tok
    r=requests.post(ZOHO_OAUTH,data={
        "refresh_token":ZOHO_REFRESH_TOKEN,
        "client_id":ZOHO_CLIENT_ID,"client_secret":ZOHO_CLIENT_SECRET,
        "grant_type":"refresh_token"},timeout=15).json()
    if "access_token" not in r: raise RuntimeError(r)
    _tok,_exp=r["access_token"],time.time()+int(r["expires_in"])-30
    return _tok
def zh(): return {"Authorization":f"Zoho-oauthtoken {zoho_tok()}"}

# ─── helpers ───
PHONE_RE=re.compile(r"[^\d]+");   sq_hdr=lambda:{"Authorization":f"Bearer {SQUARE_ACCESS_TOKEN}","Square-Version":SQUARE_VER}
def clean(num): return f"+{PHONE_RE.sub('',num)}" if num else None
def back(req,*a,**k):
    for i in range(5):
        r=req(*a,**k)
        if r.status_code!=429: return r
        time.sleep((2**i)+random.random()); 
    return r

def verify(body:bytes,sig256,sig1):
    if not (VERIFY_SIG and SQUARE_WEBHOOK_KEY and SQUARE_NOTIFICATION_URL):
        return True
    base=SQUARE_NOTIFICATION_URL.encode()+body
    ok=False
    if sig256:
        ok|=hmac.compare_digest(
            base64.b64encode(hmac.new(SQUARE_WEBHOOK_KEY.encode(),base,hashlib.sha256).digest()).decode(), sig256)
    if sig1 and len(sig1)==40:
        ok|=hmac.compare_digest(
            hmac.new(SQUARE_WEBHOOK_KEY.encode(),base,hashlib.sha1).hexdigest(), sig1)
    return ok

def search(mod,crit):
    r=back(requests.get,f"{ZOHO_API}/{mod}/search",headers=zh(),params={"criteria":crit},timeout=15).json()
    return r.get("data",[])

def upsert(email,phone,f,l,addr):
    ce=f"(Email:equals:{email})" if email else ""; cp=f"(Phone:equals:{PHONE_RE.sub('',phone or '')})" if phone else ""
    crit=f"({ce}or{cp})" if ce and cp else (ce or cp)
    for m in ("Leads","Contacts"):
        hit=search(m,crit)
        if hit:
            rid=hit[0]["id"]
            if addr and not hit[0].get("Mailing_Street"):
                back(requests.put,f"{ZOHO_API}/{m}",headers=zh(),json={"data":[{"id":rid,**addr}]},timeout=15)
            return rid,m
    lead={"First_Name":f,"Last_Name":l or "(Square)","Email":email,"Phone":phone or "","Lead_Source":"Square",**addr}
    r=back(requests.post,f"{ZOHO_API}/Leads",headers=zh(),json={"data":[lead]},timeout=15).json()
    return r["data"][0]["details"]["id"],"Leads"

def create_evt(rec,m,sq_id,title,start,end,stat,desc):
    data={"Event_Title":title,"Start_DateTime":start,"End_DateTime":end,"Meeting_Status":stat,
          "Description":desc,"$se_module":m,"What_Id":rec,ZOHO_FIELD_SQUARE_ID:sq_id}
    back(requests.post,f"{ZOHO_API}/Events",headers=zh(),json={"data":[data]},timeout=15)

def update_evt(eid,title,start,end,stat,desc):
    patch={"id":eid,"Event_Title":title,"Start_DateTime":start,"End_DateTime":end,"Meeting_Status":stat,"Description":desc}
    back(requests.put,f"{ZOHO_API}/Events",headers=zh(),json={"data":[patch]},timeout=15)

# ─── webhook ───
@app.post("/square/webhook")
async def sq_web(req:Request):
    body=await req.body()
    if not verify(body,req.headers.get("x-square-hmacsha256-signature"),req.headers.get("x-square-signature")):
        raise HTTPException(401,"Signature mismatch")
    p=json.loads(body); ev=p.get("type","")
    if not ev.startswith("booking."): return {"ignored":ev}
    b=p["data"]["object"]["booking"]; sq_id=b["id"]

    cust=requests.get(f"{SQUARE_API}/customers/{b['customer_id']}",headers=sq_hdr(),timeout=10).json()["customer"]
    email=(cust.get("email_address") or f"{sq_id}@square.local").lower(); phone=clean(cust.get("phone_number"))
    first,last=cust.get("given_name",""),cust.get("family_name","")
    a=cust.get("address") or {}
    addr=dict(Mailing_Street=a.get("address_line_1",""),Mailing_City=a.get("locality",""),
              Mailing_State=a.get("administrative_district_level_1",""),Mailing_Zip=a.get("postal_code",""),Country=a.get("country","US"))
    rec,mod=upsert(email,phone,first,last,addr)

    svc_id=b["appointment_segments"][0]["service_variation_id"]
    svc=requests.get(f"{SQUARE_API}/catalog/object/{svc_id}",headers=sq_hdr(),timeout=10).json()
    svc_name=svc.get("object",{}).get("item_variation",{}).get("name","Service")
    loc=requests.get(f"{SQUARE_API}/locations/{b['location_id']}",headers=sq_hdr(),timeout=10).json()["location"]
    loc_name=loc.get("name","Location")
    title=f"Himplant virtual consultation – {svc_name} – {loc_name} – {first} {last}".strip()

    start=b["start_at"]; dur=b["appointment_segments"][0]["duration_minutes"]
    end=(datetime.fromisoformat(start.replace("Z","+00:00"))+timedelta(minutes=dur)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status_map={"booking.created":"Scheduled","booking.updated":"Rescheduled","booking.canceled":"Canceled"}
    mstat=status_map.get(ev,"Scheduled")
    desc=f"Square booking status: {b['status']}\nBooking URL: https://squareup.com/dashboard/appointments/bookings/{sq_id}"

    hit=search("Events",f"({ZOHO_FIELD_SQUARE_ID}:equals:{sq_id})")
    if hit and ev in {"booking.updated","booking.canceled"}:
        update_evt(hit[0]["id"],title,start,end,mstat,desc)
    elif not hit and ev=="booking.created":
        create_evt(rec,mod,sq_id,title,start,end,mstat,desc)

    return {"ok":True}
# --------------------------------------
