from fastapi import FastAPI, Request
import requests, json
from datetime import datetime, timedelta

app = FastAPI()

# ── hard-coded secrets (you asked to embed them) ───────────────────────────────
ZOHO_CLIENT_ID     = "1000.2PZ7HUBP9RCAVECH1SEE616UBK73MK"
ZOHO_CLIENT_SECRET = "0a533b640122a6b8a3141e370a67efcc79854adea9"
ZOHO_REFRESH_TOKEN = "1000.63e3587d3069eef3052f0eed6aee8c08.661a7fb3c625790d3f814d5b5d3817a7"
SQUARE_ACCESS_TOKEN = "EAAAl2gm21GAvqvFLVF-WokI1WnErZE1yP0_FFarN9DsC_LKVtp7Dvw5u8SRKt3V"
# ──────────────────────────────────────────────────────────────────────────────

def zoho_access_token():
    r = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token"
        },
        timeout=15).json()
    return r["access_token"]

def iso_end(start_iso, minutes):
    dt = datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")

@app.get("/oauth/callback")
def oauth_callback():
    return {"status": "Redirect OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request):
    payload = await req.json()
    if payload.get("type") != "booking.created":
        return {"ignored": payload.get("type")}

    booking  = payload["data"]["object"]["booking"]
    cust_id  = booking["customer_id"]

    # 1) fetch customer
    sq_hdr = {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"}
    customer = requests.get(
        f"https://connect.squareup.com/v2/customers/{cust_id}",
        headers=sq_hdr, timeout=15).json().get("customer", {})

    email  = customer.get("email_address")
    first  = customer.get("given_name", "")
    last   = customer.get("family_name", "") or "Square"
    phone  = customer.get("phone_number", "")

    # 2) Zoho token + headers
    ztok = zoho_access_token()
    zhdr = {"Authorization": f"Zoho-oauthtoken {ztok}", "Content-Type": "application/json"}

    # 3) search or create Lead
    lead_id = None
    if email:
        srch = requests.get(
            f"https://www.zohoapis.com/crm/v2/Leads/search?email={email}",
            headers=zhdr, timeout=15).json()
        if "data" in srch:
            lead_id = srch["data"][0]["id"]

    if not lead_id:
        new_lead = {
            "data": [{
                "First_Name": first,
                "Last_Name": last,
                "Email": email,
                "Phone": phone,
                "Lead_Source": "Square"
            }]
        }
        lead_id = requests.post(
            "https://www.zohoapis.com/crm/v2/Leads",
            headers=zhdr, data=json.dumps(new_lead), timeout=15
        ).json()["data"][0]["details"]["id"]

    # 4) create Event
    start_iso = booking["start_at"]
    mins      = booking["appointment_segments"][0]["duration_minutes"]
    end_iso   = iso_end(start_iso, mins)

    event = {
        "data": [{
            "Event_Title": f"Square Booking - {first or last}",
            "Start_DateTime": start_iso,
            "End_DateTime": end_iso,
            "All_day": False,
            "Meeting_Status": "Scheduled",
            "What_Id": lead_id,
            "$se_module": "Leads",
            "Description": f"Square booking ID {booking['id']}"
        }]
    }
    evt_res = requests.post(
        "https://www.zohoapis.com/crm/v2/Events",
        headers=zhdr, data=json.dumps(event), timeout=15
    ).json()

    return {"status": "success", "lead_id": lead_id, "event": evt_res}
