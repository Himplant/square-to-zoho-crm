from fastapi import FastAPI, Request
import os, requests, json
from datetime import datetime, timedelta

app = FastAPI()

# ── Render-side env vars you already set ────────────────────────────
ZOHO_CLIENT_ID      = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET  = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN  = os.getenv("ZOHO_REFRESH_TOKEN")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "").strip()
# -------------------------------------------------------------------

ACCESS_TOKEN = None
TOKEN_EXPIRY = datetime.utcnow()

def zoho_access_token() -> str:
    """refresh every ~55 min"""
    global ACCESS_TOKEN, TOKEN_EXPIRY
    if ACCESS_TOKEN and datetime.utcnow() < TOKEN_EXPIRY:
        return ACCESS_TOKEN

    r = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id":     ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type":    "refresh_token",
        },
        timeout=15,
    ).json()

    if "access_token" not in r:
        raise RuntimeError(f"Zoho token error: {r}")

    ACCESS_TOKEN = r["access_token"]
    TOKEN_EXPIRY = datetime.utcnow() + timedelta(minutes=55)
    return ACCESS_TOKEN


def iso_end(start_iso: str, minutes: int) -> str:
    dt = datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


@app.post("/square/webhook")
async def square_webhook(request: Request):
    body      = await request.json()
    evt_type  = body.get("type")
    if evt_type not in ("booking.created", "booking.updated"):
        return {"ignored": evt_type}

    booking    = body["data"]["object"]["booking"]
    square_id  = booking["id"]
    cust_id    = booking["customer_id"]

    # 1️⃣  Pull full customer profile FIRST
    sq_hdr = {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"}
    cust   = requests.get(f"https://connect.squareup.com/v2/customers/{cust_id}",
                          headers=sq_hdr, timeout=15).json().get("customer", {})

    email = cust.get("email_address") or ""
    phone = cust.get("phone_number")  or ""
    first = cust.get("given_name", "")
    last  = cust.get("family_name", "") or "Square"

    # 2️⃣  Zoho search (Leads then Contacts)
    zhdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}",
            "Content-Type": "application/json"}

    record_id = None
    module    = None

    if email:
        lead = requests.get(
            f"https://www.zohoapis.com/crm/v2/Leads/search?criteria=(Email:equals:{email})",
            headers=zhdr, timeout=15).json()
        if "data" in lead:
            record_id = lead["data"][0]["id"]
            module    = "Leads"

        if not record_id:
            contact = requests.get(
                f"https://www.zohoapis.com/crm/v2/Contacts/search?criteria=(Email:equals:{email})",
                headers=zhdr, timeout=15).json()
            if "data" in contact:
                record_id = contact["data"][0]["id"]
                module    = "Contacts"

    # 3️⃣  Create Lead if nothing found
    if not record_id:
        lead_body = {"data": [{
            "First_Name": first,
            "Last_Name" : last,
            "Email"     : email,
            "Phone"     : phone,
            "Lead_Source": "Square"
        }]}
        res = requests.post("https://www.zohoapis.com/crm/v2/Leads",
                            headers=zhdr, data=json.dumps(lead_body), timeout=15).json()
        record_id = res["data"][0]["details"]["id"]
        module    = "Leads"

    # 4️⃣  Create or update Event
    evt_search = requests.get(
        f"https://www.zohoapis.com/crm/v2/Events/search?"
        f"criteria=(Square_Booking_ID:equals:{square_id})",
        headers=zhdr, timeout=15).json()

    start_iso = booking["start_at"]
    mins      = booking["appointment_segments"][0].get("duration_minutes", 15)
    end_iso   = iso_end(start_iso, mins)

    if "data" not in evt_search:          # ➟ create new
        evt_body = {"data": [{
            "Event_Title"      : f"Square Booking - {first} {last}",
            "Start_DateTime"   : start_iso,
            "End_DateTime"     : end_iso,
            "All_day"          : False,
            "Meeting_Status"   : "Scheduled",
            "Square_Booking_ID": square_id,
            "What_Id"          : record_id,
            "$se_module"       : module,
            "Description"      : f"Booking status: {booking['status']}"
        }]}
        res = requests.post("https://www.zohoapis.com/crm/v2/Events",
                            headers=zhdr, data=json.dumps(evt_body), timeout=15).json()
        return {"created_event": res}

    else:                                 # ➟ update existing
        evt_id  = evt_search["data"][0]["id"]
        status  = booking.get("status", "ACCEPTED")
        updates = {
            "id"            : evt_id,
            "Start_DateTime": start_iso,
            "End_DateTime"  : end_iso,
            "Meeting_Status": "Canceled" if status == "CANCELED" else "Rescheduled"
        }
        res = requests.put("https://www.zohoapis.com/crm/v2/Events",
                           headers=zhdr, data=json.dumps({"data":[updates]}), timeout=15).json()
        return {"updated_event": res}

