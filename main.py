from fastapi import FastAPI, Request
import os, requests, json
from datetime import datetime, timedelta

app = FastAPI()

# ── Render env vars (already set) ────────────────────────────────
ZOHO_CLIENT_ID      = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET  = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN  = os.getenv("ZOHO_REFRESH_TOKEN")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "").strip()
# ----------------------------------------------------------------

ACCESS_TOKEN  = None
TOKEN_EXPIRY  = datetime.utcnow()
ZOHO_API      = "https://www.zohoapis.com/crm/v2"
CUSTOM_FIELD  = "Square_Meeting_ID"          # ← API name of your Event field

def zoho_access_token() -> str:
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


def square_get(url_path: str):
    hdr = {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
           "Accept": "application/json"}
    return requests.get(f"https://connect.squareup.com{url_path}", headers=hdr, timeout=15).json()


@app.post("/square/webhook")
async def square_webhook(request: Request):
    body     = await request.json()
    evt_type = body.get("type")

    if evt_type not in ("booking.created", "booking.updated", "booking.canceled"):
        return {"ignored": evt_type}

    booking   = body["data"]["object"]["booking"]
    book_id   = booking["id"]
    cust_id   = booking["customer_id"]
    loc_id    = booking["location_id"]

    # 1️⃣  full customer profile
    cust = square_get(f"/v2/customers/{cust_id}").get("customer", {})
    email = cust.get("email_address") or ""
    phone = cust.get("phone_number")  or ""
    first = cust.get("given_name", "")
    last  = cust.get("family_name", "") or "Square"

    # 1️⃣b location (= surgeon name in your account)
    location_name = square_get(f"/v2/locations/{loc_id}").get("location", {}).get("name", "Square")

    # 2️⃣  Zoho auth header
    zhdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}",
            "Content-Type": "application/json"}

    # 3️⃣  find existing lead/contact
    record_id = None
    module    = None
    if email:
        lead = requests.get(f"{ZOHO_API}/Leads/search?criteria=(Email:equals:{email})",
                            headers=zhdr, timeout=15).json()
        if "data" in lead:
            record_id = lead["data"][0]["id"]
            module    = "Leads"
        else:
            contact = requests.get(f"{ZOHO_API}/Contacts/search?criteria=(Email:equals:{email})",
                                   headers=zhdr, timeout=15).json()
            if "data" in contact:
                record_id = contact["data"][0]["id"]
                module    = "Contacts"

    # 4️⃣  create Lead when nothing found
    if not record_id:
        new_lead = {"data":[{
            "First_Name": first,
            "Last_Name" : last,
            "Email"     : email,
            "Phone"     : phone,
            "Lead_Source": "Square"
        }]}
        res = requests.post(f"{ZOHO_API}/Leads", headers=zhdr,
                            data=json.dumps(new_lead), timeout=15).json()
        record_id = res["data"][0]["details"]["id"]
        module    = "Leads"

    # 5️⃣  look for existing Event with the custom field
    evt_search = requests.get(
        f"{ZOHO_API}/Events/search?criteria=({CUSTOM_FIELD}:equals:{book_id})",
        headers=zhdr, timeout=15).json()

    start_iso = booking["start_at"]
    mins      = booking["appointment_segments"][0].get("duration_minutes", 15)
    end_iso   = iso_end(start_iso, mins)

    title     = f"Himplant Consultation – {first} {last} – {location_name}"
    status    = booking.get("status", "ACCEPTED")
    meet_stat = {"ACCEPTED":"Scheduled", "CANCELED":"Canceled"}.get(status, "Rescheduled")

    if "data" not in evt_search:               # create new
        evt = {"data": [{
            "Event_Title"      : title,
            "Start_DateTime"   : start_iso,
            "End_DateTime"     : end_iso,
            "All_day"          : False,
            "Meeting_Status"   : meet_stat,
            CUSTOM_FIELD       : book_id,
            "What_Id"          : record_id,
            "$se_module"       : module,
            "Description"      : f"Booking status: {status}"
        }]}
        res = requests.post(f"{ZOHO_API}/Events", headers=zhdr,
                            data=json.dumps(evt), timeout=15).json()
        return {"created_event": res}

    # otherwise update
    evt_id = evt_search["data"][0]["id"]
    upd    = {"data":[{
        "id"            : evt_id,
        "Event_Title"   : title,
        "Start_DateTime": start_iso,
        "End_DateTime"  : end_iso,
        "Meeting_Status": meet_stat,
        "Description"   : f"Booking status: {status}"
    }]}
    res = requests.put(f"{ZOHO_API}/Events", headers=zhdr,
                       data=json.dumps(upd), timeout=15).json()
    return {"updated_event": res}
