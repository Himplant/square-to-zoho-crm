from fastapi import FastAPI, Request
import os, requests, json
from datetime import datetime, timedelta

app = FastAPI()

# ── secrets from Render env ────────────────────────────────────────────────
ZOHO_CLIENT_ID      = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET  = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN  = os.getenv("ZOHO_REFRESH_TOKEN")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "").strip()
# ───────────────────────────────────────────────────────────────────────────

# in-memory cache for Zoho access tokens
ACCESS_TOKEN: str | None = None
TOKEN_EXPIRY = datetime.utcnow()  # expired so first call refreshes

def zoho_access_token() -> str:
    global ACCESS_TOKEN, TOKEN_EXPIRY
    if ACCESS_TOKEN and datetime.utcnow() < TOKEN_EXPIRY:
        return ACCESS_TOKEN

    resp = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=15,
    ).json()

    if "access_token" not in resp:
        raise RuntimeError(f"Zoho token error: {resp}")

    ACCESS_TOKEN = resp["access_token"]
    TOKEN_EXPIRY = datetime.utcnow() + timedelta(minutes=55)  # renew 5 min early
    return ACCESS_TOKEN


def iso_end(start_iso: str, minutes: int) -> str:
    dt = datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


@app.get("/oauth/callback")
def oauth_cb():
    return {"status": "OK"}


@app.post("/square/webhook")
async def square_webhook(request: Request):
    body     = await request.json()
    evt_type = body.get("type")

    if evt_type not in ("booking.created", "booking.updated"):
        return {"ignored": evt_type}

    booking   = body["data"]["object"]["booking"]
    square_id = booking["id"]
    cust_id   = booking.get("customer_id") or booking.get("creator_details", {}).get("customer_id")

    # 1️⃣  Square customer lookup
    sq_hdr = {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"}
    customer = requests.get(
        f"https://connect.squareup.com/v2/customers/{cust_id}",
        headers=sq_hdr, timeout=15).json().get("customer", {})

    email = customer.get("email_address")
    first = customer.get("given_name", "")
    last  = customer.get("family_name", "") or "Square"
    phone = customer.get("phone_number", "")

    # 2️⃣  Zoho headers
    zhdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}",
            "Content-Type": "application/json"}

    # 3️⃣  find or create Lead
    lead_id = None
    if email:
        srch = requests.get(
            f"https://www.zohoapis.com/crm/v2/Leads/search?email={email}",
            headers=zhdr, timeout=15).json()
        if "data" in srch:
            lead_id = srch["data"][0]["id"]

    if not lead_id:
        lead_body = {"data":[{
            "First_Name": first,
            "Last_Name" : last,
            "Email"     : email,
            "Phone"     : phone,
            "Lead_Source": "Square"
        }]}
        lead_id = requests.post(
            "https://www.zohoapis.com/crm/v2/Leads",
            headers=zhdr, data=json.dumps(lead_body), timeout=15
        ).json()["data"][0]["details"]["id"]

    # 4️⃣  create or update Event
    evt_search = requests.get(
        f"https://www.zohoapis.com/crm/v2/Events/search?criteria=(Square_Booking_ID:equals:{square_id})",
        headers=zhdr, timeout=15).json()

    if evt_type == "booking.created" and "data" not in evt_search:
        start = booking["start_at"]
        mins  = booking["appointment_segments"][0].get("duration_minutes", 15)
        end   = iso_end(start, mins)

        evt_body = {"data":[{
            "Event_Title"      : f"Square Booking - {first or last}",
            "Start_DateTime"   : start,
            "End_DateTime"     : end,
            "All_day"          : False,
            "Meeting_Status"   : "Scheduled",
            "Square_Booking_ID": square_id,
            "What_Id"          : lead_id,
            "$se_module"       : "Leads",
            "Description"      : f"Square booking ID {square_id}"
        }]}
        res = requests.post(
            "https://www.zohoapis.com/crm/v2/Events",
            headers=zhdr, data=json.dumps(evt_body), timeout=15).json()
        return {"status": "created", "event": res}

    if evt_type == "booking.updated" and "data" in evt_search:
        evt_id = evt_search["data"][0]["id"]
        status = booking.get("status", "ACCEPTED")

        if status == "CANCELED":
            upd_body = {"data":[{"id": evt_id, "Meeting_Status": "Canceled"}]}
        else:  # rescheduled
            start = booking["start_at"]
            mins  = booking["appointment_segments"][0].get("duration_minutes", 15)
            end   = iso_end(start, mins)
            upd_body = {"data":[{
                "id"             : evt_id,
                "Start_DateTime" : start,
                "End_DateTime"   : end,
                "Meeting_Status" : "Rescheduled"
            }]}

        res = requests.put(
            "https://www.zohoapis.com/crm/v2/Events",
            headers=zhdr, data=json.dumps(upd_body), timeout=15).json()
        return {"status": "updated", "event": res}

    return {"status": "ignored_or_already_exists"}
