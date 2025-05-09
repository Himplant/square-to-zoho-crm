from fastapi import FastAPI, Request
import os, requests, json
from datetime import datetime, timedelta

app = FastAPI()

# ───── Secrets come from Render ▶ Environment tab ─────────────────────────
ZOHO_CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
# ───────────────────────────────────────────────────────────────────────────

# ─── Helpers ───────────────────────────────────────────────────────────────
def zoho_access_token() -> str:
    """Swap refresh-token for short-lived access-token (valid ~1 h)."""
    r = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=15,
    ).json()
    if "access_token" not in r:
        raise RuntimeError(f"Zoho token error: {r}")
    return r["access_token"]

def iso_end(start_iso: str, minutes: int) -> str:
    dt = datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")

# dummy route to satisfy Zoho redirect-URI
@app.get("/oauth/callback")
def oauth_cb(): return {"status":"OK"}

# ─── Main webhook ──────────────────────────────────────────────────────────
@app.post("/square/webhook")
async def square_webhook(request: Request):
    body = await request.json()
    evt  = body.get("type")

    if evt not in ("booking.created", "booking.updated"):
        return {"ignored": evt}

    booking = body["data"]["object"]["booking"]
    square_id = booking["id"]
    cust_id   = booking.get("customer_id") \
             or booking.get("creator_details", {}).get("customer_id")

    # 1) fetch customer from Square
    sq_hdr = {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept":"application/json"}
    cust = requests.get(
        f"https://connect.squareup.com/v2/customers/{cust_id}",
        headers=sq_hdr, timeout=15).json().get("customer", {})

    email  = cust.get("email_address")
    first  = cust.get("given_name", "")
    last   = cust.get("family_name", "") or "Square"
    phone  = cust.get("phone_number", "")

    # 2) Zoho auth & headers
    zhdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}",
            "Content-Type": "application/json"}

    # 3) locate existing Event (for updates) or Lead
    lead_id = None
    if email:
        srch = requests.get(
            f"https://www.zohoapis.com/crm/v2/Leads/search?email={email}",
            headers=zhdr, timeout=15).json()
        if "data" in srch:
            lead_id = srch["data"][0]["id"]

    if not lead_id:             # create lead
        lead_payload = {"data":[{
            "First_Name": first,
            "Last_Name" : last,
            "Email"     : email,
            "Phone"     : phone,
            "Lead_Source":"Square"
        }]}
        lead_id = requests.post(
            "https://www.zohoapis.com/crm/v2/Leads",
            headers=zhdr, data=json.dumps(lead_payload), timeout=15
        ).json()["data"][0]["details"]["id"]

    # 4) create or update Event
    #    search by custom field Square_Booking_ID (create it once in CRM)
    evt_search = requests.get(
        f"https://www.zohoapis.com/crm/v2/Events/search?criteria=(Square_Booking_ID:equals:{square_id})",
        headers=zhdr, timeout=15).json()

    if evt == "booking.created" and "data" not in evt_search:
        start = booking["start_at"]
        mins  = booking["appointment_segments"][0].get("duration_minutes", 15)
        end   = iso_end(start, mins)

        event_payload = {"data":[{
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
        evt_res = requests.post(
            "https://www.zohoapis.com/crm/v2/Events",
            headers=zhdr, data=json.dumps(event_payload), timeout=15).json()
        return {"status":"created", "event":evt_res}

    if evt == "booking.updated" and "data" in evt_search:
        evt_id  = evt_search["data"][0]["id"]
        status  = booking["status"]           # ACCEPTED or CANCELED
        if status == "CANCELED":
            upd = {"data":[{"id":evt_id,"Meeting_Status":"Canceled"}]}
        else:  # rescheduled
            start = booking["start_at"]
            mins  = booking["appointment_segments"][0].get("duration_minutes", 15)
            end   = iso_end(start, mins)
            upd = {"data":[{
                "id":evt_id,
                "Start_DateTime": start,
                "End_DateTime"  : end,
                "Meeting_Status": "Rescheduled"
            }]}
        evt_res = requests.put(
            "https://www.zohoapis.com/crm/v2/Events",
            headers=zhdr, data=json.dumps(upd), timeout=15).json()
        return {"status":"updated", "event":evt_res}

    return {"status":"ignored_or_already_exists"}
