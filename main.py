import os
import requests
from fastapi import FastAPI, Request, Header, HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Env vars
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY")

def zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    r = requests.post(url, data=payload).json()
    return r.get("access_token")

@app.get("/", status_code=200)
def root():
    return {"status": "OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    if x_square_signature != SQUARE_WEBHOOK_KEY:
        raise HTTPException(status_code=401, detail="Invalid Square Webhook Key")

    body = await req.json()
    event_type = body.get("type")
    booking_id = body.get("data", {}).get("id")
    location_id = body.get("data", {}).get("object", {}).get("booking", {}).get("location_id")

    if event_type not in ["booking.created", "booking.updated"]:
        return {"ignored": True}

    # Get booking details
    booking = requests.get(
        f"https://connect.squareup.com/v2/bookings/{booking_id}",
        headers={"Authorization": f"Bearer {os.getenv('SQUARE_ACCESS_TOKEN')}"}
    ).json().get("booking", {})

    if not booking:
        raise HTTPException(status_code=404, detail="Booking not found")

    customer_id = booking.get("customer_id")
    service_variation_id = booking.get("appointment_segments", [{}])[0].get("service_variation_id")
    start_time = booking.get("start_at")
    location_name = booking.get("location_id")  # optional mapping lookup if needed
    square_meeting_id = booking.get("id")

    # Get customer info
    customer = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers={"Authorization": f"Bearer {os.getenv('SQUARE_ACCESS_TOKEN')}"}
    ).json().get("customer", {})

    first_name = customer.get("given_name", "")
    last_name = customer.get("family_name", "")
    email = customer.get("email_address", "")
    phone = customer.get("phone_number", "").replace(" ", "").replace("-", "")
    address = customer.get("address", {})

    if not email or not last_name:
        raise HTTPException(status_code=400, detail="Missing required fields")

    full_name = f"{first_name} {last_name}".strip()
    title = f"Himplant Virtual Consultation with {location_name}, {full_name}"
    phone = "+" + phone if not phone.startswith("+") else phone

    token = zoho_access_token()
    zhdr = {"Authorization": f"Zoho-oauthtoken {token}"}

    # Lookup lead/contact
    search_url = f"https://www.zohoapis.com/crm/v2/Leads/search?criteria=(Email:equals:{email})"
    lead_resp = requests.get(search_url, headers=zhdr).json()
    leads = lead_resp.get("data", [])
    lead_id = leads[0]["id"] if leads else None

    if not lead_id:
        # Create new Lead
        payload = {
            "data": [{
                "Last_Name": last_name,
                "First_Name": first_name,
                "Email": email,
                "Phone": phone,
                "Square Meeting ID": square_meeting_id,
                "Street": address.get("address_line_1", ""),
                "City": address.get("locality", ""),
                "State": address.get("administrative_district_level_1", ""),
                "Country": address.get("country", ""),
                "Zip_Code": address.get("postal_code", "")
            }]
        }
        res = requests.post("https://www.zohoapis.com/crm/v2/Leads", json=payload, headers=zhdr).json()
        lead_id = res.get("data", [{}])[0].get("details", {}).get("id")

    # Prevent duplicate meetings
    check_event = requests.get(
        f"https://www.zohoapis.com/crm/v2/Events/search?criteria=(Square_Meeting_ID:equals:{square_meeting_id})",
        headers=zhdr
    ).json()
    if check_event.get("data"):
        return {"message": "Meeting already exists."}

    # Create Event
    evt = {
        "data": [{
            "Event_Title": title,
            "Who_Id": lead_id,
            "Start_DateTime": start_time,
            "End_DateTime": (datetime.fromisoformat(start_time.replace("Z", "")) + timedelta(minutes=30)).isoformat(),
            "Square Meeting ID": square_meeting_id,
            "Meeting_Status": "Scheduled"
        }]
    }
    requests.post("https://www.zohoapis.com/crm/v2/Events", json=evt, headers=zhdr)
    return {"status": "Event created"}
