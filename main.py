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
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")

ZOHO_BASE_URL = "https://www.zohoapis.com/crm/v2"
ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

def zoho_access_token():
    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    r = requests.post(ZOHO_TOKEN_URL, data=payload).json()
    return r.get("access_token")

def normalize_phone(phone):
    if not phone:
        return ""
    phone = ''.join(c for c in phone if c.isdigit() or c == '+')
    if not phone.startswith('+') and phone:
        phone = '+' + phone
    return phone

def find_lead_or_contact(email, phone, headers):
    # Check Leads by email
    if email:
        lead_url = f"{ZOHO_BASE_URL}/Leads/search?criteria=(Email:equals:{email})"
        lead_resp = requests.get(lead_url, headers=headers).json()
        leads = lead_resp.get("data", [])
        if leads:
            return {"module": "Leads", "id": leads[0]["id"]}

    # Check Contacts by email
    if email:
        contact_url = f"{ZOHO_BASE_URL}/Contacts/search?criteria=(Email:equals:{email})"
        contact_resp = requests.get(contact_url, headers=headers).json()
        contacts = contact_resp.get("data", [])
        if contacts:
            return {"module": "Contacts", "id": contacts[0]["id"]}

    # Check Leads by phone
    if phone:
        lead_url = f"{ZOHO_BASE_URL}/Leads/search?criteria=(Phone:equals:{phone})"
        lead_resp = requests.get(lead_url, headers=headers).json()
        leads = lead_resp.get("data", [])
        if leads:
            return {"module": "Leads", "id": leads[0]["id"]}

    # Check Contacts by phone
    if phone:
        contact_url = f"{ZOHO_BASE_URL}/Contacts/search?criteria=(Phone:equals:{phone})"
        contact_resp = requests.get(contact_url, headers=headers).json()
        contacts = contact_resp.get("data", [])
        if contacts:
            return {"module": "Contacts", "id": contacts[0]["id"]}

    return None

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
    booking_resp = requests.get(
        f"https://connect.squareup.com/v2/bookings/{booking_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}
    )
    booking = booking_resp.json().get("booking", {})
    if not booking:
        raise HTTPException(status_code=404, detail="Booking not found")

    customer_id = booking.get("customer_id")
    service_variation_id = booking.get("appointment_segments", [{}])[0].get("service_variation_id")
    start_time = booking.get("start_at")
    location_name = booking.get("location_id")
    square_meeting_id = booking.get("id")

    # Get customer info
    customer_resp = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}
    )
    customer = customer_resp.json().get("customer", {})
    first_name = customer.get("given_name", "")
    last_name = customer.get("family_name", "")
    email = customer.get("email_address", "")
    phone = normalize_phone(customer.get("phone_number", ""))
    address = customer.get("address", {})

    if not email and not phone:
        raise HTTPException(status_code=400, detail="Missing required fields: email or phone required")

    full_name = f"{first_name} {last_name}".strip()
    title = f"Himplant Virtual Consultation with {location_name}, {full_name}"

    token = zoho_access_token()
    zhdr = {"Authorization": f"Zoho-oauthtoken {token}"}

    # Find Lead or Contact by email, then phone
    record = find_lead_or_contact(email, phone, zhdr)

    if not record:
        # Create new Lead
        payload = {
            "data": [{
                "Last_Name": last_name or "Unknown",
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
        lead_resp = requests.post(f"{ZOHO_BASE_URL}/Leads", json=payload, headers=zhdr).json()
        lead_id = lead_resp.get("data", [{}])[0].get("details", {}).get("id")
        record = {"module": "Leads", "id": lead_id}

    # Prevent duplicate meetings
    event_search_url = f"{ZOHO_BASE_URL}/Events/search?criteria=(Square_Meeting_ID:equals:{square_meeting_id})"
    check_event = requests.get(event_search_url, headers=zhdr).json()
    if check_event.get("data"):
        return {"message": "Meeting already exists."}

    # Create Event
    start_dt = datetime.fromisoformat(start_time.replace("Z", ""))
    end_dt = start_dt + timedelta(minutes=30)
    evt = {
        "data": [{
            "Event_Title": title,
            "Who_Id": record["id"],
            "$who_type": record["module"],   # Specify module for Who_Id
            "Start_DateTime": start_dt.isoformat(),
            "End_DateTime": end_dt.isoformat(),
            "Square_Meeting_ID": square_meeting_id,
            "Meeting_Status": "Scheduled"
        }]
    }
    event_resp = requests.post(f"{ZOHO_BASE_URL}/Events", json=evt, headers=zhdr).json()
    if "data" in event_resp:
        return {"status": "Event created"}
    else:
        raise HTTPException(status_code=500, detail=f"Event creation failed: {event_resp}")
