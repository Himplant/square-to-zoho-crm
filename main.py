import os
import requests
import logging
from fastapi import FastAPI, Request, Header, HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv
from square.utilities.webhooks_helper import is_valid_webhook_event_signature

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()

# Env vars
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://square-to-zoho-crm.onrender.com/square/webhook")

# Validate required environment variables
required_env_vars = [
    "ZOHO_CLIENT_ID",
    "ZOHO_CLIENT_SECRET",
    "ZOHO_REFRESH_TOKEN",
    "SQUARE_WEBHOOK_KEY",
    "SQUARE_ACCESS_TOKEN"
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

ZOHO_BASE_URL = "https://www.zohoapis.com/crm/v2"
ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

def normalize_phone(phone):
    """Normalize phone number to international format."""
    if not phone:
        return ""
    # Remove all non-digit characters except '+'
    phone = ''.join(c for c in phone if c.isdigit() or c == '+')
    # Add '+' prefix if not present
    if not phone.startswith('+') and phone:
        phone = '+' + phone
    return phone

def zoho_access_token():
    """Get Zoho CRM access token using refresh token."""
    try:
        payload = {
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token"
        }
        r = requests.post(ZOHO_TOKEN_URL, data=payload)
        r.raise_for_status()
        response = r.json()
        if "access_token" not in response:
            logger.error(f"Failed to get access token: {response}")
            raise HTTPException(status_code=500, detail="Failed to get Zoho access token")
        return response["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting Zoho access token: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting Zoho access token: {str(e)}")

def find_lead_or_contact(email, phone, headers):
    """Find existing lead or contact by email or phone."""
    try:
        # Check Leads by email
        if email:
            lead_url = f"{ZOHO_BASE_URL}/Leads/search?criteria=(Email:equals:{email})"
            lead_resp = requests.get(lead_url, headers=headers)
            lead_resp.raise_for_status()
            leads = lead_resp.json().get("data", [])
            if leads:
                logger.info(f"Found existing lead by email: {email}")
                return {"module": "Leads", "id": leads[0]["id"]}

        # Check Contacts by email
        if email:
            contact_url = f"{ZOHO_BASE_URL}/Contacts/search?criteria=(Email:equals:{email})"
            contact_resp = requests.get(contact_url, headers=headers)
            contact_resp.raise_for_status()
            contacts = contact_resp.json().get("data", [])
            if contacts:
                logger.info(f"Found existing contact by email: {email}")
                return {"module": "Contacts", "id": contacts[0]["id"]}

        # Check Leads by phone
        if phone:
            lead_url = f"{ZOHO_BASE_URL}/Leads/search?criteria=(Phone:equals:{phone})"
            lead_resp = requests.get(lead_url, headers=headers)
            lead_resp.raise_for_status()
            leads = lead_resp.json().get("data", [])
            if leads:
                logger.info(f"Found existing lead by phone: {phone}")
                return {"module": "Leads", "id": leads[0]["id"]}

        # Check Contacts by phone
        if phone:
            contact_url = f"{ZOHO_BASE_URL}/Contacts/search?criteria=(Phone:equals:{phone})"
            contact_resp = requests.get(contact_url, headers=headers)
            contact_resp.raise_for_status()
            contacts = contact_resp.json().get("data", [])
            if contacts:
                logger.info(f"Found existing contact by phone: {phone}")
                return {"module": "Contacts", "id": contacts[0]["id"]}

        logger.info("No existing lead or contact found")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error searching for lead/contact: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error searching for lead/contact: {str(e)}")

def create_lead(customer_data, headers):
    """Create a new lead in Zoho CRM."""
    try:
        payload = {
            "data": [{
                "Last_Name": customer_data["last_name"] or "Unknown",
                "First_Name": customer_data["first_name"],
                "Email": customer_data["email"],
                "Phone": customer_data["phone"],
                "Square_Meeting_ID": customer_data["square_meeting_id"],
                "Street": customer_data["address"].get("address_line_1", ""),
                "City": customer_data["address"].get("locality", ""),
                "State": customer_data["address"].get("administrative_district_level_1", ""),
                "Country": customer_data["address"].get("country", ""),
                "Zip_Code": customer_data["address"].get("postal_code", "")
            }]
        }
        lead_resp = requests.post(f"{ZOHO_BASE_URL}/Leads", json=payload, headers=headers)
        lead_resp.raise_for_status()
        response = lead_resp.json()
        if "data" not in response:
            logger.error(f"Failed to create lead: {response}")
            raise HTTPException(status_code=500, detail="Failed to create lead in Zoho")
        
        lead_id = response["data"][0]["details"]["id"]
        logger.info(f"Created new lead with ID: {lead_id}")
        return {"module": "Leads", "id": lead_id}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating lead: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating lead: {str(e)}")

def create_event(event_data, headers):
    """Create a new event in Zoho CRM."""
    try:
        event_resp = requests.post(f"{ZOHO_BASE_URL}/Events", json=event_data, headers=headers)
        event_resp.raise_for_status()
        response = event_resp.json()
        if "data" not in response:
            logger.error(f"Failed to create event: {response}")
            raise HTTPException(status_code=500, detail="Failed to create event in Zoho")
        
        logger.info(f"Created new event for {event_data['data'][0]['Who_Id']}")
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")

@app.get("/", status_code=200)
def root():
    """Health check endpoint."""
    return {"status": "OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    """Handle Square webhook events."""
    try:
        # Get raw body for signature validation
        body = await req.body()
        body_str = body.decode('utf-8')
        
        # Log the incoming signature for debugging
        logger.info(f"Received Square signature: {x_square_signature}")
        logger.info(f"Webhook URL: {WEBHOOK_URL}")

        if not x_square_signature:
            logger.error("No Square signature provided in request")
            raise HTTPException(status_code=401, detail="No Square signature provided")

        if not SQUARE_WEBHOOK_KEY:
            logger.error("SQUARE_WEBHOOK_KEY environment variable is not set")
            raise HTTPException(status_code=500, detail="Webhook key not configured")

        # Validate the webhook signature using Square's helper
        is_valid = is_valid_webhook_event_signature(
            body_str,
            x_square_signature,
            SQUARE_WEBHOOK_KEY,
            WEBHOOK_URL
        )

        if not is_valid:
            logger.warning(f"Invalid Square webhook signature. Received: {x_square_signature}")
            raise HTTPException(status_code=401, detail="Invalid Square Webhook Key")

        logger.info("Webhook signature validated successfully")
        logger.info(f"Received webhook body: {body_str}")

        # Parse JSON body
        try:
            body_json = await req.json()
        except Exception as e:
            logger.error(f"Failed to parse webhook body as JSON: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        event_type = body_json.get("type")
        booking_id = body_json.get("data", {}).get("id")
        location_id = body_json.get("data", {}).get("object", {}).get("booking", {}).get("location_id")

        logger.info(f"Processing event type: {event_type}, booking_id: {booking_id}, location_id: {location_id}")

        if event_type not in ["booking.created", "booking.updated"]:
            logger.info(f"Ignoring event type: {event_type}")
            return {"ignored": True}

        # Get booking details
        try:
            booking_resp = requests.get(
                f"https://connect.squareup.com/v2/bookings/{booking_id}",
                headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}
            )
            booking_resp.raise_for_status()
            booking = booking_resp.json().get("booking", {})
            if not booking:
                logger.error(f"Booking not found: {booking_id}")
                raise HTTPException(status_code=404, detail="Booking not found")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching booking details: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error fetching booking details: {str(e)}")

        customer_id = booking.get("customer_id")
        start_time = booking.get("start_at")
        location_name = booking.get("location_id")
        square_meeting_id = booking.get("id")

        # Get customer info
        try:
            customer_resp = requests.get(
                f"https://connect.squareup.com/v2/customers/{customer_id}",
                headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}
            )
            customer_resp.raise_for_status()
            customer = customer_resp.json().get("customer", {})
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching customer details: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error fetching customer details: {str(e)}")

        first_name = customer.get("given_name", "")
        last_name = customer.get("family_name", "")
        email = customer.get("email_address", "")
        phone = normalize_phone(customer.get("phone_number", ""))
        address = customer.get("address", {})

        if not email and not phone:
            logger.error("Missing required fields: email or phone required")
            raise HTTPException(status_code=400, detail="Missing required fields: email or phone required")

        full_name = f"{first_name} {last_name}".strip()
        title = f"Himplant Virtual Consultation with {location_name}, {full_name}"

        token = zoho_access_token()
        zhdr = {"Authorization": f"Zoho-oauthtoken {token}"}

        # Find or create lead/contact
        record = find_lead_or_contact(email, phone, zhdr)
        if not record:
            customer_data = {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "square_meeting_id": square_meeting_id,
                "address": address
            }
            record = create_lead(customer_data, zhdr)

        # Check for existing event
        try:
            event_search_url = f"{ZOHO_BASE_URL}/Events/search?criteria=(Square_Meeting_ID:equals:{square_meeting_id})"
            check_event = requests.get(event_search_url, headers=zhdr)
            check_event.raise_for_status()
            if check_event.json().get("data"):
                logger.info("Meeting already exists")
                return {"message": "Meeting already exists."}
        except requests.exceptions.RequestException as e:
            logger.error(f"Error checking for existing event: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error checking for existing event: {str(e)}")

        # Create event
        start_dt = datetime.fromisoformat(start_time.replace("Z", ""))
        end_dt = start_dt + timedelta(minutes=30)
        event_data = {
            "data": [{
                "Event_Title": title,
                "Who_Id": record["id"],
                "$who_type": record["module"],
                "Start_DateTime": start_dt.isoformat(),
                "End_DateTime": end_dt.isoformat(),
                "Square_Meeting_ID": square_meeting_id,
                "Meeting_Status": "Scheduled"
            }]
        }
        
        create_event(event_data, zhdr)
        return {"status": "Event created"}

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}")
