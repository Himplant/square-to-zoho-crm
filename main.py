import os
import requests
import logging
from fastapi import FastAPI, Request, Header, HTTPException
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()

SQUARE_WEBHOOK_KEY = os.getenv("SQUARE_WEBHOOK_KEY")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://square-to-zoho-crm.onrender.com/square/webhook")
ZOHO_FLOW_WEBHOOK_URL = "https://flow.zoho.com/766298598/flow/webhook/incoming?zapikey=1001.2779f816150d380c2e7b9833df4a9491.74406660db9786a3d65fde27ba11d305&isdebug=false"

# Helper to normalize phone numbers
def normalize_phone(phone):
    if not phone:
        return ""
    phone = ''.join(c for c in phone if c.isdigit() or c == '+')
    if not phone.startswith('+') and phone:
        phone = '+' + phone
    return phone

@app.get("/", status_code=200)
def root():
    return {"status": "OK"}

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    try:
        body = await req.body()
        body_str = body.decode('utf-8')
        logger.info(f"Headers: {dict(req.headers)}")
        logger.info(f"Raw body: {body_str}")
        logger.info(f"SQUARE_WEBHOOK_KEY: {SQUARE_WEBHOOK_KEY}")
        logger.info(f"WEBHOOK_URL: {WEBHOOK_URL}")
        logger.info(f"Received Square signature: {x_square_signature}")

        if not x_square_signature:
            logger.error("No Square signature provided in request")
            raise HTTPException(status_code=401, detail="No Square signature provided")
        if not SQUARE_WEBHOOK_KEY:
            logger.error("SQUARE_WEBHOOK_KEY environment variable is not set")
            raise HTTPException(status_code=500, detail="Webhook key not configured")

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

        try:
            body_json = await req.json()
        except Exception as e:
            logger.error(f"Failed to parse webhook body as JSON: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        event_type = body_json.get("type")
        booking_id = body_json.get("data", {}).get("id")
        logger.info(f"Processing event type: {event_type}, booking_id: {booking_id}")

        if event_type not in ["booking.created", "booking.updated"]:
            logger.info(f"Ignoring event type: {event_type}")
            return {"ignored": True}

        # Fetch booking details from Square
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
        # Fetch customer details from Square
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

        # Build enriched payload
        enriched_payload = {
            "event_type": event_type,
            "booking": booking,
            "customer": customer,
            "raw_webhook": body_json
        }

        # Forward enriched payload to Zoho Flow
        try:
            flow_resp = requests.post(ZOHO_FLOW_WEBHOOK_URL, json=enriched_payload)
            flow_resp.raise_for_status()
            logger.info(f"Successfully sent enriched data to Zoho Flow: {flow_resp.text}")
        except Exception as e:
            logger.error(f"Failed to send data to Zoho Flow: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to send data to Zoho Flow: {str(e)}")

        return {"status": "Enriched data sent to Zoho Flow"}

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}") 
