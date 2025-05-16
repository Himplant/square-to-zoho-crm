import os
import requests
import logging
from fastapi import FastAPI, Request, Header, HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv

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

def zoho_access_token():
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
// ... existing code ...

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    try:
        if x_square_signature != SQUARE_WEBHOOK_KEY:
            logger.warning("Invalid Square webhook signature")
            raise HTTPException(status_code=401, detail="Invalid Square Webhook Key")

        body = await req.json()
        logger.info(f"Received webhook: {body}")
        
        event_type = body.get("type")
        booking_id = body.get("data", {}).get("id")
        location_id = body.get("data", {}).get("object", {}).get("booking", {}).get("location_id")

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
// ... existing code ...
