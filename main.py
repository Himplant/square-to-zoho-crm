import os
import hmac
import hashlib
import base64
import requests
from fastapi import FastAPI, Request, HTTPException

# Create FastAPI app
app = FastAPI()

# Environment variables
SQUARE_ACCESS_TOKEN = os.environ.get("SQUARE_ACCESS_TOKEN")
SQUARE_WEBHOOK_KEY = os.environ.get("SQUARE_WEBHOOK_KEY")
ZOHO_CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.environ.get("ZOHO_REFRESH_TOKEN")
ZOHO_ACCOUNTS_BASE = os.environ.get("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ZOHO_CRM_BASE = os.environ.get("ZOHO_CRM_BASE", "https://www.zohoapis.com")
DEFAULT_PIPELINE = os.environ.get("DEFAULT_PIPELINE", "Default")
DEFAULT_DEAL_STAGE = os.environ.get("DEFAULT_DEAL_STAGE", "Qualification")
CANCELED_DEAL_STAGE = os.environ.get("CANCELED_DEAL_STAGE", "Closed Lost")

# --- Root health check ---
@app.get("/")
def health():
    return {"service": "square→zoho", "status": "ok"}

# --- Zoho OAuth token refresh ---
def get_zoho_access_token():
    url = f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    params = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(url, params=params)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to refresh Zoho access token")
    return response.json()["access_token"]

# --- Square webhook signature verification ---
def verify_square_signature(request_body: bytes, signature_header: str):
    if not SQUARE_WEBHOOK_KEY:
        raise HTTPException(status_code=500, detail="Missing Square Webhook Key")
    hmac_hash = hmac.new(
        SQUARE_WEBHOOK_KEY.encode("utf-8"),
        request_body,
        hashlib.sha1
    ).digest()
    computed_signature = base64.b64encode(hmac_hash).decode()
    return hmac.compare_digest(computed_signature, signature_header)

# --- Square webhook handler ---
async def process_square_webhook(request: Request):
    body_bytes = await request.body()
    signature = request.headers.get("x-square-hmacsha256-signature")
    if not signature or not verify_square_signature(body_bytes, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = await request.json()
    event_type = payload.get("type")
    data_object = payload.get("data", {}).get("object", {})

    if event_type == "booking.created":
        await handle_booking_created(data_object)
    elif event_type == "booking.canceled":
        await handle_booking_canceled(data_object)

    return {"status": "success"}

# --- Business logic for booking created ---
async def handle_booking_created(data_object):
    customer_id = data_object.get("booking", {}).get("customer_id")
    if not customer_id:
        return

    # Fetch customer details from Square
    customer = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}"}
    ).json().get("customer", {})

    first_name = customer.get("given_name", "")
    last_name = customer.get("family_name", "")
    email = customer.get("email_address", "")
    phone = customer.get("phone_number", "")

    # Push to Zoho CRM
    access_token = get_zoho_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    deal_data = {
        "data": [{
            "Deal_Name": f"{first_name} {last_name} - Booking",
            "Stage": DEFAULT_DEAL_STAGE,
            "Pipeline": DEFAULT_PIPELINE,
            "Email": email,
            "Phone": phone
        }]
    }
    requests.post(f"{ZOHO_CRM_BASE}/crm/v2/Deals", headers=headers, json=deal_data)

# --- Business logic for booking canceled ---
async def handle_booking_canceled(data_object):
    customer_id = data_object.get("booking", {}).get("customer_id")
    if not customer_id:
        return
    # Could add logic to find existing deal and update stage to canceled
    access_token = get_zoho_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    # Search and update logic would go here

# Register BOTH paths so trailing slash doesn’t break it
@app.post("/square/webhook")
async def webhook_no_slash(request: Request):
    return await process_square_webhook(request)

@app.post("/square/webhook/")
async def webhook_with_slash(request: Request):
    return await process_square_webhook(request)
