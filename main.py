import os
import requests
from fastapi import FastAPI, Request, Header, HTTPException
from datetime import datetime

app = FastAPI()

ZOHO_CLIENT_ID = os.environ["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET = os.environ["ZOHO_CLIENT_SECRET"]
ZOHO_REFRESH_TOKEN = os.environ["ZOHO_REFRESH_TOKEN"]
ZOHO_API_DOMAIN = "https://www.zohoapis.com"
SQUARE_WEBHOOK_KEY = os.environ["SQUARE_WEBHOOK_KEY"]
MODULE = "Leads"
CUSTOM_SQUARE_ID_FIELD = "Square_Meeting_ID"


def zoho_access_token():
    url = f"https://accounts.zoho.com/oauth/v2/token?refresh_token={ZOHO_REFRESH_TOKEN}&client_id={ZOHO_CLIENT_ID}&client_secret={ZOHO_CLIENT_SECRET}&grant_type=refresh_token"
    r = requests.post(url)
    try:
        return r.json()["access_token"]
    except:
        raise RuntimeError(f"Zoho token error: {r.text}")


@app.get("/", status_code=200)
def g():
    return {"ok": True}


@app.post("/square/webhook")
async def square_webhook(request: Request, x_square_signature: str = Header(None)):
    if x_square_signature != SQUARE_WEBHOOK_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await request.json()
    booking = body.get("data", {}).get("object", {}).get("booking", {})
    if not booking:
        return {"status": "ignored"}

    customer_id = booking.get("customer_id")
    location_id = booking.get("location_id")
    booking_id = booking.get("id")
    start_at = booking.get("start_at")
    team_member_id = booking.get("appointment_segments", [{}])[0].get("team_member_id")

    square_token = os.environ["SQUARE_API_KEY"]
    square_headers = {"Authorization": f"Bearer {square_token}"}

    cust = requests.get(
        f"https://connect.squareup.com/v2/customers/{customer_id}",
        headers=square_headers,
        timeout=15,
    ).json().get("customer", {})

    email = cust.get("email_address")
    phone = cust.get("phone_number")
    full_name = cust.get("given_name", "") + " " + cust.get("family_name", "")
    address_data = cust.get("address", {})
    address = ", ".join([v for k, v in address_data.items() if v])

    if not email or not phone or not full_name.strip():
        return {"status": "missing required fields"}

    phone = "+" + ''.join(filter(str.isdigit, phone))
    
    token = zoho_access_token()
    zhdr = {"Authorization": f"Zoho-oauthtoken {token}", "Content-Type": "application/json"}

    def find_record():
        for identifier, value in [("Email", email), ("Phone", phone)]:
            r = requests.get(
                f"{ZOHO_API_DOMAIN}/crm/v2/{MODULE}/search?criteria=({identifier}:equals:{value})",
                headers=zhdr,
                timeout=15,
            )
            data = r.json()
            if data.get("data"):
                return data["data"][0]
        return None

    lead = find_record()

    if not lead:
        payload = {
            "data": [
                {
                    "Last_Name": cust.get("family_name") or full_name,
                    "First_Name": cust.get("given_name", ""),
                    "Phone": phone,
                    "Email": email,
                    "Full_Name": full_name,
                    "Street": address,
                }
            ]
        }
        r = requests.post(
            f"{ZOHO_API_DOMAIN}/crm/v2/{MODULE}",
            headers=zhdr,
            json=payload,
            timeout=15,
        ).json()
        lead = r.get("data", [{}])[0]

    lead_id = lead["id"]

    # Prevent duplicates: check if meeting with same Square ID exists
    search_resp = requests.get(
        f"{ZOHO_API_DOMAIN}/crm/v2/Events/search?criteria=({CUSTOM_SQUARE_ID_FIELD}:equals:{booking_id})",
        headers=zhdr,
        timeout=15,
    ).json()

    if search_resp.get("data"):
        return {"status": "meeting already exists"}

    title = f"Himplant Virtual Consultation with {location_id}, {full_name}"
    evt = {
        "data": [
            {
                "Event_Title": title,
                "Start_DateTime": start_at,
                "End_DateTime": start_at,
                "Who_Id": lead_id,
                CUSTOM_SQUARE_ID_FIELD: booking_id,
                "Event_Type": "Virtual Consultation",
            }
        ]
    }

    requests.post(
        f"{ZOHO_API_DOMAIN}/crm/v2/Events",
        headers=zhdr,
        json=evt,
        timeout=15,
    )

    return {"status": "success"}
