// ... existing code ...

def find_lead_or_contact(email, phone, headers):
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

@app.post("/square/webhook")
async def square_webhook(req: Request, x_square_signature: str = Header(None)):
    try:
        # ... existing webhook validation code ...

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
