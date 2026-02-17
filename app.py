"""
OrderFloz — Cin7 to HubSpot Sync
================================
- Fetches wholesale orders from Cin7
- Shows what would be synced to HubSpot
- Push orders to HubSpot as Closed Won deals
"""

import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
from pathlib import Path

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="OrderFloz",
    page_icon="🌊",
    layout="wide"
)

# =============================================================================
# ADMIN CONFIGURATION
# =============================================================================
BRANDING_FILE = Path(".orderfloz_branding.json")

def get_admin_emails():
    """Get list of admin emails from secrets."""
    try:
        if hasattr(st, 'secrets') and 'admin' in st.secrets:
            emails = st.secrets.admin.get('emails', [])
            if isinstance(emails, str):
                return [emails]
            return list(emails)
    except:
        pass
    return ["sam@monemtech.com"]  # Default admin

def get_admin_password():
    """Get admin password from secrets."""
    try:
        if hasattr(st, 'secrets') and 'admin' in st.secrets:
            return st.secrets.admin.get('password', 'orderfloz2024')
    except:
        pass
    return "orderfloz2024"  # Default password - CHANGE IN SECRETS

def is_admin_authenticated():
    """Check if current session is admin authenticated."""
    return st.session_state.get('admin_authenticated', False)

def authenticate_admin(email, password):
    """Verify admin credentials."""
    admin_emails = get_admin_emails()
    admin_password = get_admin_password()
    
    if email.lower() in [e.lower() for e in admin_emails] and password == admin_password:
        st.session_state.admin_authenticated = True
        st.session_state.admin_email = email
        return True
    return False

def logout_admin():
    """Log out admin."""
    st.session_state.admin_authenticated = False
    st.session_state.admin_email = None

# =============================================================================
# BRANDING CONFIGURATION
# =============================================================================
def get_default_branding():
    """Return default branding settings."""
    return {
        "company_name": "OrderFloz",
        "logo_url": "",
        "primary_color": "#1a5276",
        "accent_color": "#2ecc71",
        "support_email": "support@orderfloz.com",
        "powered_by": True
    }

def load_branding():
    """Load branding from file, secrets, or defaults (in that order)."""
    defaults = get_default_branding()
    
    # First try branding file (admin edits)
    if BRANDING_FILE.exists():
        try:
            branding = json.loads(BRANDING_FILE.read_text())
            for key, value in defaults.items():
                if key not in branding:
                    branding[key] = value
            return branding
        except:
            pass
    
    # Then try secrets
    try:
        if hasattr(st, 'secrets') and 'branding' in st.secrets:
            branding = dict(st.secrets.branding)
            for key, value in defaults.items():
                if key not in branding:
                    branding[key] = value
            return branding
    except:
        pass
    
    return defaults

def save_branding(branding):
    """Save branding to file."""
    BRANDING_FILE.write_text(json.dumps(branding, indent=2))

def get_branding():
    """Get current branding (from session state or load fresh)."""
    if 'branding' not in st.session_state:
        st.session_state.branding = load_branding()
    return st.session_state.branding

BRANDING = get_branding()

# =============================================================================
# CONSTANTS
# =============================================================================
RETAIL_SOURCES = ['shopify retail', 'shopify', 'web', 'website', 'online', 'retail']
WHOLESALE_SOURCES = ['backend', 'wholesale', 'b2b', 'manual']
CONFIG_FILE = Path(".orderfloz_config.json")

# Status filter: Only import these statuses
IMPORTABLE_STATUSES = ['approved', 'dispatched', 'voided']

# =============================================================================
# SESSION STATE
# =============================================================================
if 'fetched_orders' not in st.session_state:
    st.session_state.fetched_orders = None
if 'fetch_since' not in st.session_state:
    st.session_state.fetch_since = None
if 'fetch_until' not in st.session_state:
    st.session_state.fetch_until = None
if 'selected_import' not in st.session_state:
    st.session_state.selected_import = set()
if 'selected_review' not in st.session_state:
    st.session_state.selected_review = set()
if 'admin_authenticated' not in st.session_state:
    st.session_state.admin_authenticated = False
if 'admin_email' not in st.session_state:
    st.session_state.admin_email = None
if 'branding' not in st.session_state:
    st.session_state.branding = load_branding()

# =============================================================================
# CONFIG FILE HANDLING
# =============================================================================
def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except:
            return {}
    return {}

def save_config(config):
    CONFIG_FILE.write_text(json.dumps(config))

def clear_config():
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()

# =============================================================================
# CLASSIFICATION
# =============================================================================
def classify_order(order: dict) -> str:
    """Classify an order as 'Wholesale' or 'Retail'."""
    source = (order.get('source') or '').lower().strip()
    project = (order.get('projectName') or '').lower().strip()
    company = (order.get('company') or '').strip()
    
    for kw in RETAIL_SOURCES:
        if kw in source or kw in project:
            return 'Retail'
    for kw in WHOLESALE_SOURCES:
        if kw in source or kw in project:
            return 'Wholesale'
    if company and company.upper() not in ['N/A', 'NONE', 'GUEST', 'CUSTOMER']:
        return 'Wholesale'
    return 'Retail'

# =============================================================================
# CIN7 API
# =============================================================================
def test_cin7(username: str, api_key: str) -> tuple:
    try:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={"rows": 1},
            timeout=30
        )
        if r.status_code == 200:
            return True, "Connected"
        elif r.status_code == 401:
            return False, "Invalid credentials"
        else:
            return False, f"Error {r.status_code}"
    except Exception as e:
        return False, str(e)

def fetch_orders(username: str, api_key: str, since: datetime, until: datetime) -> list:
    start_str = since.strftime("%Y-%m-%dT00:00:00Z")
    end_str = until.strftime("%Y-%m-%dT23:59:59Z")
    
    all_orders = []
    page = 1
    
    while True:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={
                "where": f"dispatchedDate >= '{start_str}' AND dispatchedDate <= '{end_str}'",
                "page": page,
                "rows": 250
            },
            timeout=60
        )
        if r.status_code != 200:
            break
        orders = r.json()
        if not orders:
            break
        all_orders.extend(orders)
        if len(orders) < 250:
            break
        page += 1
    
    # Add segment classification
    for o in all_orders:
        o['_segment'] = classify_order(o)
    
    return all_orders

# =============================================================================
# HUBSPOT API - FULL SYNC SYSTEM
# =============================================================================
# Deal stages in HubSpot (default pipeline)
HUBSPOT_STAGE_CLOSED_WON = "closedwon"
HUBSPOT_STAGE_PENDING_PAYMENT = "decisionmakerboughtin"  # Update this to your actual stage ID

def is_paid(order: dict) -> bool:
    """Check if order is fully paid based on multiple possible fields."""
    # Method 1: Check totalOwing (most reliable if present)
    total_owing = order.get('totalOwing')
    if total_owing is not None:
        try:
            return float(total_owing) == 0
        except:
            pass
    
    # Method 2: Check paid field (various formats)
    paid = str(order.get('paid') or '').lower().strip()
    if paid:
        # Handle "PAID: 100%", "100%", "paid", "yes", etc.
        if '100%' in paid or paid == 'paid' or paid == 'yes' or paid == 'true':
            return True
        # Handle "0%", "unpaid", "no", etc.
        if '0%' in paid or paid == 'unpaid' or paid == 'no' or paid == 'false':
            return False
    
    # Method 3: Check paymentStatus field
    payment_status = str(order.get('paymentStatus') or '').lower()
    if 'paid' in payment_status and 'unpaid' not in payment_status:
        return True
    if 'unpaid' in payment_status or 'pending' in payment_status:
        return False
    
    # Method 4: Compare totalPaid vs total
    total_paid = order.get('totalPaid')
    total = order.get('total', 0) or 0
    if total_paid is not None:
        try:
            return float(total_paid) >= float(total) and float(total) > 0
        except:
            pass
    
    # Default: If order is dispatched and we can't determine, assume paid
    # (Most wholesale orders are paid, Net 30 is the exception)
    status = str(order.get('stage') or order.get('status') or '').lower()
    if status == 'dispatched':
        return True
    
    return True  # Default to paid if unknown

def get_payment_debug(order: dict) -> str:
    """Return debug info about payment fields for troubleshooting."""
    fields = []
    if order.get('totalOwing') is not None:
        fields.append(f"owing:{order.get('totalOwing')}")
    if order.get('paid'):
        fields.append(f"paid:{order.get('paid')}")
    if order.get('paymentStatus'):
        fields.append(f"status:{order.get('paymentStatus')}")
    if order.get('totalPaid') is not None:
        fields.append(f"totalPaid:{order.get('totalPaid')}")
    if order.get('paymentTerms'):
        fields.append(f"terms:{order.get('paymentTerms')}")
    return ' | '.join(fields) if fields else 'No payment fields found'

def get_deal_stage(order: dict) -> tuple:
    """Determine HubSpot deal stage based on payment status.
    Returns (stage_id, stage_label)"""
    if is_paid(order):
        return HUBSPOT_STAGE_CLOSED_WON, "Closed Won"
    return HUBSPOT_STAGE_PENDING_PAYMENT, "Pending Payment"

def get_headers(api_key: str) -> dict:
    """Get standard HubSpot API headers."""
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

# -----------------------------------------------------------------------------
# SEARCH FUNCTIONS
# -----------------------------------------------------------------------------
def search_deal_by_order_ref(api_key: str, order_ref: str) -> dict:
    """Search for existing deal by Cin7 order reference. Returns deal dict or None."""
    headers = get_headers(api_key)
    search_url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    
    # Search in deal name (format: "Company - OrderRef" or just "OrderRef")
    search_body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "dealname",
                "operator": "CONTAINS_TOKEN",
                "value": order_ref
            }]
        }],
        "properties": ["dealname", "amount", "dealstage", "pipeline"]
    }
    
    try:
        r = requests.post(search_url, headers=headers, json=search_body, timeout=30)
        if r.status_code == 200:
            results = r.json().get('results', [])
            # Find exact match (order ref should be in deal name)
            for deal in results:
                if order_ref in deal.get('properties', {}).get('dealname', ''):
                    return deal
    except:
        pass
    return None

def search_contact_by_email(api_key: str, email: str) -> dict:
    """Search for existing contact by email. Returns contact dict or None."""
    if not email:
        return None
    
    headers = get_headers(api_key)
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    search_body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "email",
                "operator": "EQ",
                "value": email
            }]
        }],
        "properties": ["email", "firstname", "lastname", "phone", "company"]
    }
    
    try:
        r = requests.post(search_url, headers=headers, json=search_body, timeout=30)
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results:
                return results[0]
    except:
        pass
    return None

def search_company_by_name(api_key: str, company_name: str) -> dict:
    """Search for existing company by name. Returns company dict or None."""
    if not company_name:
        return None
    
    headers = get_headers(api_key)
    search_url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    search_body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "name",
                "operator": "EQ",
                "value": company_name
            }]
        }],
        "properties": ["name", "phone", "address", "city", "state", "zip", "country"]
    }
    
    try:
        r = requests.post(search_url, headers=headers, json=search_body, timeout=30)
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results:
                return results[0]
    except:
        pass
    return None

# -----------------------------------------------------------------------------
# UPDATE FUNCTIONS
# -----------------------------------------------------------------------------
def update_deal(api_key: str, deal_id: str, properties: dict) -> bool:
    """Update deal properties. Returns success boolean."""
    headers = get_headers(api_key)
    url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}"
    
    try:
        r = requests.patch(url, headers=headers, json={"properties": properties}, timeout=30)
        return r.status_code == 200
    except:
        return False

def update_contact(api_key: str, contact_id: str, properties: dict) -> bool:
    """Update contact properties. Returns success boolean."""
    headers = get_headers(api_key)
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    
    try:
        r = requests.patch(url, headers=headers, json={"properties": properties}, timeout=30)
        return r.status_code == 200
    except:
        return False

def update_company(api_key: str, company_id: str, properties: dict) -> bool:
    """Update company properties. Returns success boolean."""
    headers = get_headers(api_key)
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
    
    try:
        r = requests.patch(url, headers=headers, json={"properties": properties}, timeout=30)
        return r.status_code == 200
    except:
        return False

# -----------------------------------------------------------------------------
# CREATE FUNCTIONS
# -----------------------------------------------------------------------------
def create_contact(api_key: str, email: str, first_name: str, last_name: str, 
                   company: str, phone: str = "") -> str:
    """Create new contact. Returns contact ID or None."""
    headers = get_headers(api_key)
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    
    properties = {"email": email}
    if first_name:
        properties["firstname"] = first_name
    if last_name:
        properties["lastname"] = last_name
    if company:
        properties["company"] = company
    if phone:
        properties["phone"] = phone
    
    try:
        r = requests.post(url, headers=headers, json={"properties": properties}, timeout=30)
        if r.status_code == 201:
            return r.json()['id']
    except:
        pass
    return None

def create_company(api_key: str, name: str, phone: str = "", address: str = "",
                   city: str = "", state: str = "", zip_code: str = "", country: str = "") -> str:
    """Create new company. Returns company ID or None."""
    if not name:
        return None
    
    headers = get_headers(api_key)
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    
    properties = {"name": name}
    if phone:
        properties["phone"] = phone
    if address:
        properties["address"] = address
    if city:
        properties["city"] = city
    if state:
        properties["state"] = state
    if zip_code:
        properties["zip"] = zip_code
    if country:
        properties["country"] = country
    
    try:
        r = requests.post(url, headers=headers, json={"properties": properties}, timeout=30)
        if r.status_code == 201:
            return r.json()['id']
    except:
        pass
    return None

def create_deal(api_key: str, order: dict, contact_id: str = None, company_id: str = None) -> tuple:
    """Create new deal. Returns (deal_id, stage_label) or (None, error_message)."""
    headers = get_headers(api_key)
    
    # Get deal stage based on payment status
    stage_id, stage_label = get_deal_stage(order)
    
    # Extract order details
    order_ref = order.get('reference', '')
    company = order.get('company') or order.get('billingCompany') or ''
    total = order.get('total', 0) or 0
    payment_terms = order.get('paymentTerms') or 'Standard'
    total_owing = order.get('totalOwing', total)
    
    # Build deal name
    deal_name = f"{company} - {order_ref}" if company else order_ref
    
    deal_data = {
        "properties": {
            "dealname": deal_name,
            "amount": str(total),
            "dealstage": stage_id,
            "pipeline": "default",
            "description": f"Cin7 Order: {order_ref}\nPayment Terms: {payment_terms}\nOwing: ${total_owing}"
        }
    }
    
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals", 
                         headers=headers, json=deal_data, timeout=30)
        if r.status_code == 201:
            deal_id = r.json()['id']
            
            # Associate with contact
            if contact_id:
                assoc_url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/contacts/{contact_id}/deal_to_contact"
                requests.put(assoc_url, headers=headers, timeout=30)
            
            # Associate with company
            if company_id:
                assoc_url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/companies/{company_id}/deal_to_company"
                requests.put(assoc_url, headers=headers, timeout=30)
            
            return deal_id, stage_label
        else:
            return None, f"Error {r.status_code}: {r.text[:100]}"
    except Exception as e:
        return None, str(e)

# -----------------------------------------------------------------------------
# FULL SYNC FUNCTION
# -----------------------------------------------------------------------------
def sync_order_to_hubspot(api_key: str, order: dict) -> dict:
    """
    Full sync of a single order to HubSpot.
    - Creates or updates deal
    - Creates or updates contact
    - Creates or updates company
    
    Returns result dict with action taken and details.
    """
    result = {
        "order_ref": order.get('reference', 'Unknown'),
        "success": False,
        "action": "none",
        "deal_stage": "",
        "details": []
    }
    
    # Extract order info
    order_ref = order.get('reference', '')
    email = order.get('email') or order.get('memberEmail') or ''
    first_name = order.get('firstName') or order.get('billingFirstName') or ''
    last_name = order.get('lastName') or order.get('billingLastName') or ''
    company_name = order.get('company') or order.get('billingCompany') or ''
    phone = order.get('phone') or order.get('billingPhone') or ''
    total = order.get('total', 0) or 0
    
    # Address fields
    address = order.get('billingAddress1') or order.get('deliveryAddress1') or ''
    city = order.get('billingCity') or order.get('deliveryCity') or ''
    state = order.get('billingState') or order.get('deliveryState') or ''
    zip_code = order.get('billingPostCode') or order.get('deliveryPostCode') or ''
    country = order.get('billingCountry') or order.get('deliveryCountry') or ''
    
    # Get expected deal stage
    expected_stage_id, expected_stage_label = get_deal_stage(order)
    result["deal_stage"] = expected_stage_label
    
    # -------------------------------------------------------------------------
    # STEP 1: Search for existing deal (FIRST - most common case is "already exists")
    # -------------------------------------------------------------------------
    existing_deal = search_deal_by_order_ref(api_key, order_ref)
    
    if existing_deal:
        deal_id = existing_deal['id']
        current_stage = existing_deal.get('properties', {}).get('dealstage', '')
        current_amount = float(existing_deal.get('properties', {}).get('amount', 0) or 0)
        
        # Check if deal needs update
        needs_update = False
        update_props = {}
        
        # Check stage change (Pending Payment → Closed Won when paid)
        if current_stage != expected_stage_id:
            update_props["dealstage"] = expected_stage_id
            needs_update = True
            result["details"].append(f"Stage: {current_stage} → {expected_stage_id}")
        
        # Check amount change
        if abs(current_amount - total) > 0.01:
            update_props["amount"] = str(total)
            needs_update = True
            result["details"].append(f"Amount: ${current_amount:.2f} → ${total:.2f}")
        
        if needs_update:
            if update_deal(api_key, deal_id, update_props):
                result["action"] = "updated"
                result["success"] = True
            else:
                result["action"] = "update_failed"
                result["details"].append("Failed to update deal")
        else:
            result["action"] = "skipped"
            result["success"] = True
            result["details"].append("No changes needed")
        
        # Still sync contact and company even if deal unchanged
        contact_id = None
        company_id = None
        
    else:
        # -------------------------------------------------------------------------
        # STEP 2: Deal not found - need to create
        # -------------------------------------------------------------------------
        contact_id = None
        company_id = None
        
        # -------------------------------------------------------------------------
        # STEP 2a: Sync contact (most orders are Closed Won, so process quickly)
        # -------------------------------------------------------------------------
        if email:
            existing_contact = search_contact_by_email(api_key, email)
            
            if existing_contact:
                contact_id = existing_contact['id']
                # Check if contact needs update
                props = existing_contact.get('properties', {})
                update_props = {}
                
                if first_name and props.get('firstname', '') != first_name:
                    update_props['firstname'] = first_name
                if last_name and props.get('lastname', '') != last_name:
                    update_props['lastname'] = last_name
                if phone and props.get('phone', '') != phone:
                    update_props['phone'] = phone
                if company_name and props.get('company', '') != company_name:
                    update_props['company'] = company_name
                
                if update_props:
                    update_contact(api_key, contact_id, update_props)
                    result["details"].append("Contact updated")
                else:
                    result["details"].append("Contact unchanged")
            else:
                contact_id = create_contact(api_key, email, first_name, last_name, company_name, phone)
                if contact_id:
                    result["details"].append("Contact created")
                else:
                    result["details"].append("Contact creation failed")
        
        # -------------------------------------------------------------------------
        # STEP 2b: Sync company
        # -------------------------------------------------------------------------
        if company_name:
            existing_company = search_company_by_name(api_key, company_name)
            
            if existing_company:
                company_id = existing_company['id']
                # Check if company needs update
                props = existing_company.get('properties', {})
                update_props = {}
                
                if phone and props.get('phone', '') != phone:
                    update_props['phone'] = phone
                if address and props.get('address', '') != address:
                    update_props['address'] = address
                if city and props.get('city', '') != city:
                    update_props['city'] = city
                if state and props.get('state', '') != state:
                    update_props['state'] = state
                if zip_code and props.get('zip', '') != zip_code:
                    update_props['zip'] = zip_code
                
                if update_props:
                    update_company(api_key, company_id, update_props)
                    result["details"].append("Company updated")
                else:
                    result["details"].append("Company unchanged")
            else:
                company_id = create_company(api_key, company_name, phone, address, city, state, zip_code, country)
                if company_id:
                    result["details"].append("Company created")
                else:
                    result["details"].append("Company creation failed")
        
        # -------------------------------------------------------------------------
        # STEP 2c: Create deal
        # -------------------------------------------------------------------------
        deal_id, stage_or_error = create_deal(api_key, order, contact_id, company_id)
        
        if deal_id:
            result["action"] = "created"
            result["success"] = True
            result["details"].append(f"Deal created as {stage_or_error}")
        else:
            result["action"] = "create_failed"
            result["details"].append(f"Deal creation failed: {stage_or_error}")
    
    return result

def push_orders_to_hubspot(api_key: str, orders: list, progress_callback=None) -> dict:
    """
    Full sync of multiple orders to HubSpot.
    Returns detailed results dict.
    """
    results = {
        "created": [],
        "updated": [],
        "skipped": [],
        "failed": [],
        "closed_won": 0,
        "pending_payment": 0
    }
    
    for i, order in enumerate(orders):
        sync_result = sync_order_to_hubspot(api_key, order)
        order_ref = sync_result["order_ref"]
        
        # Categorize result
        if sync_result["success"]:
            if sync_result["action"] == "created":
                results["created"].append(sync_result)
            elif sync_result["action"] == "updated":
                results["updated"].append(sync_result)
            else:  # skipped
                results["skipped"].append(sync_result)
            
            # Count by stage
            if sync_result["deal_stage"] == "Closed Won":
                results["closed_won"] += 1
            else:
                results["pending_payment"] += 1
        else:
            results["failed"].append(sync_result)
        
        if progress_callback:
            progress_callback((i + 1) / len(orders))
    
    return results

def test_hubspot(api_key: str) -> tuple:
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 1},
            timeout=30
        )
        if r.status_code == 200:
            return True, "Connected"
        elif r.status_code == 401:
            return False, "Invalid API key"
        else:
            return False, f"Error {r.status_code}"
    except Exception as e:
        return False, str(e)

def fetch_pipeline_stages(api_key: str) -> list:
    """Fetch all deal pipelines and their stages from HubSpot."""
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/pipelines/deals",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30
        )
        if r.status_code == 200:
            return r.json().get('results', [])
    except:
        pass
    return []

# =============================================================================
# FILTER ORDERS
# =============================================================================
def filter_orders(orders: list, exclude_shopify: bool) -> tuple:
    """
    Filter orders into: to_import, to_review, to_skip
    
    Filter Logic (in order):
    1. Retail segment → Skip
    2. Company or email contains "vivant" → Review (internal/test)
    3. Shopify Retail source (if checkbox) → Skip
    4. Status not Approved/Dispatched/Voided → Skip
    5. No email + Dispatched → Review (likely employee)
    6. $0 value + Has email → Import (client sample/promo)
    7. $0 value + No email → Review (likely employee)
    8. Has value + Has email → Import
    """
    to_import = []
    to_review = []
    to_skip = []
    
    for o in orders:
        source = (o.get('source') or '').lower()
        total = o.get('total', 0) or 0
        segment = o.get('_segment', 'Retail')
        status = (o.get('stage') or o.get('status') or '').lower()
        company = (o.get('company') or o.get('billingCompany') or '').lower()
        has_email = bool((o.get('email') or o.get('memberEmail') or '').strip())
        
        # 1. Skip retail
        if segment == 'Retail':
            o['_skip_reason'] = 'Retail segment'
            to_skip.append(o)
            continue
        
        # 2. Review internal/test orders (Vivant in company name or email)
        email_address = (o.get('email') or o.get('memberEmail') or '').lower()
        if 'vivant' in company or 'vivant' in email_address:
            o['_review_reason'] = 'Internal (Vivant in company or email)'
            to_review.append(o)
            continue
        
        # 3. Skip excluded sources
        if exclude_shopify and 'shopify retail' in source:
            o['_skip_reason'] = 'Shopify Retail excluded'
            to_skip.append(o)
            continue
        
        # 4. Check status - only import Approved, Dispatched, and Voided
        if status not in IMPORTABLE_STATUSES:
            o['_skip_reason'] = f'Status: {status}'
            to_skip.append(o)
            continue
        
        # 5. No email + Dispatched = likely employee order, send to review
        if not has_email and status == 'dispatched':
            o['_review_reason'] = 'No email (likely employee)'
            to_review.append(o)
            continue
        
        # 6 & 7. Handle $0 orders
        if total == 0:
            if has_email:
                # $0 + email = client order (sample/promo), import it
                to_import.append(o)
            else:
                # $0 + no email = likely employee, review
                o['_review_reason'] = '$0 + No email (likely employee)'
                to_review.append(o)
            continue
        
        # 8. Has value + has email = import
        to_import.append(o)
    
    return to_import, to_review, to_skip

# =============================================================================
# DISPLAY HELPERS
# =============================================================================
def order_to_summary(order: dict, include_reason: bool = False) -> dict:
    """Convert order to display format with raw numbers for proper sorting."""
    total = order.get('total', 0) or 0
    
    # Get customer name from multiple possible fields
    customer = (
        order.get('customerName') or 
        order.get('contactName') or 
        order.get('billingName') or
        order.get('deliveryName') or
        order.get('memberName') or
        order.get('contact') or
        ''
    )
    
    # If still empty, try firstName + lastName
    if not customer:
        first = order.get('firstName') or order.get('billingFirstName') or ''
        last = order.get('lastName') or order.get('billingLastName') or ''
        customer = f"{first} {last}".strip()
    
    result = {
        'Order #': order.get('reference', ''),
        'Source': order.get('source', ''),
        'Segment': order.get('_segment', ''),
        'Total_Numeric': float(total),  # Hidden column for sorting
        'Total': float(total),  # Display column
        'Company': order.get('company') or order.get('billingCompany') or '',
        'Customer': customer,
        'Email': order.get('email') or order.get('memberEmail') or '',
        'Order Date': (order.get('createdDate') or '')[:10],
        'Dispatched': (order.get('dispatchedDate') or '')[:10],
        'Payment': '✅ Paid' if is_paid(order) else '⏳ Unpaid',
        'Pay Debug': get_payment_debug(order),  # TEMP: Debug column
        'Deal Stage': get_deal_stage(order)[1],  # "Closed Won" or "Pending Payment"
        'Status': order.get('stage') or order.get('status') or '',
    }
    
    if include_reason:
        result['Reason'] = order.get('_review_reason') or order.get('_skip_reason') or ''
    
    return result

def prepare_dataframe(orders: list, include_reason: bool = False) -> pd.DataFrame:
    """Create DataFrame from orders, properly sorted by numeric Total."""
    if not orders:
        return pd.DataFrame()
    
    df = pd.DataFrame([order_to_summary(o, include_reason) for o in orders])
    
    # Ensure Total is numeric
    df['Total_Numeric'] = pd.to_numeric(df['Total_Numeric'], errors='coerce').fillna(0)
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    
    # Sort by numeric value (descending)
    df = df.sort_values('Total_Numeric', ascending=False)
    
    # Drop the helper column
    df = df.drop(columns=['Total_Numeric'])
    
    return df

def get_column_config():
    """Column configuration for currency formatting."""
    return {
        'Total': st.column_config.NumberColumn(
            'Total',
            format='$ %.2f'
        )
    }

# =============================================================================
# MAIN APP
# =============================================================================
def main():
    # Get current branding (may have been updated by admin)
    branding = get_branding()
    
    # Header with branding - centered logo
    if branding.get("logo_url"):
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            try:
                st.image(branding["logo_url"], width=120)
            except:
                pass
    
    # Centered title
    st.markdown(f"<h1 style='text-align: center;'>{branding['company_name']}</h1>", unsafe_allow_html=True)
    
    st.subheader("Cin7 → HubSpot Order Sync")
    st.info("**Full Sync:** Creates/updates deals, contacts & companies. Paid → **Closed Won**. Unpaid → **Pending Payment**.")
    
    # -------------------------------------------------------------------------
    # SIDEBAR
    # -------------------------------------------------------------------------
    with st.sidebar:
        config = load_config()
        
        st.header("🔌 Connections")
        
        # Cin7
        st.subheader("Cin7 Omni")
        cin7_user = st.text_input("Username", value=config.get('cin7_username', ''))
        cin7_key = st.text_input("API Key", type="password", value=config.get('cin7_api_key', ''))
        
        if st.button("Test Cin7"):
            if cin7_user and cin7_key:
                ok, msg = test_cin7(cin7_user, cin7_key)
                if ok:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
            else:
                st.error("Enter credentials")
        
        cin7_ok, _ = test_cin7(cin7_user, cin7_key) if cin7_user and cin7_key else (False, "")
        st.caption(f"Status: {'✅ Connected' if cin7_ok else '❌ Not connected'}")
        
        st.divider()
        
        # HubSpot
        st.subheader("HubSpot")
        hs_key = st.text_input("Private App Token", type="password", value=config.get('hubspot_api_key', ''))
        
        if st.button("Test HubSpot"):
            if hs_key:
                ok, msg = test_hubspot(hs_key)
                if ok:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
            else:
                st.error("Enter API key")
        
        hs_ok, _ = test_hubspot(hs_key) if hs_key else (False, "")
        st.caption(f"Status: {'✅ Connected' if hs_ok else '❌ Not connected'}")
        
        st.divider()
        
        # Filters
        st.header("⚙️ Filters")
        exclude_shopify = st.checkbox("Exclude 'Shopify Retail'", value=True)
        
        st.divider()
        
        # Remember credentials
        remember = st.checkbox("🔑 Remember credentials", value=config.get('remember', False), help="Save credentials locally")
        if remember:
            save_config({
                'cin7_username': cin7_user,
                'cin7_api_key': cin7_key,
                'hubspot_api_key': hs_key,
                'remember': True
            })
            st.caption("✅ Credentials saved locally")
        else:
            if config.get('remember'):
                clear_config()
        
        st.divider()
        
        # Admin Settings
        with st.expander("🔐 Admin Settings"):
            if is_admin_authenticated():
                st.success(f"✅ Logged in as {st.session_state.get('admin_email', 'Admin')}")
                
                if st.button("🚪 Logout"):
                    logout_admin()
                    st.rerun()
                
                st.divider()
                st.subheader("🎨 Branding")
                
                # Load current branding
                current_branding = get_branding()
                
                # Editable fields
                new_company = st.text_input("Company Name", value=current_branding.get('company_name', ''))
                new_logo = st.text_input("Logo URL", value=current_branding.get('logo_url', ''), help="URL to your logo image")
                new_color = st.color_picker("Primary Color", value=current_branding.get('primary_color', '#1a5276'))
                new_email = st.text_input("Support Email", value=current_branding.get('support_email', ''))
                new_powered = st.checkbox("Show 'Powered by OrderFloz'", value=current_branding.get('powered_by', True))
                
                # Preview logo if URL provided
                if new_logo:
                    st.caption("Logo preview:")
                    try:
                        st.image(new_logo, width=80)
                    except:
                        st.warning("⚠️ Could not load logo from URL")
                
                if st.button("💾 Save Branding", type="primary"):
                    updated_branding = {
                        "company_name": new_company,
                        "logo_url": new_logo,
                        "primary_color": new_color,
                        "accent_color": current_branding.get('accent_color', '#2ecc71'),
                        "support_email": new_email,
                        "powered_by": new_powered
                    }
                    save_branding(updated_branding)
                    st.session_state.branding = updated_branding
                    st.success("✅ Branding saved!")
                    st.rerun()
                
                st.divider()
                st.subheader("🔧 HubSpot Pipeline Stages")
                st.caption("View your HubSpot deal stages to configure the connector")
                
                if hs_key:
                    if st.button("🔍 Fetch Pipeline Stages"):
                        pipelines = fetch_pipeline_stages(hs_key)
                        if pipelines:
                            for pipeline in pipelines:
                                st.markdown(f"**📊 {pipeline['label']}** (ID: `{pipeline['id']}`)")
                                stage_data = []
                                for stage in pipeline.get('stages', []):
                                    stage_data.append({
                                        "Order": stage['displayOrder'],
                                        "Stage Name": stage['label'],
                                        "Stage ID": stage['id']
                                    })
                                st.dataframe(pd.DataFrame(stage_data), use_container_width=True, hide_index=True)
                            
                            st.info("""
                            **Current configuration:**
                            - Closed Won: `closedwon`
                            - Pending Payment: `decisionmakerboughtin`
                            
                            If your stage IDs are different, update `HUBSPOT_STAGE_CLOSED_WON` and `HUBSPOT_STAGE_PENDING_PAYMENT` in the code.
                            """)
                        else:
                            st.warning("Could not fetch pipelines. Check your API key.")
                else:
                    st.warning("Enter HubSpot API key first")
                
            else:
                st.caption("Admin login required to edit settings")
                admin_email = st.text_input("Email", key="admin_email_input")
                admin_password = st.text_input("Password", type="password", key="admin_password_input")
                
                if st.button("🔑 Login"):
                    if authenticate_admin(admin_email, admin_password):
                        st.success("✅ Logged in!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials")
    
    # -------------------------------------------------------------------------
    # MAIN CONTENT
    # -------------------------------------------------------------------------
    st.header("📅 Select Dispatched Date Range")
    st.caption("Fetches orders that were **dispatched** within this date range (not created date)")
    col1, col2 = st.columns(2)
    with col1:
        since_date = st.date_input("From", value=datetime.now() - timedelta(days=7))
    with col2:
        until_date = st.date_input("To", value=datetime.now())
    
    since = datetime.combine(since_date, datetime.min.time())
    until = datetime.combine(until_date, datetime.max.time())
    
    # Fetch button
    if st.button("🔄 Fetch Orders (Read Only)", type="primary", use_container_width=True):
        if not cin7_user or not cin7_key:
            st.error("Enter Cin7 credentials in sidebar")
        else:
            with st.spinner("Fetching orders from Cin7..."):
                orders = fetch_orders(cin7_user, cin7_key, since, until)
            st.session_state.fetched_orders = orders
            st.session_state.fetch_since = since_date
            st.session_state.fetch_until = until_date
            # Reset selections
            st.session_state.selected_import = set()
            st.session_state.selected_review = set()
            st.success(f"Fetched {len(orders)} orders")
    
    # -------------------------------------------------------------------------
    # RESULTS (from session state)
    # -------------------------------------------------------------------------
    orders = st.session_state.fetched_orders
    if orders is None:
        st.caption("👆 Select a date range and click Fetch Orders")
        return
    
    st.caption(f"🟢 {len(orders)} orders loaded (dispatched between {st.session_state.fetch_since} and {st.session_state.fetch_until})")
    
    # Filter orders
    to_import, to_review, to_skip = filter_orders(orders, exclude_shopify)
    
    # Separate retail from other skipped
    retail_orders = [o for o in to_skip if o.get('_segment') == 'Retail']
    other_skipped = [o for o in to_skip if o.get('_segment') != 'Retail']
    
    # Revenue calculations
    import_revenue = sum(o.get('total', 0) or 0 for o in to_import)
    review_revenue = sum(o.get('total', 0) or 0 for o in to_review)
    retail_revenue = sum(o.get('total', 0) or 0 for o in retail_orders)
    
    # Initialize selections (import orders pre-selected, review orders not)
    import_refs = {o.get('reference') for o in to_import}
    review_refs = {o.get('reference') for o in to_review}
    
    # If selections are empty, pre-select all import orders
    if not st.session_state.selected_import and to_import:
        st.session_state.selected_import = import_refs.copy()
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # METRICS SUMMARY
    # -------------------------------------------------------------------------
    st.header("📊 Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Ready to Import", len(to_import), delta=f"${import_revenue:,.0f}")
    with col2:
        st.metric("Needs Review", len(to_review), delta=f"${review_revenue:,.0f}")
    with col3:
        st.metric("Retail (view only)", len(retail_orders), delta=f"${retail_revenue:,.0f}")
    with col4:
        st.metric("Skipped", len(other_skipped))
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # SECTION 1: READY TO IMPORT (pre-selected)
    # -------------------------------------------------------------------------
    st.header(f"✅ Ready to Import ({len(to_import)} orders)")
    st.caption("These orders passed all filters and are pre-selected for import. Click column headers to sort.")
    
    if to_import:
        # Select All / Deselect All buttons
        col1, col2, col3 = st.columns([1, 1, 4])
        with col1:
            if st.button("Select All", key="select_all_import"):
                st.session_state.selected_import = import_refs.copy()
                st.rerun()
        with col2:
            if st.button("Deselect All", key="deselect_all_import"):
                st.session_state.selected_import = set()
                st.rerun()
        
        # Create dataframe with Select column
        df_import = prepare_dataframe(to_import)
        df_import.insert(0, 'Select', df_import['Order #'].apply(lambda x: x in st.session_state.selected_import))
        
        # Editable dataframe
        edited_import = st.data_editor(
            df_import,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Select': st.column_config.CheckboxColumn('Select', default=True),
                'Total': st.column_config.NumberColumn('Total', format='$ %.2f'),
                'Deal Stage': st.column_config.TextColumn('Deal Stage', width='small')
            },
            disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer', 'Email', 'Order Date', 'Dispatched', 'Payment', 'Pay Debug', 'Deal Stage', 'Status'],
            key="import_editor"
        )
        
        # Update selections based on edits
        st.session_state.selected_import = set(edited_import[edited_import['Select']]['Order #'].tolist())
        
        # Show count of selected
        selected_import_count = len(st.session_state.selected_import & import_refs)
        selected_import_total = sum(
            (o.get('total', 0) or 0) 
            for o in to_import 
            if o.get('reference') in st.session_state.selected_import
        )
        st.caption(f"✓ {selected_import_count} of {len(to_import)} selected (${selected_import_total:,.2f})")
    else:
        st.info("No orders ready to import")
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # SECTION 2: NEEDS REVIEW (not pre-selected, collapsed by default)
    # -------------------------------------------------------------------------
    with st.expander(f"⚠️ Needs Review ({len(to_review)} orders) — Click to expand"):
        st.caption("These orders need manual review before import — check the box to include. Click column headers to sort.")
        
        if to_review:
            # Select All / Deselect All buttons
            col1, col2, col3 = st.columns([1, 1, 4])
            with col1:
                if st.button("Select All", key="select_all_review"):
                    st.session_state.selected_review = review_refs.copy()
                    st.rerun()
            with col2:
                if st.button("Deselect All", key="deselect_all_review"):
                    st.session_state.selected_review = set()
                    st.rerun()
            
            # Create dataframe with Select column and Reason
            df_review = prepare_dataframe(to_review, include_reason=True)
            df_review.insert(0, 'Select', df_review['Order #'].apply(lambda x: x in st.session_state.selected_review))
            
            # Editable dataframe
            edited_review = st.data_editor(
                df_review,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'Select': st.column_config.CheckboxColumn('Select', default=False),
                    'Total': st.column_config.NumberColumn('Total', format='$ %.2f'),
                    'Deal Stage': st.column_config.TextColumn('Deal Stage', width='small'),
                    'Reason': st.column_config.TextColumn('Reason', width='medium')
                },
                disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer', 'Email', 'Order Date', 'Dispatched', 'Payment', 'Pay Debug', 'Deal Stage', 'Status', 'Reason'],
                key="review_editor"
            )
            
            # Update selections based on edits
            st.session_state.selected_review = set(edited_review[edited_review['Select']]['Order #'].tolist())
            
            # Show count of selected
            selected_review_count = len(st.session_state.selected_review & review_refs)
            selected_review_total = sum(
                (o.get('total', 0) or 0) 
                for o in to_review 
                if o.get('reference') in st.session_state.selected_review
            )
            st.caption(f"✓ {selected_review_count} of {len(to_review)} selected (${selected_review_total:,.2f})")
        else:
            st.info("No orders need review")
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # PUSH BUTTON
    # -------------------------------------------------------------------------
    all_selected = (st.session_state.selected_import & import_refs) | (st.session_state.selected_review & review_refs)
    total_selected = len(all_selected)
    total_selected_revenue = sum(
        (o.get('total', 0) or 0) 
        for o in to_import + to_review 
        if o.get('reference') in all_selected
    )
    
    st.header("🚀 Sync to HubSpot")
    
    if total_selected > 0:
        # Get selected orders
        selected_orders = [o for o in to_import + to_review if o.get('reference') in all_selected]
        
        # Count by deal stage (using payment status, not terms)
        closed_won_count = sum(1 for o in selected_orders if is_paid(o))
        pending_count = total_selected - closed_won_count
        
        st.success(f"**{total_selected} orders selected** — Total: ${total_selected_revenue:,.2f}")
        
        # Show breakdown by deal stage
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Closed Won", closed_won_count, help="Fully paid orders")
        with col2:
            st.metric("Pending Payment", pending_count, help="Unpaid orders (Net terms)")
        
        if pending_count > 0:
            st.info(f"ℹ️ {pending_count} orders have outstanding balances and will sync as **Pending Payment** deals.")
        
        st.caption("**Full Sync:** Creates new deals, updates existing deals, syncs contacts & companies.")
        
        # Confirmation checkbox
        confirm = st.checkbox(f"I confirm I want to sync {total_selected} orders to HubSpot", key="confirm_push")
        
        if st.button(
            f"🔄 SYNC {total_selected} ORDERS TO HUBSPOT (${total_selected_revenue:,.0f})",
            type="primary",
            use_container_width=True,
            disabled=not confirm or not hs_key
        ):
            if not hs_key:
                st.error("Enter HubSpot API key in sidebar")
            else:
                # Push to HubSpot with progress bar
                progress_bar = st.progress(0, text="Syncing orders to HubSpot...")
                
                def update_progress(pct):
                    progress_bar.progress(pct, text=f"Syncing orders... {int(pct*100)}%")
                
                results = push_orders_to_hubspot(hs_key, selected_orders, update_progress)
                
                progress_bar.empty()
                
                # Show results summary
                created_count = len(results["created"])
                updated_count = len(results["updated"])
                skipped_count = len(results["skipped"])
                failed_count = len(results["failed"])
                
                st.markdown(f"""
                ### Sync Results
                
                | Action | Count | Description |
                |--------|-------|-------------|
                | ✅ Created | {created_count} | New deals created |
                | 🔄 Updated | {updated_count} | Existing deals updated |
                | ⏭️ Skipped | {skipped_count} | No changes needed |
                | ❌ Failed | {failed_count} | Errors occurred |
                
                | Stage | Count |
                |-------|-------|
                | Closed Won | {results['closed_won']} |
                | Pending Payment | {results['pending_payment']} |
                """)
                
                # Show details for updated orders (most interesting)
                if results["updated"]:
                    with st.expander(f"🔄 Updated Deals ({updated_count})"):
                        for item in results["updated"]:
                            st.caption(f"• **{item['order_ref']}**: {', '.join(item['details'])}")
                
                # Show details for failed orders
                if results["failed"]:
                    with st.expander(f"❌ Failed ({failed_count})", expanded=True):
                        for item in results["failed"][:10]:
                            st.caption(f"• **{item['order_ref']}**: {', '.join(item['details'])}")
                        if failed_count > 10:
                            st.caption(f"...and {failed_count - 10} more")
                
                if failed_count == 0:
                    st.balloons()
    else:
        st.warning("No orders selected. Check orders above to include them in the sync.")
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # SECTION 3: RETAIL (collapsed, view only)
    # -------------------------------------------------------------------------
    with st.expander(f"🛍️ Retail Orders ({len(retail_orders)}) — View Only"):
        st.caption("Retail orders are shown for reference only and cannot be imported")
        if retail_orders:
            df_retail = prepare_dataframe(retail_orders)
            st.dataframe(df_retail, use_container_width=True, hide_index=True, column_config=get_column_config())
        else:
            st.info("No retail orders")
    
    # -------------------------------------------------------------------------
    # SECTION 4: SKIPPED (collapsed, view only)
    # -------------------------------------------------------------------------
    with st.expander(f"⏭️ Skipped Orders ({len(other_skipped)}) — View Only"):
        st.caption("These orders were skipped due to filters (internal orders, wrong status, etc.)")
        if other_skipped:
            df_skip = prepare_dataframe(other_skipped, include_reason=True)
            st.dataframe(df_skip, use_container_width=True, hide_index=True, column_config=get_column_config())
        else:
            st.info("No skipped orders")
    
    # -------------------------------------------------------------------------
    # STATUS & SOURCE BREAKDOWN
    # -------------------------------------------------------------------------
    with st.expander("📊 Source Breakdown"):
        source_data = {}
        for o in orders:
            src = o.get('source') or 'Unknown'
            seg = o.get('_segment', 'Unknown')
            key = (src, seg)
            if key not in source_data:
                source_data[key] = {'Source': src, 'Segment': seg, 'Count': 0, 'Revenue': 0}
            source_data[key]['Count'] += 1
            source_data[key]['Revenue'] += o.get('total', 0) or 0
        
        df_source = pd.DataFrame(source_data.values())
        if not df_source.empty:
            df_source = df_source.sort_values('Count', ascending=False)
            st.dataframe(
                df_source, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    'Revenue': st.column_config.NumberColumn('Revenue', format='$ %.2f')
                }
            )
    
    with st.expander("📋 Status Breakdown"):
        status_data = {}
        for o in orders:
            status = o.get('stage') or o.get('status') or 'Unknown'
            seg = o.get('_segment', 'Unknown')
            key = (status, seg)
            if key not in status_data:
                status_data[key] = {'Status': status, 'Segment': seg, 'Count': 0, 'Revenue': 0}
            status_data[key]['Count'] += 1
            status_data[key]['Revenue'] += o.get('total', 0) or 0
        
        df_status = pd.DataFrame(status_data.values())
        if not df_status.empty:
            df_status = df_status.sort_values('Count', ascending=False)
            st.dataframe(
                df_status, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    'Revenue': st.column_config.NumberColumn('Revenue', format='$ %.2f')
                }
            )
            
            st.caption("✅ **Importable statuses**: Approved, Dispatched, Voided")
            st.caption("❌ **Skipped statuses**: Draft, Pending, New, and all others")
    
    # -------------------------------------------------------------------------
    # FILTER LOGIC REFERENCE
    # -------------------------------------------------------------------------
    with st.expander("📖 Filter Logic Reference"):
        st.markdown("""
        **Orders are processed in this order:**
        
        | Step | Condition | Result |
        |------|-----------|--------|
        | 1 | Retail segment | ❌ Skip |
        | 2 | Company contains "vivant" | ❌ Skip (internal) |
        | 3 | Shopify Retail source | ❌ Skip (if enabled) |
        | 4 | Status not Approved/Dispatched/Voided | ❌ Skip |
        | 5 | No email + Dispatched | ⚠️ Review (likely employee) |
        | 6 | $0 value + Has email | ✅ Import (client sample) |
        | 7 | $0 value + No email | ⚠️ Review (likely employee) |
        | 8 | Has value + Has email | ✅ Import |
        """)
    
    st.divider()
    # Footer with branding (use branding variable from main())
    branding = get_branding()
    footer_text = f"**{branding['company_name']}** — Cin7 to HubSpot order sync"
    if branding.get("powered_by", True):
        footer_text += " | Powered by OrderFloz"
    if branding.get("support_email"):
        footer_text += f" | {branding['support_email']}"
    st.caption(footer_text)

if __name__ == "__main__":
    main()
