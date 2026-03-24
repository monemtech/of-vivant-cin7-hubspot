"""
OrderFloz - Cin7 to HubSpot Sync v.20260324
================================
- Fetches wholesale orders from Cin7
- Shows what would be synced to HubSpot
- Push orders to HubSpot as Closed Won deals

UPDATED: Now uses orderDate (actual sale date) instead of createdDate
UPDATED: Rep column in order table for owner troubleshooting
UPDATED: Repairs missing company/contact associations on existing deals
UPDATED: Pre-sync duplicate scan with user confirmation before any writes
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
if 'dupe_scan_results' not in st.session_state:
    st.session_state.dupe_scan_results = None    # None = not yet scanned
if 'dupe_scan_order_set' not in st.session_state:
    st.session_state.dupe_scan_order_set = set() # tracks which order set was scanned
if 'dupes_to_delete' not in st.session_state:
    st.session_state.dupes_to_delete = {}        # order_ref -> [deal_ids to delete]

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
            timeout=15
        )
        if r.status_code == 200:
            return True, "Connected"
        elif r.status_code == 401:
            return False, "Invalid credentials"
        else:
            return False, f"Error {r.status_code}"
    except Exception as e:
        return False, str(e)

def fetch_orders(username: str, api_key: str, since: datetime, until: datetime, 
                 progress_callback=None) -> list:
    """Fetch orders from Cin7 with optional progress updates."""
    start_str = since.strftime("%Y-%m-%dT00:00:00Z")
    end_str = until.strftime("%Y-%m-%dT23:59:59Z")
    
    all_orders = []
    page = 1
    
    while True:
        if progress_callback:
            progress_callback(
                phase="fetching",
                page=page,
                orders_so_far=len(all_orders),
                message=f"Fetching page {page}..." if page > 1 else "Connecting to Cin7..."
            )
        
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
        
        if progress_callback:
            progress_callback(
                phase="fetching",
                page=page,
                orders_so_far=len(all_orders),
                message=f"Found {len(all_orders)} orders..."
            )
        
        if len(orders) < 250:
            break
        page += 1
    
    # Add segment classification
    if progress_callback:
        progress_callback(
            phase="processing",
            page=page,
            orders_so_far=len(all_orders),
            message="Classifying orders..."
        )
    
    for o in all_orders:
        o['_segment'] = classify_order(o)
    
    if progress_callback:
        progress_callback(
            phase="complete",
            page=page,
            orders_so_far=len(all_orders),
            message=f"Complete! {len(all_orders)} orders loaded."
        )
    
    return all_orders

def fetch_order_details(username: str, api_key: str, order_id: str) -> dict:
    """
    Fetch detailed order info from Cin7 including line items.
    The list API doesn't return line items, so we need to fetch individual orders.
    """
    # Approach 1: Direct ID endpoint
    try:
        r = requests.get(
            f"https://api.cin7.com/api/v1/SalesOrders/{order_id}",
            auth=(username, api_key),
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            elif isinstance(data, dict) and data:
                return data
    except:
        pass
    
    # Approach 2: Query with id filter
    try:
        r = requests.get(
            f"https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={"where": f"id={order_id}"},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
    except:
        pass
    
    # Approach 3: Query with id in quotes
    try:
        r = requests.get(
            f"https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={"where": f"id='{order_id}'"},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0]
    except:
        pass
    
    return None

# =============================================================================
# HUBSPOT API - FULL SYNC SYSTEM
# =============================================================================
# Deal stages in HubSpot (default pipeline)
HUBSPOT_STAGE_CLOSED_WON = "closedwon"
HUBSPOT_STAGE_PENDING_PAYMENT = "decisionmakerboughtin"  # Update this to your actual stage ID

# Payment terms that indicate payment is pending
NET_PAYMENT_TERMS = ['net 30', 'net 60', 'net 90', 'net 15', 'net 30 days', 'net 60 days', 'net 45']

def is_paid(order: dict) -> bool:
    """Check if order is fully paid."""
    total = order.get('total', 0) or 0
    if float(total) == 0:
        return True
    
    paid = str(order.get('paid') or '').strip()
    if '100%' in paid:
        return True
    
    payment_terms = str(order.get('paymentTerms') or '').lower()
    has_net_terms = any(net in payment_terms for net in NET_PAYMENT_TERMS)
    
    if has_net_terms:
        return False
    
    return True

def get_payment_debug(order: dict) -> str:
    """Return debug info about payment fields for troubleshooting."""
    paid = order.get('paid')
    terms = order.get('paymentTerms')
    parts = []
    if paid:
        parts.append(f"paid:{paid}")
    if terms:
        parts.append(f"terms:{terms}")
    return ' | '.join(parts) if parts else "(no payment data)"

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
    
    search_body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "dealname",
                "operator": "CONTAINS_TOKEN",
                "value": order_ref
            }]
        }],
        "properties": ["dealname", "amount", "dealstage", "pipeline", "closedate"]
    }
    
    try:
        r = requests.post(search_url, headers=headers, json=search_body, timeout=15)
        if r.status_code == 200:
            results = r.json().get('results', [])
            for deal in results:
                if order_ref in deal.get('properties', {}).get('dealname', ''):
                    return deal
    except:
        pass
    return None

def scan_for_duplicates(api_key: str, order_refs: list, progress_callback=None) -> dict:
    """
    Scan HubSpot for duplicate deals across a list of order references.
    Returns dict: { order_ref: [deal1, deal2, ...] } only for refs with 2+ deals.
    Each deal includes id, dealname, amount, closedate, createdate.
    """
    headers = get_headers(api_key)
    duplicates = {}
    total = len(order_refs)

    for i, order_ref in enumerate(order_refs):
        if progress_callback:
            progress_callback((i + 1) / total)

        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "dealname",
                    "operator": "CONTAINS_TOKEN",
                    "value": order_ref
                }]
            }],
            "properties": ["dealname", "amount", "dealstage", "closedate", "createdate"],
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
            "limit": 20
        }

        try:
            r = requests.post(
                "https://api.hubapi.com/crm/v3/objects/deals/search",
                headers=headers,
                json=search_body,
                timeout=15
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                # Only keep results where the order ref actually appears in deal name
                matched = [d for d in results if order_ref in (d.get("properties", {}).get("dealname", ""))]
                if len(matched) >= 2:
                    duplicates[order_ref] = matched
        except:
            pass

    return duplicates

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
        r = requests.post(search_url, headers=headers, json=search_body, timeout=15)
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
        r = requests.post(search_url, headers=headers, json=search_body, timeout=15)
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
        r = requests.patch(url, headers=headers, json={"properties": properties}, timeout=15)
        return r.status_code == 200
    except:
        return False

def update_contact(api_key: str, contact_id: str, properties: dict) -> bool:
    """Update contact properties. Returns success boolean."""
    headers = get_headers(api_key)
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    
    try:
        r = requests.patch(url, headers=headers, json={"properties": properties}, timeout=15)
        return r.status_code == 200
    except:
        return False

def update_company(api_key: str, company_id: str, properties: dict) -> bool:
    """Update company properties. Returns success boolean."""
    headers = get_headers(api_key)
    url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
    
    try:
        r = requests.patch(url, headers=headers, json={"properties": properties}, timeout=15)
        return r.status_code == 200
    except:
        return False

# -----------------------------------------------------------------------------
# ASSOCIATION FUNCTIONS
# -----------------------------------------------------------------------------
def get_deal_associations(api_key: str, deal_id: str) -> dict:
    """Fetch current company and contact associations for an existing deal."""
    headers = get_headers(api_key)
    result = {"companies": [], "contacts": []}
    
    for obj_type in ["companies", "contacts"]:
        try:
            r = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/{obj_type}",
                headers=headers,
                timeout=15
            )
            if r.status_code == 200:
                result[obj_type] = r.json().get("results", [])
        except:
            pass
    
    return result

def associate_deal(api_key: str, deal_id: str, obj_type: str, obj_id: str) -> bool:
    """Associate a deal with a company or contact.
    obj_type = 'companies' or 'contacts'"""
    headers = get_headers(api_key)
    assoc_type = "deal_to_company" if obj_type == "companies" else "deal_to_contact"
    url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/{obj_type}/{obj_id}/{assoc_type}"
    try:
        r = requests.put(url, headers=headers, timeout=15)
        return r.status_code in [200, 201, 204]
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
        r = requests.post(url, headers=headers, json={"properties": properties}, timeout=15)
        if r.status_code == 201:
            return r.json()['id']
    except:
        pass
    return None

def get_hubspot_owners(api_key: str) -> dict:
    """Fetch all HubSpot owners and return email → owner_id lookup dict."""
    headers = get_headers(api_key)
    owners = {}
    
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/owners",
            headers=headers,
            params={"limit": 100},
            timeout=15
        )
        if r.status_code == 200:
            for owner in r.json().get('results', []):
                email = owner.get('email', '').lower()
                owner_id = owner.get('id')
                if email and owner_id:
                    owners[email] = owner_id
    except:
        pass
    
    return owners

def bulk_update_company_owners(api_key: str, company_to_rep: dict, owner_lookup: dict) -> tuple:
    """
    Bulk update HubSpot company owners based on company name → rep email mapping.
    Returns (updated_count, skipped_count, error_count)
    """
    headers = get_headers(api_key)
    updated = 0
    skipped = 0
    errors = 0
    
    all_companies = []
    after = None
    
    while True:
        params = {"limit": 100, "properties": "name,hubspot_owner_id"}
        if after:
            params["after"] = after
        
        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/companies",
                headers=headers,
                params=params,
                timeout=30
            )
            if r.status_code == 200:
                data = r.json()
                all_companies.extend(data.get('results', []))
                paging = data.get('paging', {})
                if paging.get('next', {}).get('after'):
                    after = paging['next']['after']
                else:
                    break
            else:
                break
        except:
            break
    
    for company in all_companies:
        company_id = company.get('id')
        company_name = (company.get('properties', {}).get('name') or '').strip().upper()
        current_owner = company.get('properties', {}).get('hubspot_owner_id')
        
        rep_email = company_to_rep.get(company_name)
        if not rep_email:
            skipped += 1
            continue
        
        new_owner_id = owner_lookup.get(rep_email)
        if not new_owner_id:
            skipped += 1
            continue
        
        if current_owner == new_owner_id:
            skipped += 1
            continue
        
        try:
            r = requests.patch(
                f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}",
                headers=headers,
                json={"properties": {"hubspot_owner_id": new_owner_id}},
                timeout=15
            )
            if r.status_code == 200:
                updated += 1
            else:
                errors += 1
        except:
            errors += 1
    
    return updated, skipped, errors

def bulk_update_deal_owners(api_key: str, company_to_rep: dict, owner_lookup: dict) -> tuple:
    """
    Bulk update HubSpot deal owners based on associated company name → rep email mapping.
    Returns (updated_count, skipped_count, error_count)
    """
    headers = get_headers(api_key)
    updated = 0
    skipped = 0
    errors = 0
    
    all_deals = []
    after = None
    
    while True:
        params = {
            "limit": 100,
            "properties": "dealname,hubspot_owner_id",
            "associations": "companies"
        }
        if after:
            params["after"] = after
        
        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/deals",
                headers=headers,
                params=params,
                timeout=30
            )
            if r.status_code == 200:
                data = r.json()
                all_deals.extend(data.get('results', []))
                paging = data.get('paging', {})
                if paging.get('next', {}).get('after'):
                    after = paging['next']['after']
                else:
                    break
            else:
                break
        except:
            break
    
    company_id_to_name = {}
    after = None
    while True:
        params = {"limit": 100, "properties": "name"}
        if after:
            params["after"] = after
        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/companies",
                headers=headers,
                params=params,
                timeout=30
            )
            if r.status_code == 200:
                data = r.json()
                for c in data.get('results', []):
                    company_id_to_name[c['id']] = (c.get('properties', {}).get('name') or '').strip().upper()
                paging = data.get('paging', {})
                if paging.get('next', {}).get('after'):
                    after = paging['next']['after']
                else:
                    break
            else:
                break
        except:
            break
    
    for deal in all_deals:
        deal_id = deal.get('id')
        current_owner = deal.get('properties', {}).get('hubspot_owner_id')
        
        associations = deal.get('associations', {}).get('companies', {}).get('results', [])
        if not associations:
            skipped += 1
            continue
        
        company_id = associations[0].get('id')
        company_name = company_id_to_name.get(company_id, '')
        
        rep_email = company_to_rep.get(company_name)
        if not rep_email:
            skipped += 1
            continue
        
        new_owner_id = owner_lookup.get(rep_email)
        if not new_owner_id:
            skipped += 1
            continue
        
        if current_owner == new_owner_id:
            skipped += 1
            continue
        
        try:
            r = requests.patch(
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
                headers=headers,
                json={"properties": {"hubspot_owner_id": new_owner_id}},
                timeout=15
            )
            if r.status_code == 200:
                updated += 1
            else:
                errors += 1
        except:
            errors += 1
    
    return updated, skipped, errors

def bulk_sync_deal_closedates(hs_api_key: str, cin7_username: str, cin7_api_key: str,
                               progress_callback=None) -> tuple:
    """
    Bulk sync HubSpot deal close dates from Cin7 order dates.
    Returns (updated_count, skipped_count, error_count, details_list)
    """
    headers = get_headers(hs_api_key)
    updated = 0
    skipped = 0
    errors = 0
    details = []
    
    all_deals = []
    after = None
    
    while True:
        params = {"limit": 100, "properties": "dealname,closedate"}
        if after:
            params["after"] = after
        
        try:
            r = requests.get(
                "https://api.hubapi.com/crm/v3/objects/deals",
                headers=headers,
                params=params,
                timeout=30
            )
            if r.status_code == 200:
                data = r.json()
                all_deals.extend(data.get('results', []))
                paging = data.get('paging', {})
                if paging.get('next', {}).get('after'):
                    after = paging['next']['after']
                else:
                    break
            else:
                break
        except:
            break
    
    total_deals = len(all_deals)
    
    for i, deal in enumerate(all_deals):
        if progress_callback:
            progress_callback((i + 1) / total_deals)
        
        deal_id = deal.get('id')
        deal_name = deal.get('properties', {}).get('dealname', '')
        current_closedate = deal.get('properties', {}).get('closedate', '')
        
        order_ref = None
        if ' - ' in deal_name:
            potential_ref = deal_name.split(' - ')[-1].strip()
            order_ref = potential_ref
        else:
            order_ref = deal_name.strip()
        
        if not order_ref:
            skipped += 1
            continue
        
        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/SalesOrders",
                auth=(cin7_username, cin7_api_key),
                params={"where": f"reference='{order_ref}'"},
                timeout=15
            )
            
            if r.status_code != 200:
                skipped += 1
                continue
            
            orders = r.json()
            if not orders or len(orders) == 0:
                skipped += 1
                continue
            
            order = orders[0]
            cin7_order_date = order.get('orderDate') or order.get('createdDate') or ''
            
            if not cin7_order_date:
                skipped += 1
                continue
            
            cin7_date_str = cin7_order_date[:10] if cin7_order_date else ''
            current_date_str = current_closedate[:10] if current_closedate else ''
            
            if cin7_date_str == current_date_str:
                skipped += 1
                continue
            
            r = requests.patch(
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
                headers=headers,
                json={"properties": {"closedate": cin7_order_date}},
                timeout=15
            )
            
            if r.status_code == 200:
                updated += 1
                details.append(f"{order_ref}: {current_date_str or 'empty'} → {cin7_date_str}")
            else:
                errors += 1
                
        except Exception as e:
            errors += 1
    
    return updated, skipped, errors, details

def create_company(api_key: str, name: str, phone: str = "", address: str = "",
                   city: str = "", state: str = "", zip_code: str = "", country: str = "",
                   owner_id: str = None) -> str:
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
    if owner_id:
        properties["hubspot_owner_id"] = owner_id
    
    try:
        r = requests.post(url, headers=headers, json={"properties": properties}, timeout=15)
        if r.status_code == 201:
            return r.json()['id']
    except:
        pass
    return None

def create_deal(api_key: str, order: dict, contact_id: str = None, company_id: str = None, owner_id: str = None) -> tuple:
    """Create new deal. Returns (deal_id, stage_label) or (None, error_message)."""
    headers = get_headers(api_key)
    
    stage_id, stage_label = get_deal_stage(order)
    
    order_ref = order.get('reference', '')
    company = order.get('company') or order.get('billingCompany') or ''
    total = order.get('total', 0) or 0
    payment_terms = order.get('paymentTerms') or 'Standard'
    total_owing = order.get('totalOwing', total)
    
    # FIX: Use orderDate (actual sale date), fallback to createdDate
    order_date = order.get('orderDate') or order.get('createdDate') or ''
    order_date_display = order_date[:10] if order_date else 'Unknown'
    
    deal_name = f"{company} - {order_ref}" if company else order_ref
    
    deal_data = {
        "properties": {
            "dealname": deal_name,
            "amount": str(total),
            "dealstage": stage_id,
            "pipeline": "default",
            "closedate": order_date if order_date else None,  # Cin7 Order Date = HubSpot Close Date
            "description": f"Cin7 Order: {order_ref}\nOrder Date: {order_date_display}\nPayment Terms: {payment_terms}\nOwing: ${total_owing}"
        }
    }
    
    deal_data["properties"] = {k: v for k, v in deal_data["properties"].items() if v is not None}
    
    if owner_id:
        deal_data["properties"]["hubspot_owner_id"] = owner_id
    
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals",
                         headers=headers, json=deal_data, timeout=15)
        if r.status_code == 201:
            deal_id = r.json()['id']
            
            if contact_id:
                assoc_url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/contacts/{contact_id}/deal_to_contact"
                requests.put(assoc_url, headers=headers, timeout=15)
            
            if company_id:
                assoc_url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/companies/{company_id}/deal_to_company"
                requests.put(assoc_url, headers=headers, timeout=15)
            
            return deal_id, stage_label
        else:
            return None, f"Error {r.status_code}: {r.text[:100]}"
    except Exception as e:
        return None, str(e)

def get_deal_line_items(api_key: str, deal_id: str) -> list:
    """Get existing line items for a deal."""
    headers = get_headers(api_key)
    
    try:
        url = f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/line_items"
        r = requests.get(url, headers=headers, timeout=15)
        
        if r.status_code == 200:
            results = r.json().get('results', [])
            return results
    except:
        pass
    
    return []

def create_line_items(api_key: str, deal_id: str, order: dict) -> tuple:
    """
    Create line items for a deal from Cin7 order data using batch API.
    Returns (count_created, errors_list)
    """
    headers = get_headers(api_key)
    
    line_items = None
    for field_name in ['lineItems', 'lines', 'salesOrderLines', 'orderLines', 'items', 'lineDetails']:
        if field_name in order and order[field_name]:
            line_items = order[field_name]
            break
    
    if not line_items:
        return 0, [f"No line items found"]
    
    errors = []
    batch_inputs = []
    
    for item in line_items:
        product_name = item.get('name') or item.get('productName') or item.get('description') or 'Product'
        sku = item.get('code') or item.get('sku') or item.get('productCode') or ''
        quantity = item.get('qty') or item.get('quantity') or 1
        unit_price = item.get('unitPrice') or item.get('price') or 0
        total = float(quantity) * float(unit_price)
        
        if sku:
            name = f"{product_name} ({sku})"
        else:
            name = product_name
        
        batch_inputs.append({
            "properties": {
                "name": name[:250],
                "quantity": str(quantity),
                "price": str(unit_price),
                "amount": str(total),
            },
            "associations": [
                {
                    "to": {"id": deal_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 20}]
                }
            ]
        })
    
    created = 0
    for i in range(0, len(batch_inputs), 100):
        batch = batch_inputs[i:i+100]
        try:
            r = requests.post(
                "https://api.hubapi.com/crm/v3/objects/line_items/batch/create",
                headers=headers,
                json={"inputs": batch},
                timeout=60
            )
            
            if r.status_code in [200, 201]:
                results = r.json().get('results', [])
                created += len(results)
            else:
                errors.append(f"Batch create failed: {r.status_code} - {r.text[:200]}")
        except Exception as e:
            errors.append(f"Batch error: {str(e)}")
    
    return created, errors

# -----------------------------------------------------------------------------
# FULL SYNC FUNCTION
# -----------------------------------------------------------------------------
def sync_order_to_hubspot(api_key: str, order: dict, cin7_username: str = None, cin7_api_key: str = None,
                          contact_cache: dict = None, company_cache: dict = None, cache_lock=None,
                          owner_lookup: dict = None) -> dict:
    """
    Full sync of a single order to HubSpot.
    - Creates or updates deal
    - Creates or updates contact
    - Creates or updates company
    - Creates line items
    - Repairs missing company/contact associations on existing deals
    """
    result = {
        "order_ref": order.get('reference', 'Unknown'),
        "success": False,
        "action": "none",
        "deal_stage": "",
        "details": []
    }
    
    order_ref = order.get('reference', '')
    email = order.get('email') or order.get('memberEmail') or ''
    first_name = order.get('firstName') or order.get('billingFirstName') or ''
    last_name = order.get('lastName') or order.get('billingLastName') or ''
    company_name = order.get('company') or order.get('billingCompany') or ''
    phone = order.get('phone') or order.get('billingPhone') or ''
    total = order.get('total', 0) or 0
    
    address = order.get('billingAddress1') or order.get('deliveryAddress1') or ''
    city = order.get('billingCity') or order.get('deliveryCity') or ''
    state = order.get('billingState') or order.get('deliveryState') or ''
    zip_code = order.get('billingPostCode') or order.get('deliveryPostCode') or ''
    country = order.get('billingCountry') or order.get('deliveryCountry') or ''
    
    sales_rep_email = (order.get('salesPersonEmail') or '').lower()
    owner_id = None
    if sales_rep_email and owner_lookup:
        owner_id = owner_lookup.get(sales_rep_email)
        if owner_id:
            result["details"].append(f"Rep: {sales_rep_email.split('@')[0]}")
    
    expected_stage_id, expected_stage_label = get_deal_stage(order)
    result["deal_stage"] = expected_stage_label
    
    # -------------------------------------------------------------------------
    # STEP 1: Search for existing deal
    # -------------------------------------------------------------------------
    existing_deal = search_deal_by_order_ref(api_key, order_ref)
    
    if existing_deal:
        deal_id = existing_deal['id']
        current_stage = existing_deal.get('properties', {}).get('dealstage', '')
        current_amount = float(existing_deal.get('properties', {}).get('amount', 0) or 0)
        current_closedate = existing_deal.get('properties', {}).get('closedate', '')
        
        cin7_order_date = order.get('orderDate') or order.get('createdDate') or ''
        
        needs_update = False
        update_props = {}
        
        if current_stage != expected_stage_id:
            update_props["dealstage"] = expected_stage_id
            needs_update = True
            result["details"].append(f"Stage: {current_stage} → {expected_stage_id}")
        
        if abs(current_amount - total) > 0.01:
            update_props["amount"] = str(total)
            needs_update = True
            result["details"].append(f"Amount: ${current_amount:.2f} → ${total:.2f}")
        
        if cin7_order_date:
            cin7_date_str = cin7_order_date[:10] if cin7_order_date else ''
            current_date_str = current_closedate[:10] if current_closedate else ''
            if cin7_date_str != current_date_str:
                update_props["closedate"] = cin7_order_date
                needs_update = True
                result["details"].append(f"Close Date: {current_date_str or 'empty'} → {cin7_date_str}")
        
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
        
        # -------------------------------------------------------------------------
        # Check for missing line items
        # -------------------------------------------------------------------------
        existing_line_items = get_deal_line_items(api_key, deal_id)
        
        if not existing_line_items:
            line_items_created, line_errors = create_line_items(api_key, deal_id, order)
            if line_items_created > 0:
                result["details"].append(f"{line_items_created} line items added")
                if result["action"] == "skipped":
                    result["action"] = "updated"
            if line_errors:
                result["details"].append(f"Line item errors: {line_errors[:3]}")
        else:
            result["details"].append(f"{len(existing_line_items)} line items already exist")
        
        # -------------------------------------------------------------------------
        # Repair missing company/contact associations
        # -------------------------------------------------------------------------
        existing_assocs = get_deal_associations(api_key, deal_id)
        has_company = len(existing_assocs["companies"]) > 0
        has_contact = len(existing_assocs["contacts"]) > 0

        if not has_company and company_name:
            existing_company = search_company_by_name(api_key, company_name)
            if existing_company:
                company_id = existing_company["id"]
                result["details"].append("Company found")
            else:
                company_id = create_company(
                    api_key, company_name, phone, address,
                    city, state, zip_code, country, owner_id
                )
                if company_id:
                    result["details"].append("Company created")
                else:
                    company_id = None
            if company_id and associate_deal(api_key, deal_id, "companies", company_id):
                result["details"].append("Company association repaired")
                if result["action"] == "skipped":
                    result["action"] = "updated"

        if not has_contact and email:
            existing_contact = search_contact_by_email(api_key, email)
            if existing_contact:
                contact_id = existing_contact["id"]
                result["details"].append("Contact found")
            else:
                contact_id = create_contact(
                    api_key, email, first_name, last_name, company_name, phone
                )
                if contact_id:
                    result["details"].append("Contact created")
                else:
                    contact_id = None
            if contact_id and associate_deal(api_key, deal_id, "contacts", contact_id):
                result["details"].append("Contact association repaired")
                if result["action"] == "skipped":
                    result["action"] = "updated"

        # Mark success if we haven't hit a hard failure
        if result["action"] != "update_failed":
            result["success"] = True

    else:
        # -------------------------------------------------------------------------
        # STEP 2: Deal not found — create
        # -------------------------------------------------------------------------
        contact_id = None
        company_id = None
        
        # -------------------------------------------------------------------------
        # STEP 2a: Sync contact (with caching)
        # -------------------------------------------------------------------------
        if email:
            cached_contact_id = None
            if contact_cache is not None and cache_lock:
                with cache_lock:
                    cached_contact_id = contact_cache.get(email.lower())
            
            if cached_contact_id:
                contact_id = cached_contact_id
                result["details"].append("Contact (cached)")
            else:
                existing_contact = search_contact_by_email(api_key, email)
                
                if existing_contact:
                    contact_id = existing_contact['id']
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
                
                if contact_id and contact_cache is not None and cache_lock:
                    with cache_lock:
                        contact_cache[email.lower()] = contact_id
        
        # -------------------------------------------------------------------------
        # STEP 2b: Sync company (with caching)
        # -------------------------------------------------------------------------
        if company_name:
            cached_company_id = None
            if company_cache is not None and cache_lock:
                with cache_lock:
                    cached_company_id = company_cache.get(company_name.lower())
            
            if cached_company_id:
                company_id = cached_company_id
                result["details"].append("Company (cached)")
            else:
                existing_company = search_company_by_name(api_key, company_name)
                
                if existing_company:
                    company_id = existing_company['id']
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
                    company_id = create_company(api_key, company_name, phone, address, city, state, zip_code, country, owner_id)
                    if company_id:
                        result["details"].append("Company created")
                    else:
                        result["details"].append("Company creation failed")
                
                if company_id and company_cache is not None and cache_lock:
                    with cache_lock:
                        company_cache[company_name.lower()] = company_id
        
        # -------------------------------------------------------------------------
        # STEP 2c: Create deal
        # -------------------------------------------------------------------------
        deal_id, stage_or_error = create_deal(api_key, order, contact_id, company_id, owner_id)
        
        if deal_id:
            result["action"] = "created"
            result["success"] = True
            result["details"].append(f"Deal created as {stage_or_error}")
            
            # -------------------------------------------------------------------------
            # STEP 2d: Create line items
            # -------------------------------------------------------------------------
            line_items_created, line_errors = create_line_items(api_key, deal_id, order)
            if line_items_created > 0:
                result["details"].append(f"{line_items_created} line items added")
            if line_errors:
                result["details"].append(f"Line item errors: {line_errors[:3]}")
        else:
            result["action"] = "create_failed"
            result["details"].append(f"Deal creation failed: {stage_or_error}")
    
    return result

def push_orders_to_hubspot(api_key: str, orders: list, progress_callback=None,
                           cin7_username: str = None, cin7_api_key: str = None) -> dict:
    """
    Full sync of multiple orders to HubSpot using parallel processing.
    Returns detailed results dict.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    results = {
        "created": [],
        "updated": [],
        "skipped": [],
        "failed": [],
        "closed_won": 0,
        "pending_payment": 0
    }
    
    owner_lookup = get_hubspot_owners(api_key)
    
    contact_cache = {}
    company_cache = {}
    cache_lock = threading.Lock()
    
    progress_lock = threading.Lock()
    completed_count = [0]
    
    def process_order(order):
        return sync_order_to_hubspot(api_key, order, cin7_username, cin7_api_key,
                                     contact_cache, company_cache, cache_lock, owner_lookup)
    
    max_workers = 5
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_order = {executor.submit(process_order, order): order for order in orders}
        
        for future in as_completed(future_to_order):
            sync_result = future.result()
            
            with progress_lock:
                completed_count[0] += 1
                if progress_callback:
                    progress_callback(completed_count[0] / len(orders))
            
            if sync_result["success"]:
                if sync_result["action"] == "created":
                    results["created"].append(sync_result)
                elif sync_result["action"] == "updated":
                    results["updated"].append(sync_result)
                else:
                    results["skipped"].append(sync_result)
                
                if sync_result["deal_stage"] == "Closed Won":
                    results["closed_won"] += 1
                else:
                    results["pending_payment"] += 1
            else:
                results["failed"].append(sync_result)
    
    return results

def test_hubspot(api_key: str) -> tuple:
    try:
        r = requests.get(
            "https://api.hubapi.com/crm/v3/objects/contacts",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 1},
            timeout=15
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
            timeout=15
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
        
        if segment == 'Retail':
            o['_skip_reason'] = 'Retail segment'
            to_skip.append(o)
            continue
        
        email_address = (o.get('email') or o.get('memberEmail') or '').lower()
        if 'vivant' in company or 'vivant' in email_address:
            o['_review_reason'] = 'Internal (Vivant in company or email)'
            to_review.append(o)
            continue
        
        if exclude_shopify and 'shopify retail' in source:
            o['_skip_reason'] = 'Shopify Retail excluded'
            to_skip.append(o)
            continue
        
        if status not in IMPORTABLE_STATUSES:
            o['_skip_reason'] = f'Status: {status}'
            to_skip.append(o)
            continue
        
        if not has_email and status == 'dispatched':
            o['_review_reason'] = 'No email (likely employee)'
            to_review.append(o)
            continue
        
        if total == 0:
            if has_email:
                to_import.append(o)
            else:
                o['_review_reason'] = '$0 + No email (likely employee)'
                to_review.append(o)
            continue
        
        to_import.append(o)
    
    return to_import, to_review, to_skip

# =============================================================================
# DISPLAY HELPERS
# =============================================================================
def order_to_summary(order: dict, include_reason: bool = False) -> dict:
    """Convert order to display format with raw numbers for proper sorting."""
    total = order.get('total', 0) or 0
    
    customer = (
        order.get('customerName') or
        order.get('contactName') or
        order.get('billingName') or
        order.get('deliveryName') or
        order.get('memberName') or
        order.get('contact') or
        ''
    )
    
    if not customer:
        first = order.get('firstName') or order.get('billingFirstName') or ''
        last = order.get('lastName') or order.get('billingLastName') or ''
        customer = f"{first} {last}".strip()
    
    # FIX: Use orderDate (actual sale date), fallback to createdDate
    order_date = order.get('orderDate') or order.get('createdDate') or ''

    # Rep column: show username portion or warning if missing
    sales_person_email = order.get('salesPersonEmail') or ''
    rep_display = sales_person_email.split('@')[0] if sales_person_email else '⚠️ No Rep'

    result = {
        'Order #': order.get('reference', ''),
        'Source': order.get('source', ''),
        'Segment': order.get('_segment', ''),
        'Total_Numeric': float(total),  # Hidden column for sorting
        'Total': float(total),          # Display column
        'Company': order.get('company') or order.get('billingCompany') or '',
        'Customer': customer,
        'Email': order.get('email') or order.get('memberEmail') or '',
        'Order Date': order_date[:10] if order_date else '',
        'Payment': '✅ Paid' if is_paid(order) else '⏳ Unpaid',
        'Deal Stage': get_deal_stage(order)[1],
        'Rep': rep_display,
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
    
    df['Total_Numeric'] = pd.to_numeric(df['Total_Numeric'], errors='coerce').fillna(0)
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    
    df = df.sort_values('Total_Numeric', ascending=False)
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
    branding = get_branding()
    
    if branding.get("logo_url"):
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            try:
                st.image(branding["logo_url"], width=120)
            except:
                pass
    
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
                
                current_branding = get_branding()
                
                new_company = st.text_input("Company Name", value=current_branding.get('company_name', ''))
                new_logo = st.text_input("Logo URL", value=current_branding.get('logo_url', ''), help="URL to your logo image")
                new_color = st.color_picker("Primary Color", value=current_branding.get('primary_color', '#1a5276'))
                new_email = st.text_input("Support Email", value=current_branding.get('support_email', ''))
                new_powered = st.checkbox("Show 'Powered by OrderFloz'", value=current_branding.get('powered_by', True))
                
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
        # BULK OWNER SYNC TOOL
        # -------------------------------------------------------------------------
        with st.expander("👥 Bulk Owner Sync (One-Time Setup)"):
            st.caption("Update existing HubSpot Companies and Deals with correct owners based on your mapping spreadsheet")
            
            if not hs_key:
                st.warning("Enter HubSpot API key first")
            else:
                uploaded_file = st.file_uploader(
                    "Upload Company → Owner mapping spreadsheet (Excel)",
                    type=['xlsx', 'xls'],
                    key="owner_mapping_file"
                )
                
                if uploaded_file:
                    try:
                        df = pd.read_excel(uploaded_file)
                        
                        required_cols = ['Company Name', 'Rep Email']
                        if not all(col in df.columns for col in required_cols):
                            st.error(f"Spreadsheet must have columns: {required_cols}")
                        else:
                            company_to_rep = {}
                            for _, row in df.iterrows():
                                company = str(row.get('Company Name', '')).strip().upper()
                                rep_email = str(row.get('Rep Email', '')).strip().lower()
                                if company and rep_email and rep_email != 'nan':
                                    company_to_rep[company] = rep_email
                            
                            st.success(f"✅ Loaded {len(company_to_rep)} company → rep mappings")
                            
                            with st.expander("Preview mappings (first 10)"):
                                preview_items = list(company_to_rep.items())[:10]
                                for company, email in preview_items:
                                    st.caption(f"• {company} → {email}")
                            
                            owner_lookup = get_hubspot_owners(hs_key)
                            if owner_lookup:
                                st.info(f"Found {len(owner_lookup)} HubSpot owners")
                            else:
                                st.warning("Could not fetch HubSpot owners")
                            
                            st.divider()
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                if st.button("🔄 Sync Company Owners", type="primary"):
                                    with st.spinner("Updating company owners..."):
                                        updated, skipped, errors = bulk_update_company_owners(
                                            hs_key, company_to_rep, owner_lookup
                                        )
                                    
                                    st.success(f"✅ Updated: {updated} companies")
                                    if skipped:
                                        st.info(f"⏭️ Skipped: {skipped} (no match or already set)")
                                    if errors:
                                        st.warning(f"⚠️ Errors: {errors}")
                            
                            with col2:
                                if st.button("🔄 Sync Deal Owners", type="primary"):
                                    with st.spinner("Updating deal owners..."):
                                        updated, skipped, errors = bulk_update_deal_owners(
                                            hs_key, company_to_rep, owner_lookup
                                        )
                                    
                                    st.success(f"✅ Updated: {updated} deals")
                                    if skipped:
                                        st.info(f"⏭️ Skipped: {skipped} (no match or already set)")
                                    if errors:
                                        st.warning(f"⚠️ Errors: {errors}")
                            
                    except Exception as e:
                        st.error(f"Error reading file: {str(e)}")
        
        # -------------------------------------------------------------------------
        # BULK CLOSE DATE SYNC TOOL
        # -------------------------------------------------------------------------
        with st.expander("📅 Bulk Close Date Sync (Fix Historical Deals)"):
            st.caption("Update close dates on ALL existing HubSpot deals using Cin7 order dates")
            
            if not hs_key:
                st.warning("Enter HubSpot API key first")
            elif not cin7_user or not cin7_key:
                st.warning("Enter Cin7 credentials first")
            else:
                st.info("""
                **What this does:**
                1. Fetches all deals from HubSpot
                2. Extracts order reference from deal name
                3. Looks up each order in Cin7 to get the real `orderDate`
                4. Updates HubSpot `closedate` if it differs
                
                ⚠️ This may take several minutes for large deal counts.
                """)
                
                if st.button("🔄 Sync All Deal Close Dates", type="primary", key="bulk_closedate_sync"):
                    progress_bar = st.progress(0)
                    status = st.empty()
                    
                    def update_progress(pct):
                        progress_bar.progress(pct)
                        status.info(f"Processing deals... {int(pct * 100)}%")
                    
                    with st.spinner("Syncing close dates from Cin7..."):
                        updated, skipped, errors, details = bulk_sync_deal_closedates(
                            hs_key, cin7_user, cin7_key, update_progress
                        )
                    
                    progress_bar.empty()
                    status.empty()
                    
                    st.success(f"✅ Updated: {updated} deals")
                    if skipped:
                        st.info(f"⏭️ Skipped: {skipped} (no match, same date, or not found in Cin7)")
                    if errors:
                        st.warning(f"⚠️ Errors: {errors}")
                    
                    if details:
                        with st.expander(f"📋 Updated Deals ({len(details)})"):
                            for detail in details[:50]:
                                st.caption(f"• {detail}")
                            if len(details) > 50:
                                st.caption(f"...and {len(details) - 50} more")
        
        # -------------------------------------------------------------------------
        # HUBSPOT CLEANUP TOOL
        # -------------------------------------------------------------------------
        with st.expander("🧹 HubSpot Cleanup (Delete Duplicates)"):
            st.caption("Find and delete duplicate or junk deals for a specific company/contact")
            
            if not hs_key:
                st.warning("Enter HubSpot API key first")
            else:
                cleanup_search = st.text_input(
                    "Search company or contact name",
                    placeholder="e.g., MONARCH SKIN",
                    key="cleanup_search"
                )
                
                if st.button("🔍 Find Deals", key="cleanup_find") and cleanup_search:
                    headers = get_headers(hs_key)
                    
                    search_body = {
                        "filterGroups": [{
                            "filters": [{
                                "propertyName": "dealname",
                                "operator": "CONTAINS_TOKEN",
                                "value": cleanup_search
                            }]
                        }],
                        "properties": ["dealname", "amount", "dealstage", "closedate", "createdate", "hubspot_owner_id"],
                        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
                        "limit": 100
                    }
                    
                    try:
                        r = requests.post(
                            "https://api.hubapi.com/crm/v3/objects/deals/search",
                            headers=headers,
                            json=search_body,
                            timeout=30
                        )
                        
                        if r.status_code == 200:
                            deals = r.json().get('results', [])
                            st.session_state.cleanup_deals = deals
                            st.success(f"Found {len(deals)} deals matching '{cleanup_search}'")
                        else:
                            st.error(f"Search failed: {r.status_code}")
                            st.session_state.cleanup_deals = []
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        st.session_state.cleanup_deals = []
                
                if 'cleanup_deals' in st.session_state and st.session_state.cleanup_deals:
                    deals = st.session_state.cleanup_deals
                    
                    deal_data = []
                    for d in deals:
                        props = d.get('properties', {})
                        deal_data.append({
                            'Deal ID': d.get('id'),
                            'Deal Name': props.get('dealname', ''),
                            'Amount': f"${float(props.get('amount') or 0):,.2f}",
                            'Close Date': (props.get('closedate') or '')[:10],
                            'Created': (props.get('createdate') or '')[:10],
                        })
                    
                    df = pd.DataFrame(deal_data)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    
                    order_refs = {}
                    for d in deals:
                        name = d.get('properties', {}).get('dealname', '')
                        if ' - ' in name:
                            ref = name.split(' - ')[-1].strip()
                        else:
                            ref = name.strip()
                        
                        if ref not in order_refs:
                            order_refs[ref] = []
                        order_refs[ref].append(d)
                    
                    duplicates = {ref: deals_list for ref, deals_list in order_refs.items() if len(deals_list) > 1}
                    
                    if duplicates:
                        st.warning(f"⚠️ Found {len(duplicates)} order(s) with duplicate deals:")
                        for ref, dup_deals in duplicates.items():
                            st.caption(f"• **{ref}**: {len(dup_deals)} deals")
                    
                    st.divider()
                    
                    delete_options = [f"{d.get('id')} - {d.get('properties', {}).get('dealname', '')} (Created: {(d.get('properties', {}).get('createdate') or '')[:10]})" for d in deals]
                    
                    selected_to_delete = st.multiselect(
                        "Select deals to DELETE",
                        options=delete_options,
                        key="deals_to_delete"
                    )
                    
                    if selected_to_delete:
                        st.error(f"⚠️ You are about to DELETE {len(selected_to_delete)} deal(s). This cannot be undone!")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            confirm = st.checkbox("I understand this is permanent", key="confirm_delete")
                        
                        with col2:
                            if confirm and st.button("🗑️ DELETE Selected Deals", type="primary", key="delete_deals"):
                                headers = get_headers(hs_key)
                                deleted = 0
                                errors = 0
                                
                                progress = st.progress(0)
                                for i, selection in enumerate(selected_to_delete):
                                    deal_id = selection.split(' - ')[0]
                                    try:
                                        r = requests.delete(
                                            f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
                                            headers=headers,
                                            timeout=15
                                        )
                                        if r.status_code == 204:
                                            deleted += 1
                                        else:
                                            errors += 1
                                    except:
                                        errors += 1
                                    
                                    progress.progress((i + 1) / len(selected_to_delete))
                                
                                progress.empty()
                                st.success(f"✅ Deleted {deleted} deal(s)")
                                if errors:
                                    st.warning(f"⚠️ {errors} deletion(s) failed")
                                
                                st.session_state.cleanup_deals = []
                                st.rerun()
    
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
    
    date_range_days = (until_date - since_date).days + 1
    
    if st.button("🔄 Fetch Orders (Read Only)", type="primary", use_container_width=True):
        if not cin7_user or not cin7_key:
            st.error("Enter Cin7 credentials in sidebar")
        else:
            status_container = st.empty()
            progress_bar = st.progress(0)
            
            fetch_state = {"last_message": "", "orders": 0, "page": 1}
            
            def update_progress(phase, page, orders_so_far, message):
                fetch_state["last_message"] = message
                fetch_state["orders"] = orders_so_far
                fetch_state["page"] = page
                
                if phase == "fetching":
                    progress = min(0.1 + (page * 0.15), 0.85)
                elif phase == "processing":
                    progress = 0.90
                else:
                    progress = 1.0
                
                progress_bar.progress(progress)
                
                if phase == "fetching":
                    if orders_so_far > 0:
                        status_container.info(f"🔄 **Fetching orders...** Found {orders_so_far:,} orders so far (page {page})")
                    else:
                        status_container.info(f"🔌 **Connecting to Cin7...** Requesting {date_range_days} days of data")
                elif phase == "processing":
                    status_container.info(f"⚙️ **Processing...** Classifying {orders_so_far:,} orders")
                else:
                    status_container.success(f"✅ **Complete!** Loaded {orders_so_far:,} orders")
            
            try:
                orders = fetch_orders(cin7_user, cin7_key, since, until, update_progress)
                
                progress_bar.empty()
                status_container.empty()
                
                st.session_state.fetched_orders = orders
                st.session_state.fetch_since = since_date
                st.session_state.fetch_until = until_date
                st.session_state.selected_import = set()
                st.session_state.selected_review = set()
                
                if orders:
                    wholesale_count = sum(1 for o in orders if classify_order(o) == 'Wholesale')
                    retail_count = len(orders) - wholesale_count
                    st.success(f"✅ Fetched **{len(orders):,}** orders ({wholesale_count:,} wholesale, {retail_count:,} retail)")
                else:
                    st.warning("No orders found in the selected date range")
                    
            except Exception as e:
                progress_bar.empty()
                status_container.empty()
                st.error(f"❌ Error fetching orders: {str(e)}")
    
    # -------------------------------------------------------------------------
    # RESULTS (from session state)
    # -------------------------------------------------------------------------
    orders = st.session_state.fetched_orders
    if orders is None:
        st.caption("👆 Select a date range and click Fetch Orders")
        return
    
    st.caption(f"🟢 {len(orders)} orders loaded (dispatched between {st.session_state.fetch_since} and {st.session_state.fetch_until})")
    
    to_import, to_review, to_skip = filter_orders(orders, exclude_shopify)
    
    retail_orders = [o for o in to_skip if o.get('_segment') == 'Retail']
    other_skipped = [o for o in to_skip if o.get('_segment') != 'Retail']
    
    import_revenue = sum(o.get('total', 0) or 0 for o in to_import)
    review_revenue = sum(o.get('total', 0) or 0 for o in to_review)
    retail_revenue = sum(o.get('total', 0) or 0 for o in retail_orders)
    
    import_refs = {o.get('reference') for o in to_import}
    review_refs = {o.get('reference') for o in to_review}
    
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
    # SEARCH FILTER
    # -------------------------------------------------------------------------
    st.header("🔍 Search & Filter")
    
    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_term = st.text_input(
            "Search orders",
            placeholder="Search by Order #, Company, Customer Name, or Email...",
            key="order_search"
        )
    with search_col2:
        search_field = st.selectbox(
            "Search in",
            ["All Fields", "Order #", "Company", "Customer", "Email"],
            key="search_field"
        )
    
    def matches_search(order: dict, term: str, field: str) -> bool:
        """Check if order matches search criteria."""
        if not term:
            return True
        
        term_lower = term.lower().strip()
        
        order_ref = (order.get('reference') or '').lower()
        company = (order.get('company') or order.get('billingCompany') or '').lower()
        email = (order.get('email') or order.get('memberEmail') or '').lower()
        
        customer = (
            order.get('customerName') or
            order.get('contactName') or
            order.get('billingName') or
            order.get('deliveryName') or
            order.get('memberName') or
            order.get('contact') or
            ''
        ).lower()
        if not customer:
            first = order.get('firstName') or order.get('billingFirstName') or ''
            last = order.get('lastName') or order.get('billingLastName') or ''
            customer = f"{first} {last}".strip().lower()
        
        if field == "Order #":
            return term_lower in order_ref
        elif field == "Company":
            return term_lower in company
        elif field == "Customer":
            return term_lower in customer
        elif field == "Email":
            return term_lower in email
        else:
            return (term_lower in order_ref or
                    term_lower in company or
                    term_lower in customer or
                    term_lower in email)
    
    if search_term:
        to_import_filtered = [o for o in to_import if matches_search(o, search_term, search_field)]
        to_review_filtered = [o for o in to_review if matches_search(o, search_term, search_field)]
        
        total_matches = len(to_import_filtered) + len(to_review_filtered)
        if total_matches > 0:
            st.success(f"🔎 Found **{total_matches}** matching orders ({len(to_import_filtered)} ready, {len(to_review_filtered)} review)")
        else:
            st.warning(f"No orders found matching '{search_term}'")
        
        to_import = to_import_filtered
        to_review = to_review_filtered
        
        import_refs = {o.get('reference') for o in to_import}
        review_refs = {o.get('reference') for o in to_review}
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # LINE ITEMS PREVIEW (Debug tool)
    # -------------------------------------------------------------------------
    with st.expander("🔍 Preview Line Items (Debug Tool)"):
        st.caption("Test fetching line items from Cin7 before syncing to HubSpot")
        
        all_orders = to_import + to_review
        if all_orders:
            order_options = {f"{o.get('reference')} - {o.get('company', 'Unknown')} (${o.get('total', 0):,.2f})": o for o in all_orders}
            selected_order_name = st.selectbox("Select an order to preview:", list(order_options.keys()))
            
            if st.button("🔎 Fetch Line Items from Cin7", key="preview_line_items"):
                selected_order = order_options[selected_order_name]
                order_id = selected_order.get('id')
                order_ref = selected_order.get('reference')
                sales_rep = selected_order.get('salesPersonEmail', '⚠️ Not assigned')
                
                st.write(f"**Order Reference:** {order_ref}")
                st.write(f"**Order ID:** {order_id}")
                st.write(f"**Sales Rep (→ Deal Owner):** {sales_rep}")
                
                st.subheader("Fields from List API (already loaded):")
                st.code(list(selected_order.keys()))
                
                for field_name in ['lineItems', 'lines', 'salesOrderLines', 'orderLines', 'items', 'lineDetails']:
                    if field_name in selected_order and selected_order[field_name]:
                        st.success(f"✅ Line items already present in list API under '{field_name}'!")
                        st.json(selected_order[field_name][:3])
                        break
                else:
                    st.info("No line items in list API response - need to fetch details")
                
                if order_id:
                    st.divider()
                    st.subheader("Fetching Detailed Order...")
                    
                    approaches = [
                        ("Direct endpoint /SalesOrders/{id}", f"https://api.cin7.com/api/v1/SalesOrders/{order_id}", {}),
                        ("Query: id={id}", "https://api.cin7.com/api/v1/SalesOrders", {"where": f"id={order_id}"}),
                        ("Query: id='{id}'", "https://api.cin7.com/api/v1/SalesOrders", {"where": f"id='{order_id}'"}),
                        ("Query: reference='{ref}'", "https://api.cin7.com/api/v1/SalesOrders", {"where": f"reference='{order_ref}'"}),
                    ]
                    
                    for approach_name, url, params in approaches:
                        st.write(f"**Trying:** {approach_name}")
                        try:
                            r = requests.get(url, auth=(cin7_user, cin7_key), params=params if params else None, timeout=15)
                            st.write(f"  Status: {r.status_code}")
                            
                            if r.status_code == 200:
                                data = r.json()
                                
                                if isinstance(data, list):
                                    st.write(f"  Response: List with {len(data)} items")
                                    if len(data) > 0:
                                        order_data = data[0]
                                        st.success(f"  ✅ Got order data!")
                                        st.write(f"  Keys: {list(order_data.keys())}")
                                        
                                        for field_name in ['lineItems', 'lines', 'salesOrderLines', 'orderLines', 'items', 'lineDetails']:
                                            if field_name in order_data and order_data[field_name]:
                                                st.success(f"  ✅ Found line items under '{field_name}': {len(order_data[field_name])} items")
                                                st.json(order_data[field_name][:2])
                                                break
                                        else:
                                            st.warning("  ⚠️ No line items found in response")
                                            for k, v in order_data.items():
                                                if isinstance(v, list) and len(v) > 0:
                                                    st.write(f"  List field '{k}': {len(v)} items")
                                        break
                                elif isinstance(data, dict) and data:
                                    st.write(f"  Response: Dict")
                                    st.write(f"  Keys: {list(data.keys())}")
                                else:
                                    st.write(f"  Response: Empty")
                            else:
                                st.write(f"  Error: {r.text[:200]}")
                        except Exception as e:
                            st.write(f"  Exception: {str(e)}")
                else:
                    st.error("❌ No order ID found in order data")
        else:
            st.info("No orders loaded yet")
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # SECTION 1: READY TO IMPORT
    # -------------------------------------------------------------------------
    st.header(f"✅ Ready to Import ({len(to_import)} orders)")
    st.caption("These orders passed all filters and are pre-selected for import.")
    
    if to_import:
        df_import = prepare_dataframe(to_import)
        df_import.insert(0, 'Select', df_import['Order #'].apply(lambda x: x in st.session_state.selected_import))
        
        edited = st.data_editor(
            df_import,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Select': st.column_config.CheckboxColumn('Select', default=True),
                'Total': st.column_config.NumberColumn('Total', format='$ %.2f'),
            },
            disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer', 'Email', 'Order Date', 'Payment', 'Deal Stage', 'Rep', 'Status'],
            key="import_editor"
        )
        
        st.session_state.selected_import = set(edited[edited['Select']]['Order #'].tolist())
        
        n = len(st.session_state.selected_import & import_refs)
        t = sum((o.get('total', 0) or 0) for o in to_import if o.get('reference') in st.session_state.selected_import)
        st.caption(f"✓ {n} of {len(to_import)} selected (${t:,.2f})")
    else:
        st.info("No orders ready to import")
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # SECTION 2: NEEDS REVIEW
    # -------------------------------------------------------------------------
    with st.expander(f"⚠️ Needs Review ({len(to_review)} orders) — Click to expand"):
        st.caption("These orders need manual review before import.")
        
        if to_review:
            df_review = prepare_dataframe(to_review, include_reason=True)
            df_review.insert(0, 'Select', df_review['Order #'].apply(lambda x: x in st.session_state.selected_review))
            
            edited_review = st.data_editor(
                df_review,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'Select': st.column_config.CheckboxColumn('Select', default=False),
                    'Total': st.column_config.NumberColumn('Total', format='$ %.2f'),
                    'Reason': st.column_config.TextColumn('Reason', width='medium')
                },
                disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer', 'Email', 'Order Date', 'Payment', 'Deal Stage', 'Rep', 'Status', 'Reason'],
                key="review_editor"
            )
            
            st.session_state.selected_review = set(edited_review[edited_review['Select']]['Order #'].tolist())
            
            n = len(st.session_state.selected_review & review_refs)
            t = sum((o.get('total', 0) or 0) for o in to_review if o.get('reference') in st.session_state.selected_review)
            st.caption(f"✓ {n} of {len(to_review)} selected (${t:,.2f})")
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
        selected_orders = [o for o in to_import + to_review if o.get('reference') in all_selected]
        
        closed_won_count = sum(1 for o in selected_orders if is_paid(o))
        pending_count = total_selected - closed_won_count
        
        st.success(f"**{total_selected} orders selected** — Total: ${total_selected_revenue:,.2f}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Closed Won", closed_won_count, help="Fully paid orders")
        with col2:
            st.metric("Pending Payment", pending_count, help="Unpaid orders (Net terms)")
        
        if pending_count > 0:
            st.info(f"ℹ️ {pending_count} orders have outstanding balances and will sync as **Pending Payment** deals.")
        
        st.caption("**Full Sync:** Creates new deals, updates existing deals, syncs contacts & companies, repairs missing associations.")

        # -------------------------------------------------------------------------
        # STEP 1: PRE-SYNC DUPLICATE SCAN
        # -------------------------------------------------------------------------
        st.subheader("Step 1: Scan for Duplicates")
        st.caption("Check HubSpot for existing duplicate deals before writing anything.")

        # Invalidate prior scan if selected orders have changed
        if st.session_state.dupe_scan_order_set != all_selected:
            st.session_state.dupe_scan_results = None
            st.session_state.dupes_to_delete = {}

        if not hs_key:
            st.warning("Enter HubSpot API key in sidebar before scanning.")
        else:
            if st.button("🔍 Scan for Duplicate Deals", key="run_dupe_scan"):
                scan_progress = st.progress(0)
                scan_status = st.empty()
                scan_status.info("Scanning HubSpot for duplicate deals...")

                def scan_progress_cb(pct):
                    scan_progress.progress(pct)
                    scan_status.info(f"Scanning... {int(pct * 100)}%")

                dupe_results = scan_for_duplicates(
                    hs_key,
                    list(all_selected),
                    scan_progress_cb
                )

                scan_progress.empty()
                scan_status.empty()

                st.session_state.dupe_scan_results = dupe_results
                st.session_state.dupe_scan_order_set = all_selected.copy()

                # Default: mark all older dupes for deletion (keep newest per ref)
                auto_delete = {}
                for ref, deals in dupe_results.items():
                    # Deals are sorted ASC by createdate — delete all but the last
                    auto_delete[ref] = [d["id"] for d in deals[:-1]]
                st.session_state.dupes_to_delete = auto_delete

            # ---- Show scan results ----
            if st.session_state.dupe_scan_results is not None:
                dupe_results = st.session_state.dupe_scan_results

                if not dupe_results:
                    st.success("✅ No duplicates found. Safe to sync.")
                else:
                    st.warning(f"⚠️ Found **{len(dupe_results)}** order ref(s) with duplicate deals in HubSpot.")
                    st.caption("Review each group below. The newest deal (bottom row) will be **kept**. Older deals are pre-selected for deletion. Uncheck any you want to keep.")

                    updated_delete_map = {}

                    for ref, deals in dupe_results.items():
                        st.markdown(f"**Order Ref: `{ref}`** — {len(deals)} deals found")

                        # Build display rows
                        rows = []
                        for d in deals:
                            props = d.get("properties", {})
                            rows.append({
                                "Deal ID": d["id"],
                                "Deal Name": props.get("dealname", ""),
                                "Amount": f"${float(props.get('amount') or 0):,.2f}",
                                "Close Date": (props.get("closedate") or "")[:10],
                                "Created": (props.get("createdate") or "")[:10],
                            })

                        df_dupes = pd.DataFrame(rows)

                        # Mark newest row
                        newest_id = deals[-1]["id"]
                        df_dupes["Action"] = df_dupes["Deal ID"].apply(
                            lambda x: "✅ KEEP (newest)" if x == newest_id else "🗑️ Delete"
                        )

                        st.dataframe(df_dupes, use_container_width=True, hide_index=True)

                        # Checkbox per older deal
                        delete_ids_for_ref = []
                        for d in deals[:-1]:  # All except newest
                            props = d.get("properties", {})
                            created = (props.get("createdate") or "")[:10]
                            amount = f"${float(props.get('amount') or 0):,.2f}"
                            label = f"Delete: {props.get('dealname', d['id'])} (Created {created}, {amount})"
                            checked = st.checkbox(label, value=True, key=f"del_{d['id']}")
                            if checked:
                                delete_ids_for_ref.append(d["id"])

                        updated_delete_map[ref] = delete_ids_for_ref
                        st.divider()

                    st.session_state.dupes_to_delete = updated_delete_map

                    total_to_delete = sum(len(v) for v in updated_delete_map.values())
                    if total_to_delete > 0:
                        st.error(f"⚠️ **{total_to_delete} duplicate deal(s) will be deleted** before sync runs.")
                    else:
                        st.info("No deals marked for deletion. Sync will run normally.")

        # -------------------------------------------------------------------------
        # STEP 2: CONFIRM & SYNC
        # -------------------------------------------------------------------------
        st.subheader("Step 2: Confirm & Sync")

        # Block sync if scan hasn't been run yet
        scan_done = st.session_state.dupe_scan_results is not None
        if not scan_done:
            st.info("👆 Run the duplicate scan above before syncing.")

        confirm = st.checkbox(
            f"I confirm I want to sync {total_selected} orders to HubSpot",
            key="confirm_push",
            disabled=not scan_done
        )

        if st.button(
            f"🔄 SYNC {total_selected} ORDERS TO HUBSPOT (${total_selected_revenue:,.0f})",
            type="primary",
            use_container_width=True,
            disabled=not confirm or not hs_key or not scan_done
        ):
            if not hs_key:
                st.error("Enter HubSpot API key in sidebar")
            else:
                headers_hs = get_headers(hs_key)

                # ---- Delete confirmed duplicate deals first ----
                dupes_to_delete = st.session_state.dupes_to_delete
                total_to_delete = sum(len(v) for v in dupes_to_delete.values())

                if total_to_delete > 0:
                    delete_progress = st.progress(0)
                    delete_status = st.empty()
                    deleted_count = 0
                    delete_errors = 0

                    all_delete_ids = [did for ids in dupes_to_delete.values() for did in ids]

                    for i, deal_id in enumerate(all_delete_ids):
                        delete_status.info(f"🗑️ Deleting duplicate {i + 1}/{total_to_delete}...")
                        try:
                            r = requests.delete(
                                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
                                headers=headers_hs,
                                timeout=15
                            )
                            if r.status_code == 204:
                                deleted_count += 1
                            else:
                                delete_errors += 1
                        except:
                            delete_errors += 1
                        delete_progress.progress((i + 1) / total_to_delete)

                    delete_progress.empty()
                    delete_status.empty()

                    if delete_errors == 0:
                        st.success(f"✅ Deleted {deleted_count} duplicate deal(s). Proceeding with sync...")
                    else:
                        st.warning(f"⚠️ Deleted {deleted_count}, {delete_errors} deletion(s) failed. Proceeding with sync...")

                # ---- Run the main sync ----
                status_container = st.empty()
                progress_bar = st.progress(0)

                sync_state = {"current": 0, "total": total_selected}

                def update_progress(pct):
                    sync_state["current"] = int(pct * total_selected)
                    progress_bar.progress(pct)
                    status_container.info(
                        f"🔄 **Syncing to HubSpot...** {sync_state['current']}/{total_selected} orders "
                        f"({int(pct*100)}%)"
                    )

                try:
                    results = push_orders_to_hubspot(
                        hs_key,
                        selected_orders,
                        update_progress,
                        cin7_username=cin7_user,
                        cin7_api_key=cin7_key
                    )

                    progress_bar.empty()
                    status_container.empty()

                    created_count = len(results["created"])
                    updated_count = len(results["updated"])
                    skipped_count = len(results["skipped"])
                    failed_count = len(results["failed"])

                    if failed_count == 0:
                        st.success(f"✅ **Sync complete!** {created_count + updated_count + skipped_count} orders processed successfully.")
                    else:
                        st.warning(f"⚠️ **Sync completed with errors.** {failed_count} orders failed.")

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

                    if results["updated"]:
                        with st.expander(f"🔄 Updated Deals ({updated_count})"):
                            for item in results["updated"]:
                                st.caption(f"• **{item['order_ref']}**: {', '.join(item['details'])}")

                    if results["skipped"]:
                        with st.expander(f"⏭️ Skipped Deals ({skipped_count}) - already synced"):
                            for item in results["skipped"][:20]:
                                st.caption(f"• **{item['order_ref']}**: {', '.join(item['details'])}")
                            if skipped_count > 20:
                                st.caption(f"...and {skipped_count - 20} more")

                    if results["failed"]:
                        with st.expander(f"❌ Failed ({failed_count})", expanded=True):
                            for item in results["failed"][:10]:
                                st.caption(f"• **{item['order_ref']}**: {', '.join(item['details'])}")
                            if failed_count > 10:
                                st.caption(f"...and {failed_count - 10} more")

                    # Reset scan state after successful sync
                    st.session_state.dupe_scan_results = None
                    st.session_state.dupe_scan_order_set = set()
                    st.session_state.dupes_to_delete = {}

                    if failed_count == 0:
                        st.balloons()

                except Exception as e:
                    progress_bar.empty()
                    status_container.empty()
                    st.error(f"❌ Error syncing to HubSpot: {str(e)}")
    else:
        st.warning("No orders selected. Check orders above to include them in the sync.")
    
    st.divider()
    
    # -------------------------------------------------------------------------
    # SECTION 3: RETAIL (view only)
    # -------------------------------------------------------------------------
    with st.expander(f"🛍️ Retail Orders ({len(retail_orders)}) — View Only"):
        st.caption("Retail orders are shown for reference only and cannot be imported")
        if retail_orders:
            df_retail = prepare_dataframe(retail_orders)
            st.dataframe(df_retail, use_container_width=True, hide_index=True, column_config=get_column_config())
        else:
            st.info("No retail orders")
    
    # -------------------------------------------------------------------------
    # SECTION 4: SKIPPED (view only)
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
        if orders:
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
        else:
            st.info("No orders loaded")
    
    with st.expander("📋 Status Breakdown"):
        if orders:
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
        else:
            st.info("No orders loaded")
    
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
    
    # Footer
    branding = get_branding()
    footer_text = f"**{branding['company_name']}** — Cin7 to HubSpot order sync"
    if branding.get("powered_by", True):
        footer_text += " | Powered by OrderFloz"
    if branding.get("support_email"):
        footer_text += f" | {branding['support_email']}"
    st.caption(footer_text)

if __name__ == "__main__":
    main()
