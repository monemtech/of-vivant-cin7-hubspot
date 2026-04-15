"""
OrderFloz - Cin7 to HubSpot Sync
================================
- Fetches wholesale orders from Cin7
- Shows what would be synced to HubSpot
- Push orders to HubSpot as Closed Won deals

UPDATED: Now uses orderDate (actual sale date) instead of createdDate
UPDATED: Rep column in order table for owner troubleshooting
UPDATED: Repairs missing company/contact associations on existing deals
UPDATED: Pre-sync duplicate scan with user confirmation before any writes
UPDATED: Staleness check — tracks and displays time since last successful sync
UPDATED: Live whitelist filter — only imports orders for accounts with group CM/TP/VL + wholesale stage
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
# BRANDING CONFIGURATION
# =============================================================================
BRANDING_FILE = Path(".orderfloz_branding.json")

def get_default_branding():
    return {
        "company_name": "OrderFloz",
        "logo_url": "",
        "primary_color": "#1a5276",
        "accent_color": "#2ecc71",
        "support_email": "support@orderfloz.com",
        "powered_by": True
    }

def load_branding():
    defaults = get_default_branding()
    if BRANDING_FILE.exists():
        try:
            branding = json.loads(BRANDING_FILE.read_text())
            for key, value in defaults.items():
                if key not in branding:
                    branding[key] = value
            return branding
        except:
            pass
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
    BRANDING_FILE.write_text(json.dumps(branding, indent=2))

def get_branding():
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
if 'branding' not in st.session_state:
    st.session_state.branding = load_branding()
if 'dupe_scan_results' not in st.session_state:
    st.session_state.dupe_scan_results = None
if 'dupe_scan_order_set' not in st.session_state:
    st.session_state.dupe_scan_order_set = set()
if 'dupes_to_delete' not in st.session_state:
    st.session_state.dupes_to_delete = {}
if 'dupe_scan_skipped' not in st.session_state:
    st.session_state.dupe_scan_skipped = False
if 'whitelist' not in st.session_state:
    st.session_state.whitelist = None   # None = not yet fetched; set() = fetched but empty

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

def save_last_sync(config: dict):
    """Persist last_sync_at timestamp into the existing config without overwriting credentials."""
    existing = load_config()
    existing.update(config)
    CONFIG_FILE.write_text(json.dumps(existing))

def get_staleness_banner() -> tuple:
    """
    Returns (label, color) based on time since last successful sync.
    color: 'green' | 'orange' | 'red'
    """
    config = load_config()
    last_sync = config.get('last_sync_at')
    if not last_sync:
        return "⚠️ Never synced to HubSpot", "red"
    try:
        last_dt = datetime.fromisoformat(last_sync)
        delta = datetime.now() - last_dt
        days = delta.days
        if days == 0:
            return f"✅ Last synced today at {last_dt.strftime('%I:%M %p')}", "green"
        elif days <= 3:
            return f"🟡 Last synced {days} day{'s' if days > 1 else ''} ago ({last_dt.strftime('%b %d')})", "orange"
        else:
            return f"🔴 Last synced {days} days ago ({last_dt.strftime('%b %d')}) — data may be stale", "red"
    except:
        return "⚠️ Last sync date unreadable", "red"

# =============================================================================
# WHITELIST — Qualifying accounts from Cin7 Contacts
# =============================================================================
QUALIFYING_GROUPS  = {'CM', 'TP', 'VL'}
QUALIFYING_STAGE   = 'wholesale'

def fetch_qualifying_accounts(username: str, api_key: str,
                               progress_callback=None) -> set:
    """
    Fetch all active Cin7 contacts where:
      - isActive = True
      - group in CM, TP, VL
      - stages contains 'wholesale'

    Returns a set of normalised (uppercase, stripped) company names.
    """
    qualifying = set()
    page = 1
    total_checked = 0

    while True:
        if progress_callback:
            progress_callback(f"Loading account whitelist... ({len(qualifying)} qualifying so far)")

        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/Contacts",
                auth=(username, api_key),
                params={"page": page, "rows": 250, "isActive": "true"},
                timeout=30
            )
            if r.status_code != 200:
                break

            batch = r.json()
            if not batch:
                break

            for c in batch:
                if not isinstance(c, dict):
                    continue

                total_checked += 1
                group = str(c.get("group") or "").strip().upper()
                if group not in QUALIFYING_GROUPS:
                    continue

                stages_raw = c.get("stages")
                if isinstance(stages_raw, list):
                    stages_str = " ".join(str(s).lower() for s in stages_raw)
                else:
                    stages_str = str(stages_raw or "").lower()

                if QUALIFYING_STAGE not in stages_str:
                    continue

                name = str(c.get("name") or "").strip()
                if name:
                    qualifying.add(name.upper())

            if len(batch) < 250:
                break
            page += 1

        except Exception:
            break

    return qualifying


def is_on_whitelist(order: dict, whitelist: set) -> bool:
    """Check if an order's company is on the qualifying whitelist."""
    if not whitelist:
        return False
    company = str(
        order.get('company') or order.get('billingCompany') or ''
    ).strip().upper()
    return company in whitelist


# =============================================================================
# CLASSIFICATION
# =============================================================================
def classify_order(order: dict) -> str:
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
    start_str = since.strftime("%Y-%m-%dT00:00:00Z")
    end_str = until.strftime("%Y-%m-%dT23:59:59Z")
    all_orders = []
    page = 1
    while True:
        if progress_callback:
            progress_callback(phase="fetching", page=page, orders_so_far=len(all_orders),
                             message=f"Fetching page {page}..." if page > 1 else "Connecting to Cin7...")
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders",
            auth=(username, api_key),
            params={"where": f"dispatchedDate >= '{start_str}' AND dispatchedDate <= '{end_str}'",
                    "page": page, "rows": 250},
            timeout=60
        )
        if r.status_code != 200:
            break
        orders = r.json()
        if not orders:
            break
        all_orders.extend(orders)
        if progress_callback:
            progress_callback(phase="fetching", page=page, orders_so_far=len(all_orders),
                             message=f"Found {len(all_orders)} orders...")
        if len(orders) < 250:
            break
        page += 1
    if progress_callback:
        progress_callback(phase="processing", page=page, orders_so_far=len(all_orders),
                         message="Classifying orders...")
    for o in all_orders:
        o['_segment'] = classify_order(o)
    if progress_callback:
        progress_callback(phase="complete", page=page, orders_so_far=len(all_orders),
                         message=f"Complete! {len(all_orders)} orders loaded.")
    return all_orders

def fetch_order_details(username: str, api_key: str, order_id: str) -> dict:
    for params in [None, {"where": f"id={order_id}"}, {"where": f"id='{order_id}'"}]:
        try:
            url = f"https://api.cin7.com/api/v1/SalesOrders/{order_id}" if params is None else "https://api.cin7.com/api/v1/SalesOrders"
            r = requests.get(url, auth=(username, api_key), params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    return data[0]
                elif isinstance(data, dict) and data:
                    return data
        except:
            pass
    return None

# =============================================================================
# HUBSPOT API
# =============================================================================
HUBSPOT_STAGE_CLOSED_WON = "closedwon"
HUBSPOT_STAGE_PENDING_PAYMENT = "decisionmakerboughtin"
NET_PAYMENT_TERMS = ['net 30', 'net 60', 'net 90', 'net 15', 'net 30 days', 'net 60 days', 'net 45']

def is_paid(order: dict) -> bool:
    total = order.get('total', 0) or 0
    if float(total) == 0:
        return True
    paid = str(order.get('paid') or '').strip()
    if '100%' in paid:
        return True
    payment_terms = str(order.get('paymentTerms') or '').lower()
    if any(net in payment_terms for net in NET_PAYMENT_TERMS):
        return False
    return True

def get_payment_debug(order: dict) -> str:
    parts = []
    if order.get('paid'):
        parts.append(f"paid:{order.get('paid')}")
    if order.get('paymentTerms'):
        parts.append(f"terms:{order.get('paymentTerms')}")
    return ' | '.join(parts) if parts else "(no payment data)"

def get_deal_stage(order: dict) -> tuple:
    if is_paid(order):
        return HUBSPOT_STAGE_CLOSED_WON, "Closed Won"
    return HUBSPOT_STAGE_PENDING_PAYMENT, "Pending Payment"

def get_headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

# -----------------------------------------------------------------------------
# SEARCH FUNCTIONS
# -----------------------------------------------------------------------------
def search_deal_by_order_ref(api_key: str, order_ref: str) -> dict:
    headers = get_headers(api_key)
    search_body = {
        "filterGroups": [{"filters": [{"propertyName": "dealname", "operator": "CONTAINS_TOKEN", "value": order_ref}]}],
        "properties": ["dealname", "amount", "dealstage", "pipeline", "closedate"]
    }
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search",
                         headers=headers, json=search_body, timeout=15)
        if r.status_code == 200:
            for deal in r.json().get('results', []):
                if order_ref in deal.get('properties', {}).get('dealname', ''):
                    return deal
    except:
        pass
    return None

def scan_for_duplicates(api_key: str, order_refs: list, progress_callback=None) -> dict:
    headers = get_headers(api_key)
    duplicates = {}
    total = len(order_refs)
    for i, order_ref in enumerate(order_refs):
        if progress_callback:
            progress_callback((i + 1) / total)
        search_body = {
            "filterGroups": [{"filters": [{"propertyName": "dealname", "operator": "CONTAINS_TOKEN", "value": order_ref}]}],
            "properties": ["dealname", "amount", "dealstage", "closedate", "createdate"],
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
            "limit": 20
        }
        try:
            r = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search",
                             headers=headers, json=search_body, timeout=15)
            if r.status_code == 200:
                matched = [d for d in r.json().get("results", [])
                          if order_ref in d.get("properties", {}).get("dealname", "")]
                if len(matched) >= 2:
                    duplicates[order_ref] = matched
        except:
            pass
    return duplicates

def search_contact_by_email(api_key: str, email: str) -> dict:
    if not email:
        return None
    headers = get_headers(api_key)
    search_body = {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "properties": ["email", "firstname", "lastname", "phone", "company"]
    }
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/search",
                         headers=headers, json=search_body, timeout=15)
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results:
                return results[0]
    except:
        pass
    return None

def search_company_by_name(api_key: str, company_name: str) -> dict:
    if not company_name:
        return None
    headers = get_headers(api_key)
    search_body = {
        "filterGroups": [{"filters": [{"propertyName": "name", "operator": "EQ", "value": company_name}]}],
        "properties": ["name", "phone", "address", "city", "state", "zip", "country"]
    }
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies/search",
                         headers=headers, json=search_body, timeout=15)
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
    try:
        r = requests.patch(f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
                          headers=get_headers(api_key), json={"properties": properties}, timeout=15)
        return r.status_code == 200
    except:
        return False

def update_contact(api_key: str, contact_id: str, properties: dict) -> bool:
    try:
        r = requests.patch(f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}",
                          headers=get_headers(api_key), json={"properties": properties}, timeout=15)
        return r.status_code == 200
    except:
        return False

def update_company(api_key: str, company_id: str, properties: dict) -> bool:
    try:
        r = requests.patch(f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}",
                          headers=get_headers(api_key), json={"properties": properties}, timeout=15)
        return r.status_code == 200
    except:
        return False

# -----------------------------------------------------------------------------
# ASSOCIATION FUNCTIONS
# -----------------------------------------------------------------------------
def get_deal_associations(api_key: str, deal_id: str) -> dict:
    headers = get_headers(api_key)
    result = {"companies": [], "contacts": []}
    for obj_type in ["companies", "contacts"]:
        try:
            r = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/{obj_type}",
                headers=headers, timeout=15)
            if r.status_code == 200:
                result[obj_type] = r.json().get("results", [])
        except:
            pass
    return result

def associate_deal(api_key: str, deal_id: str, obj_type: str, obj_id: str) -> bool:
    assoc_type = "deal_to_company" if obj_type == "companies" else "deal_to_contact"
    try:
        r = requests.put(
            f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/{obj_type}/{obj_id}/{assoc_type}",
            headers=get_headers(api_key), timeout=15)
        return r.status_code in [200, 201, 204]
    except:
        return False

# -----------------------------------------------------------------------------
# CREATE FUNCTIONS
# -----------------------------------------------------------------------------
def create_contact(api_key: str, email: str, first_name: str, last_name: str,
                   company: str, phone: str = "") -> str:
    properties = {"email": email}
    if first_name: properties["firstname"] = first_name
    if last_name: properties["lastname"] = last_name
    if company: properties["company"] = company
    if phone: properties["phone"] = phone
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts",
                         headers=get_headers(api_key), json={"properties": properties}, timeout=15)
        if r.status_code == 201:
            return r.json()['id']
    except:
        pass
    return None

def get_hubspot_owners(api_key: str) -> dict:
    owners = {}
    try:
        r = requests.get("https://api.hubapi.com/crm/v3/owners",
                        headers=get_headers(api_key), params={"limit": 100}, timeout=15)
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
    headers = get_headers(api_key)
    updated = skipped = errors = 0
    all_companies = []
    after = None
    while True:
        params = {"limit": 100, "properties": "name,hubspot_owner_id"}
        if after: params["after"] = after
        try:
            r = requests.get("https://api.hubapi.com/crm/v3/objects/companies",
                            headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                all_companies.extend(data.get('results', []))
                after = data.get('paging', {}).get('next', {}).get('after')
                if not after: break
            else: break
        except: break
    for company in all_companies:
        cid = company.get('id')
        cname = (company.get('properties', {}).get('name') or '').strip().upper()
        current_owner = company.get('properties', {}).get('hubspot_owner_id')
        rep_email = company_to_rep.get(cname)
        if not rep_email: skipped += 1; continue
        new_owner_id = owner_lookup.get(rep_email)
        if not new_owner_id: skipped += 1; continue
        if current_owner == new_owner_id: skipped += 1; continue
        try:
            r = requests.patch(f"https://api.hubapi.com/crm/v3/objects/companies/{cid}",
                              headers=headers, json={"properties": {"hubspot_owner_id": new_owner_id}}, timeout=15)
            if r.status_code == 200: updated += 1
            else: errors += 1
        except: errors += 1
    return updated, skipped, errors

def bulk_update_deal_owners(api_key: str, company_to_rep: dict, owner_lookup: dict) -> tuple:
    headers = get_headers(api_key)
    updated = skipped = errors = 0
    all_deals = []
    after = None
    while True:
        params = {"limit": 100, "properties": "dealname,hubspot_owner_id", "associations": "companies"}
        if after: params["after"] = after
        try:
            r = requests.get("https://api.hubapi.com/crm/v3/objects/deals",
                            headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                all_deals.extend(data.get('results', []))
                after = data.get('paging', {}).get('next', {}).get('after')
                if not after: break
            else: break
        except: break
    company_id_to_name = {}
    after = None
    while True:
        params = {"limit": 100, "properties": "name"}
        if after: params["after"] = after
        try:
            r = requests.get("https://api.hubapi.com/crm/v3/objects/companies",
                            headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                for c in data.get('results', []):
                    company_id_to_name[c['id']] = (c.get('properties', {}).get('name') or '').strip().upper()
                after = data.get('paging', {}).get('next', {}).get('after')
                if not after: break
            else: break
        except: break
    for deal in all_deals:
        did = deal.get('id')
        current_owner = deal.get('properties', {}).get('hubspot_owner_id')
        assocs = deal.get('associations', {}).get('companies', {}).get('results', [])
        if not assocs: skipped += 1; continue
        cname = company_id_to_name.get(assocs[0].get('id'), '')
        rep_email = company_to_rep.get(cname)
        if not rep_email: skipped += 1; continue
        new_owner_id = owner_lookup.get(rep_email)
        if not new_owner_id: skipped += 1; continue
        if current_owner == new_owner_id: skipped += 1; continue
        try:
            r = requests.patch(f"https://api.hubapi.com/crm/v3/objects/deals/{did}",
                              headers=headers, json={"properties": {"hubspot_owner_id": new_owner_id}}, timeout=15)
            if r.status_code == 200: updated += 1
            else: errors += 1
        except: errors += 1
    return updated, skipped, errors

def bulk_sync_deal_closedates(hs_api_key: str, cin7_username: str, cin7_api_key: str,
                               progress_callback=None) -> tuple:
    headers = get_headers(hs_api_key)
    updated = skipped = errors = 0
    details = []
    all_deals = []
    after = None
    while True:
        params = {"limit": 100, "properties": "dealname,closedate"}
        if after: params["after"] = after
        try:
            r = requests.get("https://api.hubapi.com/crm/v3/objects/deals",
                            headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                all_deals.extend(data.get('results', []))
                after = data.get('paging', {}).get('next', {}).get('after')
                if not after: break
            else: break
        except: break
    total_deals = len(all_deals)
    for i, deal in enumerate(all_deals):
        if progress_callback:
            progress_callback((i + 1) / total_deals)
        did = deal.get('id')
        deal_name = deal.get('properties', {}).get('dealname', '')
        current_closedate = deal.get('properties', {}).get('closedate', '')
        order_ref = deal_name.split(' - ')[-1].strip() if ' - ' in deal_name else deal_name.strip()
        if not order_ref: skipped += 1; continue
        try:
            r = requests.get("https://api.cin7.com/api/v1/SalesOrders",
                            auth=(cin7_username, cin7_api_key),
                            params={"where": f"reference='{order_ref}'"}, timeout=15)
            if r.status_code != 200: skipped += 1; continue
            orders = r.json()
            if not orders: skipped += 1; continue
            cin7_order_date = orders[0].get('orderDate') or orders[0].get('createdDate') or ''
            if not cin7_order_date: skipped += 1; continue
            cin7_date_str = cin7_order_date[:10]
            current_date_str = current_closedate[:10] if current_closedate else ''
            if cin7_date_str == current_date_str: skipped += 1; continue
            r = requests.patch(f"https://api.hubapi.com/crm/v3/objects/deals/{did}",
                              headers=headers, json={"properties": {"closedate": cin7_order_date}}, timeout=15)
            if r.status_code == 200:
                updated += 1
                details.append(f"{order_ref}: {current_date_str or 'empty'} → {cin7_date_str}")
            else:
                errors += 1
        except: errors += 1
    return updated, skipped, errors, details

def create_company(api_key: str, name: str, phone: str = "", address: str = "",
                   city: str = "", state: str = "", zip_code: str = "", country: str = "",
                   owner_id: str = None) -> str:
    if not name: return None
    properties = {"name": name}
    if phone: properties["phone"] = phone
    if address: properties["address"] = address
    if city: properties["city"] = city
    if state: properties["state"] = state
    if zip_code: properties["zip"] = zip_code
    if country: properties["country"] = country
    if owner_id: properties["hubspot_owner_id"] = owner_id
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies",
                         headers=get_headers(api_key), json={"properties": properties}, timeout=15)
        if r.status_code == 201:
            return r.json()['id']
    except: pass
    return None

def create_deal(api_key: str, order: dict, contact_id: str = None,
                company_id: str = None, owner_id: str = None) -> tuple:
    headers = get_headers(api_key)
    stage_id, stage_label = get_deal_stage(order)
    order_ref = order.get('reference', '')
    company = order.get('company') or order.get('billingCompany') or ''
    total = order.get('total', 0) or 0
    payment_terms = order.get('paymentTerms') or 'Standard'
    total_owing = order.get('totalOwing', total)
    order_date = order.get('orderDate') or order.get('createdDate') or ''
    order_date_display = order_date[:10] if order_date else 'Unknown'
    deal_name = f"{company} - {order_ref}" if company else order_ref
    deal_data = {
        "properties": {k: v for k, v in {
            "dealname": deal_name,
            "amount": str(total),
            "dealstage": stage_id,
            "pipeline": "default",
            "closedate": order_date if order_date else None,
            "description": f"Cin7 Order: {order_ref}\nOrder Date: {order_date_display}\nPayment Terms: {payment_terms}\nOwing: ${total_owing}"
        }.items() if v is not None}
    }
    if owner_id:
        deal_data["properties"]["hubspot_owner_id"] = owner_id
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals",
                         headers=headers, json=deal_data, timeout=15)
        if r.status_code == 201:
            deal_id = r.json()['id']
            if contact_id:
                requests.put(f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/contacts/{contact_id}/deal_to_contact",
                           headers=headers, timeout=15)
            if company_id:
                requests.put(f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/companies/{company_id}/deal_to_company",
                           headers=headers, timeout=15)
            return deal_id, stage_label
        else:
            return None, f"Error {r.status_code}: {r.text[:100]}"
    except Exception as e:
        return None, str(e)

def get_deal_line_items(api_key: str, deal_id: str) -> list:
    try:
        r = requests.get(f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/line_items",
                        headers=get_headers(api_key), timeout=15)
        if r.status_code == 200:
            return r.json().get('results', [])
    except: pass
    return []

def create_line_items(api_key: str, deal_id: str, order: dict) -> tuple:
    headers = get_headers(api_key)
    line_items = None
    for field_name in ['lineItems', 'lines', 'salesOrderLines', 'orderLines', 'items', 'lineDetails']:
        if field_name in order and order[field_name]:
            line_items = order[field_name]
            break
    if not line_items:
        return 0, ["No line items found"]
    errors = []
    batch_inputs = []
    for item in line_items:
        product_name = item.get('name') or item.get('productName') or item.get('description') or 'Product'
        sku = item.get('code') or item.get('sku') or item.get('productCode') or ''
        quantity = item.get('qty') or item.get('quantity') or 1
        unit_price = item.get('unitPrice') or item.get('price') or 0
        name = f"{product_name} ({sku})" if sku else product_name
        batch_inputs.append({
            "properties": {"name": name[:250], "quantity": str(quantity),
                          "price": str(unit_price), "amount": str(float(quantity) * float(unit_price))},
            "associations": [{"to": {"id": deal_id},
                             "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 20}]}]
        })
    created = 0
    for i in range(0, len(batch_inputs), 100):
        try:
            r = requests.post("https://api.hubapi.com/crm/v3/objects/line_items/batch/create",
                             headers=headers, json={"inputs": batch_inputs[i:i+100]}, timeout=60)
            if r.status_code in [200, 201]:
                created += len(r.json().get('results', []))
            else:
                errors.append(f"Batch create failed: {r.status_code} - {r.text[:200]}")
        except Exception as e:
            errors.append(f"Batch error: {str(e)}")
    return created, errors

# -----------------------------------------------------------------------------
# FULL SYNC FUNCTION
# -----------------------------------------------------------------------------
def sync_order_to_hubspot(api_key: str, order: dict, cin7_username: str = None,
                          cin7_api_key: str = None, contact_cache: dict = None,
                          company_cache: dict = None, cache_lock=None,
                          owner_lookup: dict = None) -> dict:
    result = {"order_ref": order.get('reference', 'Unknown'), "success": False,
              "action": "none", "deal_stage": "", "details": []}

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
            cin7_date_str = cin7_order_date[:10]
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

        existing_line_items = get_deal_line_items(api_key, deal_id)
        if not existing_line_items:
            lc, le = create_line_items(api_key, deal_id, order)
            if lc > 0:
                result["details"].append(f"{lc} line items added")
                if result["action"] == "skipped": result["action"] = "updated"
            if le: result["details"].append(f"Line item errors: {le[:3]}")
        else:
            result["details"].append(f"{len(existing_line_items)} line items already exist")

        existing_assocs = get_deal_associations(api_key, deal_id)
        has_company = len(existing_assocs["companies"]) > 0
        has_contact = len(existing_assocs["contacts"]) > 0

        if not has_company and company_name:
            existing_company = search_company_by_name(api_key, company_name)
            if existing_company:
                company_id = existing_company["id"]
                result["details"].append("Company found")
            else:
                company_id = create_company(api_key, company_name, phone, address, city, state, zip_code, country, owner_id)
                if company_id: result["details"].append("Company created")
                else: company_id = None
            if company_id and associate_deal(api_key, deal_id, "companies", company_id):
                result["details"].append("Company association repaired")
                if result["action"] == "skipped": result["action"] = "updated"

        if not has_contact and email:
            existing_contact = search_contact_by_email(api_key, email)
            if existing_contact:
                contact_id = existing_contact["id"]
                result["details"].append("Contact found")
            else:
                contact_id = create_contact(api_key, email, first_name, last_name, company_name, phone)
                if contact_id: result["details"].append("Contact created")
                else: contact_id = None
            if contact_id and associate_deal(api_key, deal_id, "contacts", contact_id):
                result["details"].append("Contact association repaired")
                if result["action"] == "skipped": result["action"] = "updated"

        if result["action"] != "update_failed":
            result["success"] = True

    else:
        contact_id = None
        company_id = None

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
                    if first_name and props.get('firstname', '') != first_name: update_props['firstname'] = first_name
                    if last_name and props.get('lastname', '') != last_name: update_props['lastname'] = last_name
                    if phone and props.get('phone', '') != phone: update_props['phone'] = phone
                    if company_name and props.get('company', '') != company_name: update_props['company'] = company_name
                    if update_props:
                        update_contact(api_key, contact_id, update_props)
                        result["details"].append("Contact updated")
                    else:
                        result["details"].append("Contact unchanged")
                else:
                    contact_id = create_contact(api_key, email, first_name, last_name, company_name, phone)
                    result["details"].append("Contact created" if contact_id else "Contact creation failed")
                if contact_id and contact_cache is not None and cache_lock:
                    with cache_lock:
                        contact_cache[email.lower()] = contact_id

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
                    if phone and props.get('phone', '') != phone: update_props['phone'] = phone
                    if address and props.get('address', '') != address: update_props['address'] = address
                    if city and props.get('city', '') != city: update_props['city'] = city
                    if state and props.get('state', '') != state: update_props['state'] = state
                    if zip_code and props.get('zip', '') != zip_code: update_props['zip'] = zip_code
                    if update_props:
                        update_company(api_key, company_id, update_props)
                        result["details"].append("Company updated")
                    else:
                        result["details"].append("Company unchanged")
                else:
                    company_id = create_company(api_key, company_name, phone, address, city, state, zip_code, country, owner_id)
                    result["details"].append("Company created" if company_id else "Company creation failed")
                if company_id and company_cache is not None and cache_lock:
                    with cache_lock:
                        company_cache[company_name.lower()] = company_id

        deal_id, stage_or_error = create_deal(api_key, order, contact_id, company_id, owner_id)
        if deal_id:
            result["action"] = "created"
            result["success"] = True
            result["details"].append(f"Deal created as {stage_or_error}")
            lc, le = create_line_items(api_key, deal_id, order)
            if lc > 0: result["details"].append(f"{lc} line items added")
            if le: result["details"].append(f"Line item errors: {le[:3]}")
        else:
            result["action"] = "create_failed"
            result["details"].append(f"Deal creation failed: {stage_or_error}")

    return result

def push_orders_to_hubspot(api_key: str, orders: list, progress_callback=None,
                           cin7_username: str = None, cin7_api_key: str = None) -> dict:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    results = {"created": [], "updated": [], "skipped": [], "failed": [],
               "closed_won": 0, "pending_payment": 0}
    owner_lookup = get_hubspot_owners(api_key)
    contact_cache = {}
    company_cache = {}
    cache_lock = threading.Lock()
    progress_lock = threading.Lock()
    completed_count = [0]

    def process_order(order):
        return sync_order_to_hubspot(api_key, order, cin7_username, cin7_api_key,
                                     contact_cache, company_cache, cache_lock, owner_lookup)

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_order = {executor.submit(process_order, order): order for order in orders}
        for future in as_completed(future_to_order):
            sync_result = future.result()
            with progress_lock:
                completed_count[0] += 1
                if progress_callback:
                    progress_callback(completed_count[0] / len(orders))
            if sync_result["success"]:
                if sync_result["action"] == "created": results["created"].append(sync_result)
                elif sync_result["action"] == "updated": results["updated"].append(sync_result)
                else: results["skipped"].append(sync_result)
                if sync_result["deal_stage"] == "Closed Won": results["closed_won"] += 1
                else: results["pending_payment"] += 1
            else:
                results["failed"].append(sync_result)
    return results

def test_hubspot(api_key: str) -> tuple:
    try:
        r = requests.get("https://api.hubapi.com/crm/v3/objects/contacts",
                        headers={"Authorization": f"Bearer {api_key}"},
                        params={"limit": 1}, timeout=15)
        if r.status_code == 200: return True, "Connected"
        elif r.status_code == 401: return False, "Invalid API key"
        else: return False, f"Error {r.status_code}"
    except Exception as e:
        return False, str(e)

def fetch_pipeline_stages(api_key: str) -> list:
    try:
        r = requests.get("https://api.hubapi.com/crm/v3/pipelines/deals",
                        headers={"Authorization": f"Bearer {api_key}"}, timeout=15)
        if r.status_code == 200:
            return r.json().get('results', [])
    except: pass
    return []

# =============================================================================
# FILTER ORDERS
# =============================================================================
def filter_orders(orders: list, exclude_shopify: bool,
                  whitelist: set = None) -> tuple:
    """
    Filter orders into: to_import, to_review, to_skip

    Whitelist logic (new):
      If whitelist is provided, only orders whose company matches a
      qualifying account (group CM/TP/VL + wholesale stage) are imported.
      Non-matching orders go to to_skip with reason 'Not on whitelist'.

    Legacy fallback:
      If whitelist is None or empty, falls back to source-based
      classification (source = 'Backend') so nothing breaks during
      a transition period.
    """
    to_import = []
    to_review = []
    to_skip   = []

    use_whitelist = bool(whitelist)

    for o in orders:
        source    = (o.get('source') or '').lower()
        total     = o.get('total', 0) or 0
        status    = (o.get('stage') or o.get('status') or '').lower()
        company   = (o.get('company') or o.get('billingCompany') or '').lower()
        has_email = bool((o.get('email') or o.get('memberEmail') or '').strip())
        email_address = (o.get('email') or o.get('memberEmail') or '').lower()

        # ── Whitelist filter (primary) ──────────────────────────────────
        if use_whitelist:
            if not is_on_whitelist(o, whitelist):
                o['_skip_reason'] = 'Not on whitelist (group/stage)'
                to_skip.append(o)
                continue
        else:
            # ── Legacy fallback: source-based classification ─────────────
            segment = o.get('_segment', 'Retail')
            if segment == 'Retail':
                o['_skip_reason'] = 'Retail segment'
                to_skip.append(o)
                continue

        # ── Internal / Vivant accounts ──────────────────────────────────
        if 'vivant' in company or 'vivant' in email_address:
            o['_review_reason'] = 'Internal (Vivant in company or email)'
            to_review.append(o)
            continue

        # ── Shopify retail exclusion ────────────────────────────────────
        if exclude_shopify and 'shopify retail' in source:
            o['_skip_reason'] = 'Shopify Retail excluded'
            to_skip.append(o)
            continue

        # ── Status filter ───────────────────────────────────────────────
        if status not in IMPORTABLE_STATUSES:
            o['_skip_reason'] = f'Status: {status}'
            to_skip.append(o)
            continue

        # ── No email + dispatched → likely employee ─────────────────────
        if not has_email and status == 'dispatched':
            o['_review_reason'] = 'No email (likely employee)'
            to_review.append(o)
            continue

        # ── $0 orders ───────────────────────────────────────────────────
        if total == 0:
            if has_email:
                to_import.append(o)
            else:
                o['_review_reason'] = '$0 + No email (likely employee)'
                to_review.append(o)
            continue

        # ── All checks passed — import ──────────────────────────────────
        to_import.append(o)

    return to_import, to_review, to_skip

# =============================================================================
# DISPLAY HELPERS
# =============================================================================
def order_to_summary(order: dict, include_reason: bool = False) -> dict:
    total = order.get('total', 0) or 0
    customer = (order.get('customerName') or order.get('contactName') or order.get('billingName') or
                order.get('deliveryName') or order.get('memberName') or order.get('contact') or '')
    if not customer:
        first = order.get('firstName') or order.get('billingFirstName') or ''
        last = order.get('lastName') or order.get('billingLastName') or ''
        customer = f"{first} {last}".strip()
    order_date = order.get('orderDate') or order.get('createdDate') or ''
    sales_person_email = order.get('salesPersonEmail') or ''
    rep_display = sales_person_email.split('@')[0] if sales_person_email else '⚠️ No Rep'
    result = {
        'Order #': order.get('reference', ''),
        'Source': order.get('source', ''),
        'Segment': order.get('_segment', ''),
        'Total_Numeric': float(total),
        'Total': float(total),
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
    if not orders:
        return pd.DataFrame()
    df = pd.DataFrame([order_to_summary(o, include_reason) for o in orders])
    df['Total_Numeric'] = pd.to_numeric(df['Total_Numeric'], errors='coerce').fillna(0)
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    df = df.sort_values('Total_Numeric', ascending=False).drop(columns=['Total_Numeric'])
    return df

def get_column_config():
    return {'Total': st.column_config.NumberColumn('Total', format='$ %.2f')}

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
            except: pass

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
                st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")
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
                st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")
            else:
                st.error("Enter API key")
        hs_ok, _ = test_hubspot(hs_key) if hs_key else (False, "")
        st.caption(f"Status: {'✅ Connected' if hs_ok else '❌ Not connected'}")

        # ---- Staleness banner ----
        staleness_label, staleness_color = get_staleness_banner()
        if staleness_color == "green":
            st.success(staleness_label)
        elif staleness_color == "orange":
            st.warning(staleness_label)
        else:
            st.error(staleness_label)

        st.divider()

        # Filters
        st.header("⚙️ Filters")
        exclude_shopify = st.checkbox("Exclude 'Shopify Retail'", value=True)

        # Whitelist status
        wl = st.session_state.whitelist
        if wl is None:
            st.caption("🏢 Whitelist: not loaded — click Fetch Orders")
        elif len(wl) == 0:
            st.warning("⚠️ Whitelist empty — no qualifying accounts found")
        else:
            st.caption(f"🏢 Whitelist: **{len(wl)} qualifying accounts** (CM/TP/VL + wholesale)")
        st.divider()

        # Remember credentials
        remember = st.checkbox("🔑 Remember credentials", value=config.get('remember', False),
                               help="Save credentials locally")
        if remember:
            save_config({'cin7_username': cin7_user, 'cin7_api_key': cin7_key,
                        'hubspot_api_key': hs_key, 'remember': True})
            st.caption("✅ Credentials saved locally")
        else:
            if config.get('remember'):
                clear_config()

        st.divider()

        # -------------------------------------------------------------------------
        # ADVANCED TOOLS
        # -------------------------------------------------------------------------
        with st.expander("🛠️ Advanced Tools", expanded=False):
            st.caption("Pipeline diagnostics, bulk fixes, and cleanup tools.")

            # -- Pipeline Stages --
            st.divider()
            st.markdown("**🔧 HubSpot Pipeline Stages**")
            st.caption("View your HubSpot deal stages to configure the connector")
            if hs_key:
                if st.button("🔍 Fetch Pipeline Stages"):
                    pipelines = fetch_pipeline_stages(hs_key)
                    if pipelines:
                        for pipeline in pipelines:
                            st.markdown(f"**📊 {pipeline['label']}** (ID: `{pipeline['id']}`)")
                            stage_data = [{"Order": s['displayOrder'], "Stage Name": s['label'], "Stage ID": s['id']}
                                         for s in pipeline.get('stages', [])]
                            st.dataframe(pd.DataFrame(stage_data), use_container_width=True, hide_index=True)
                        st.info("**Current config:** Closed Won: `closedwon` | Pending Payment: `decisionmakerboughtin`")
                    else:
                        st.warning("Could not fetch pipelines. Check your API key.")
            else:
                st.warning("Enter HubSpot API key first")

            # -- Bulk Owner Sync --
            st.divider()
            st.markdown("**👥 Bulk Owner Sync (One-Time Setup)**")
            st.caption("Update existing HubSpot Companies and Deals with correct owners based on your mapping spreadsheet")
            if not hs_key:
                st.warning("Enter HubSpot API key first")
            else:
                uploaded_file = st.file_uploader("Upload Company → Owner mapping spreadsheet (Excel)",
                                                 type=['xlsx', 'xls'], key="owner_mapping_file")
                if uploaded_file:
                    try:
                        df = pd.read_excel(uploaded_file)
                        required_cols = ['Company Name', 'Rep Email']
                        if not all(col in df.columns for col in required_cols):
                            st.error(f"Spreadsheet must have columns: {required_cols}")
                        else:
                            company_to_rep = {}
                            for _, row in df.iterrows():
                                co = str(row.get('Company Name', '')).strip().upper()
                                em = str(row.get('Rep Email', '')).strip().lower()
                                if co and em and em != 'nan':
                                    company_to_rep[co] = em
                            st.success(f"✅ Loaded {len(company_to_rep)} company → rep mappings")
                            with st.expander("Preview mappings (first 10)"):
                                for co, em in list(company_to_rep.items())[:10]:
                                    st.caption(f"• {co} → {em}")
                            owner_lookup = get_hubspot_owners(hs_key)
                            if owner_lookup: st.info(f"Found {len(owner_lookup)} HubSpot owners")
                            else: st.warning("Could not fetch HubSpot owners")
                            st.divider()
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("🔄 Sync Company Owners", type="primary"):
                                    with st.spinner("Updating company owners..."):
                                        u, s, e = bulk_update_company_owners(hs_key, company_to_rep, owner_lookup)
                                    st.success(f"✅ Updated: {u} companies")
                                    if s: st.info(f"⏭️ Skipped: {s}")
                                    if e: st.warning(f"⚠️ Errors: {e}")
                            with col2:
                                if st.button("🔄 Sync Deal Owners", type="primary"):
                                    with st.spinner("Updating deal owners..."):
                                        u, s, e = bulk_update_deal_owners(hs_key, company_to_rep, owner_lookup)
                                    st.success(f"✅ Updated: {u} deals")
                                    if s: st.info(f"⏭️ Skipped: {s}")
                                    if e: st.warning(f"⚠️ Errors: {e}")
                    except Exception as e:
                        st.error(f"Error reading file: {str(e)}")

            # -- Bulk Close Date Sync --
            st.divider()
            st.markdown("**📅 Bulk Close Date Sync (Fix Historical Deals)**")
            st.caption("Update close dates on ALL existing HubSpot deals using Cin7 order dates")
            if not hs_key:
                st.warning("Enter HubSpot API key first")
            elif not cin7_user or not cin7_key:
                st.warning("Enter Cin7 credentials first")
            else:
                st.info("Fetches all HubSpot deals, looks up each order in Cin7, updates closedate if it differs. ⚠️ May take several minutes.")
                if st.button("🔄 Sync All Deal Close Dates", type="primary", key="bulk_closedate_sync"):
                    progress_bar = st.progress(0)
                    status = st.empty()
                    def update_progress(pct):
                        progress_bar.progress(pct)
                        status.info(f"Processing deals... {int(pct * 100)}%")
                    with st.spinner("Syncing close dates from Cin7..."):
                        u, s, e, details = bulk_sync_deal_closedates(hs_key, cin7_user, cin7_key, update_progress)
                    progress_bar.empty(); status.empty()
                    st.success(f"✅ Updated: {u} deals")
                    if s: st.info(f"⏭️ Skipped: {s}")
                    if e: st.warning(f"⚠️ Errors: {e}")
                    if details:
                        with st.expander(f"📋 Updated Deals ({len(details)})"):
                            for detail in details[:50]:
                                st.caption(f"• {detail}")
                            if len(details) > 50:
                                st.caption(f"...and {len(details) - 50} more")

            # -- HubSpot Cleanup --
            st.divider()
            st.markdown("**🧹 HubSpot Cleanup (Delete Duplicates)**")
            st.caption("Find and delete duplicate or junk deals for a specific company/contact")
            if not hs_key:
                st.warning("Enter HubSpot API key first")
            else:
                cleanup_search = st.text_input("Search company or contact name",
                                               placeholder="e.g., MONARCH SKIN", key="cleanup_search")
                if st.button("🔍 Find Deals", key="cleanup_find") and cleanup_search:
                    headers = get_headers(hs_key)
                    search_body = {
                        "filterGroups": [{"filters": [{"propertyName": "dealname",
                                                       "operator": "CONTAINS_TOKEN", "value": cleanup_search}]}],
                        "properties": ["dealname", "amount", "dealstage", "closedate", "createdate", "hubspot_owner_id"],
                        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
                        "limit": 100
                    }
                    try:
                        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search",
                                         headers=headers, json=search_body, timeout=30)
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
                        deal_data.append({'Deal ID': d.get('id'), 'Deal Name': props.get('dealname', ''),
                                         'Amount': f"${float(props.get('amount') or 0):,.2f}",
                                         'Close Date': (props.get('closedate') or '')[:10],
                                         'Created': (props.get('createdate') or '')[:10]})
                    st.dataframe(pd.DataFrame(deal_data), use_container_width=True, hide_index=True)

                    order_refs = {}
                    for d in deals:
                        name = d.get('properties', {}).get('dealname', '')
                        ref = name.split(' - ')[-1].strip() if ' - ' in name else name.strip()
                        order_refs.setdefault(ref, []).append(d)
                    duplicates = {ref: dl for ref, dl in order_refs.items() if len(dl) > 1}
                    if duplicates:
                        st.warning(f"⚠️ Found {len(duplicates)} order(s) with duplicate deals:")
                        for ref, dup_deals in duplicates.items():
                            st.caption(f"• **{ref}**: {len(dup_deals)} deals")

                    st.divider()
                    delete_options = [
                        f"{d.get('id')} - {d.get('properties', {}).get('dealname', '')} "
                        f"(Created: {(d.get('properties', {}).get('createdate') or '')[:10]})"
                        for d in deals
                    ]
                    selected_to_delete = st.multiselect("Select deals to DELETE",
                                                        options=delete_options, key="deals_to_delete")
                    if selected_to_delete:
                        st.error(f"⚠️ You are about to DELETE {len(selected_to_delete)} deal(s). This cannot be undone!")
                        col1, col2 = st.columns(2)
                        with col1:
                            confirm_del = st.checkbox("I understand this is permanent", key="confirm_delete")
                        with col2:
                            if confirm_del and st.button("🗑️ DELETE Selected Deals", type="primary", key="delete_deals"):
                                headers = get_headers(hs_key)
                                deleted = errors = 0
                                progress = st.progress(0)
                                for i, selection in enumerate(selected_to_delete):
                                    did = selection.split(' - ')[0]
                                    try:
                                        r = requests.delete(f"https://api.hubapi.com/crm/v3/objects/deals/{did}",
                                                          headers=headers, timeout=15)
                                        if r.status_code == 204: deleted += 1
                                        else: errors += 1
                                    except: errors += 1
                                    progress.progress((i + 1) / len(selected_to_delete))
                                progress.empty()
                                st.success(f"✅ Deleted {deleted} deal(s)")
                                if errors: st.warning(f"⚠️ {errors} deletion(s) failed")
                                st.session_state.cleanup_deals = []
                                st.rerun()

    # -------------------------------------------------------------------------
    # MAIN CONTENT
    # -------------------------------------------------------------------------
    st.header("📅 Select Dispatched Date Range")
    st.caption("Fetches orders that were **dispatched** within this date range (not created date)")

    # ---- Staleness reminder above date picker (orange/red only) ----
    staleness_label, staleness_color = get_staleness_banner()
    if staleness_color == "orange":
        st.warning(staleness_label)
    elif staleness_color == "red":
        st.error(staleness_label)

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

            # ── Step 1: Build qualifying account whitelist ────────────────
            status_container.info("🏢 **Loading qualifying accounts from Cin7...** (group CM/TP/VL + wholesale)")
            progress_bar.progress(0.05)

            whitelist = fetch_qualifying_accounts(
                cin7_user, cin7_key,
                progress_callback=lambda msg: status_container.info(f"🏢 **{msg}**")
            )
            st.session_state.whitelist = whitelist

            if whitelist:
                status_container.success(f"✅ **{len(whitelist)} qualifying accounts loaded.** Fetching orders...")
            else:
                status_container.warning("⚠️ No qualifying accounts found — check group/stage tags in Cin7.")

            progress_bar.progress(0.1)

            # ── Step 2: Fetch orders ──────────────────────────────────────
            def update_progress(phase, page, orders_so_far, message):
                fetch_state.update({"last_message": message, "orders": orders_so_far, "page": page})
                if phase == "fetching": progress = min(0.1 + (page * 0.15), 0.85)
                elif phase == "processing": progress = 0.90
                else: progress = 1.0
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
                    # Count how many pass the whitelist
                    qualifying_count = sum(1 for o in orders if is_on_whitelist(o, whitelist))
                    st.success(
                        f"✅ Fetched **{len(orders):,}** orders — "
                        f"**{qualifying_count:,}** from your {len(whitelist)} qualifying accounts"
                    )
                else:
                    st.warning("No orders found in the selected date range")
            except Exception as e:
                progress_bar.empty()
                status_container.empty()
                st.error(f"❌ Error fetching orders: {str(e)}")

    # -------------------------------------------------------------------------
    # RESULTS
    # -------------------------------------------------------------------------
    orders = st.session_state.fetched_orders
    if orders is None:
        st.caption("👆 Select a date range and click Fetch Orders")
        return

    st.caption(f"🟢 {len(orders)} orders loaded (dispatched between {st.session_state.fetch_since} and {st.session_state.fetch_until})")

    to_import, to_review, to_skip = filter_orders(
        orders, exclude_shopify, whitelist=st.session_state.whitelist
    )
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

    # Metrics
    st.header("📊 Summary")
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("Ready to Import", len(to_import), delta=f"${import_revenue:,.0f}")
    with col2: st.metric("Needs Review", len(to_review), delta=f"${review_revenue:,.0f}")
    with col3: st.metric("Retail (view only)", len(retail_orders), delta=f"${retail_revenue:,.0f}")
    with col4: st.metric("Skipped", len(other_skipped))

    st.divider()

    # Search
    st.header("🔍 Search & Filter")
    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_term = st.text_input("Search orders",
                                    placeholder="Search by Order #, Company, Customer Name, or Email...",
                                    key="order_search")
    with search_col2:
        search_field = st.selectbox("Search in",
                                    ["All Fields", "Order #", "Company", "Customer", "Email"],
                                    key="search_field")

    def matches_search(order: dict, term: str, field: str) -> bool:
        if not term: return True
        t = term.lower().strip()
        ref = (order.get('reference') or '').lower()
        co = (order.get('company') or order.get('billingCompany') or '').lower()
        em = (order.get('email') or order.get('memberEmail') or '').lower()
        cu = (order.get('customerName') or order.get('contactName') or order.get('billingName') or
              order.get('deliveryName') or order.get('memberName') or order.get('contact') or '').lower()
        if not cu:
            f = order.get('firstName') or order.get('billingFirstName') or ''
            l = order.get('lastName') or order.get('billingLastName') or ''
            cu = f"{f} {l}".strip().lower()
        if field == "Order #": return t in ref
        elif field == "Company": return t in co
        elif field == "Customer": return t in cu
        elif field == "Email": return t in em
        else: return t in ref or t in co or t in cu or t in em

    if search_term:
        to_import_f = [o for o in to_import if matches_search(o, search_term, search_field)]
        to_review_f = [o for o in to_review if matches_search(o, search_term, search_field)]
        total_matches = len(to_import_f) + len(to_review_f)
        if total_matches > 0:
            st.success(f"🔎 Found **{total_matches}** matching orders ({len(to_import_f)} ready, {len(to_review_f)} review)")
        else:
            st.warning(f"No orders found matching '{search_term}'")
        to_import = to_import_f
        to_review = to_review_f
        import_refs = {o.get('reference') for o in to_import}
        review_refs = {o.get('reference') for o in to_review}

    st.divider()

    # Line Items Preview
    with st.expander("🔍 Preview Line Items (Debug Tool)"):
        st.caption("Test fetching line items from Cin7 before syncing to HubSpot")
        all_orders_list = to_import + to_review
        if all_orders_list:
            order_options = {f"{o.get('reference')} - {o.get('company', 'Unknown')} (${o.get('total', 0):,.2f})": o
                            for o in all_orders_list}
            selected_order_name = st.selectbox("Select an order to preview:", list(order_options.keys()))
            if st.button("🔎 Fetch Line Items from Cin7", key="preview_line_items"):
                sel = order_options[selected_order_name]
                order_id = sel.get('id')
                order_ref = sel.get('reference')
                st.write(f"**Order Reference:** {order_ref}")
                st.write(f"**Order ID:** {order_id}")
                st.write(f"**Sales Rep (→ Deal Owner):** {sel.get('salesPersonEmail', '⚠️ Not assigned')}")
                st.subheader("Fields from List API (already loaded):")
                st.code(list(sel.keys()))
                for fn in ['lineItems', 'lines', 'salesOrderLines', 'orderLines', 'items', 'lineDetails']:
                    if fn in sel and sel[fn]:
                        st.success(f"✅ Line items present under '{fn}'!")
                        st.json(sel[fn][:3])
                        break
                else:
                    st.info("No line items in list API response")
                if order_id:
                    st.divider()
                    st.subheader("Fetching Detailed Order...")
                    approaches = [
                        ("Direct /SalesOrders/{id}", f"https://api.cin7.com/api/v1/SalesOrders/{order_id}", {}),
                        ("Query: id={id}", "https://api.cin7.com/api/v1/SalesOrders", {"where": f"id={order_id}"}),
                        ("Query: id='{id}'", "https://api.cin7.com/api/v1/SalesOrders", {"where": f"id='{order_id}'"}),
                        ("Query: reference='{ref}'", "https://api.cin7.com/api/v1/SalesOrders", {"where": f"reference='{order_ref}'"}),
                    ]
                    for approach_name, url, params in approaches:
                        st.write(f"**Trying:** {approach_name}")
                        try:
                            r = requests.get(url, auth=(cin7_user, cin7_key),
                                           params=params if params else None, timeout=15)
                            st.write(f"  Status: {r.status_code}")
                            if r.status_code == 200:
                                data = r.json()
                                if isinstance(data, list) and data:
                                    order_data = data[0]
                                    st.success("  ✅ Got order data!")
                                    st.write(f"  Keys: {list(order_data.keys())}")
                                    for fn in ['lineItems', 'lines', 'salesOrderLines', 'orderLines', 'items', 'lineDetails']:
                                        if fn in order_data and order_data[fn]:
                                            st.success(f"  ✅ Found line items under '{fn}': {len(order_data[fn])} items")
                                            st.json(order_data[fn][:2])
                                            break
                                    else:
                                        st.warning("  ⚠️ No line items found")
                                    break
                                else:
                                    st.write("  Response: Empty or Dict")
                            else:
                                st.write(f"  Error: {r.text[:200]}")
                        except Exception as e:
                            st.write(f"  Exception: {str(e)}")
                else:
                    st.error("❌ No order ID found")
        else:
            st.info("No orders loaded yet")

    st.divider()

    # Section 1: Ready to Import
    st.header(f"✅ Ready to Import ({len(to_import)} orders)")
    st.caption("These orders passed all filters and are pre-selected for import.")
    if to_import:
        df_import = prepare_dataframe(to_import)
        df_import.insert(0, 'Select', df_import['Order #'].apply(lambda x: x in st.session_state.selected_import))
        edited = st.data_editor(
            df_import, use_container_width=True, hide_index=True,
            column_config={
                'Select': st.column_config.CheckboxColumn('Select', default=True),
                'Total': st.column_config.NumberColumn('Total', format='$ %.2f'),
            },
            disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer',
                      'Email', 'Order Date', 'Payment', 'Deal Stage', 'Rep', 'Status'],
            key="import_editor"
        )
        st.session_state.selected_import = set(edited[edited['Select']]['Order #'].tolist())
        n = len(st.session_state.selected_import & import_refs)
        t = sum((o.get('total', 0) or 0) for o in to_import if o.get('reference') in st.session_state.selected_import)
        st.caption(f"✓ {n} of {len(to_import)} selected (${t:,.2f})")
    else:
        st.info("No orders ready to import")

    st.divider()

    # Section 2: Needs Review
    with st.expander(f"⚠️ Needs Review ({len(to_review)} orders) — Click to expand"):
        st.caption("These orders need manual review before import.")
        if to_review:
            df_review = prepare_dataframe(to_review, include_reason=True)
            df_review.insert(0, 'Select', df_review['Order #'].apply(lambda x: x in st.session_state.selected_review))
            edited_review = st.data_editor(
                df_review, use_container_width=True, hide_index=True,
                column_config={
                    'Select': st.column_config.CheckboxColumn('Select', default=False),
                    'Total': st.column_config.NumberColumn('Total', format='$ %.2f'),
                    'Reason': st.column_config.TextColumn('Reason', width='medium')
                },
                disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer',
                          'Email', 'Order Date', 'Payment', 'Deal Stage', 'Rep', 'Status', 'Reason'],
                key="review_editor"
            )
            st.session_state.selected_review = set(edited_review[edited_review['Select']]['Order #'].tolist())
            n = len(st.session_state.selected_review & review_refs)
            t = sum((o.get('total', 0) or 0) for o in to_review if o.get('reference') in st.session_state.selected_review)
            st.caption(f"✓ {n} of {len(to_review)} selected (${t:,.2f})")
        else:
            st.info("No orders need review")

    st.divider()

    # Sync section
    all_selected = (st.session_state.selected_import & import_refs) | (st.session_state.selected_review & review_refs)
    total_selected = len(all_selected)
    total_selected_revenue = sum(
        (o.get('total', 0) or 0) for o in to_import + to_review
        if o.get('reference') in all_selected
    )

    st.header("🚀 Sync to HubSpot")

    if total_selected > 0:
        selected_orders = [o for o in to_import + to_review if o.get('reference') in all_selected]
        closed_won_count = sum(1 for o in selected_orders if is_paid(o))
        pending_count = total_selected - closed_won_count

        st.success(f"**{total_selected} orders selected** — Total: ${total_selected_revenue:,.2f}")
        col1, col2 = st.columns(2)
        with col1: st.metric("Closed Won", closed_won_count, help="Fully paid orders")
        with col2: st.metric("Pending Payment", pending_count, help="Unpaid orders (Net terms)")
        if pending_count > 0:
            st.info(f"ℹ️ {pending_count} orders have outstanding balances and will sync as **Pending Payment** deals.")
        st.caption("**Full Sync:** Creates new deals, updates existing deals, syncs contacts & companies, repairs missing associations.")

        # Step 1: Dupe scan
        st.subheader("Step 1: Scan for Duplicates (Optional)")
        st.caption("Recommended for large or historical syncs. Skip for routine weekly imports.")

        if st.session_state.dupe_scan_order_set != all_selected:
            st.session_state.dupe_scan_results = None
            st.session_state.dupes_to_delete = {}

        if not hs_key:
            st.warning("Enter HubSpot API key in sidebar before scanning.")
        else:
            col_scan, col_skip = st.columns([1, 1])
            with col_scan:
                if st.button("🔍 Scan for Duplicate Deals", key="run_dupe_scan"):
                    scan_progress = st.progress(0)
                    scan_status = st.empty()
                    scan_status.info("Scanning HubSpot for duplicate deals...")
                    def scan_progress_cb(pct):
                        scan_progress.progress(pct)
                        scan_status.info(f"Scanning... {int(pct * 100)}%")
                    dupe_results = scan_for_duplicates(hs_key, list(all_selected), scan_progress_cb)
                    scan_progress.empty()
                    scan_status.empty()
                    st.session_state.dupe_scan_results = dupe_results
                    st.session_state.dupe_scan_order_set = all_selected.copy()
                    st.session_state.dupe_scan_skipped = False
                    st.session_state.dupes_to_delete = {ref: [d["id"] for d in deals[:-1]]
                                                        for ref, deals in dupe_results.items()}
            with col_skip:
                if st.button("⏩ Skip Scan & Sync Directly", key="skip_dupe_scan"):
                    st.session_state.dupe_scan_results = {}
                    st.session_state.dupe_scan_order_set = all_selected.copy()
                    st.session_state.dupe_scan_skipped = True
                    st.session_state.dupes_to_delete = {}

            if st.session_state.dupe_scan_results is not None:
                dupe_results = st.session_state.dupe_scan_results
                scan_was_skipped = st.session_state.get("dupe_scan_skipped", False)
                if scan_was_skipped:
                    st.info("⏩ Scan skipped. Proceeding directly to sync.")
                elif not dupe_results:
                    st.success("✅ No duplicates found. Safe to sync.")
                else:
                    st.warning(f"⚠️ Found **{len(dupe_results)}** order ref(s) with duplicate deals in HubSpot.")
                    st.caption("Review each group below. The newest deal will be **kept**. Older deals are pre-selected for deletion.")
                    updated_delete_map = {}
                    for ref, deals in dupe_results.items():
                        st.markdown(f"**Order Ref: `{ref}`** — {len(deals)} deals found")
                        rows = []
                        for d in deals:
                            props = d.get("properties", {})
                            rows.append({"Deal ID": d["id"], "Deal Name": props.get("dealname", ""),
                                        "Amount": f"${float(props.get('amount') or 0):,.2f}",
                                        "Close Date": (props.get("closedate") or "")[:10],
                                        "Created": (props.get("createdate") or "")[:10]})
                        df_dupes = pd.DataFrame(rows)
                        newest_id = deals[-1]["id"]
                        df_dupes["Action"] = df_dupes["Deal ID"].apply(
                            lambda x: "✅ KEEP (newest)" if x == newest_id else "🗑️ Delete")
                        st.dataframe(df_dupes, use_container_width=True, hide_index=True)
                        delete_ids_for_ref = []
                        for d in deals[:-1]:
                            props = d.get("properties", {})
                            label = f"Delete: {props.get('dealname', d['id'])} (Created {(props.get('createdate') or '')[:10]}, ${float(props.get('amount') or 0):,.2f})"
                            if st.checkbox(label, value=True, key=f"del_{d['id']}"):
                                delete_ids_for_ref.append(d["id"])
                        updated_delete_map[ref] = delete_ids_for_ref
                        st.divider()
                    st.session_state.dupes_to_delete = updated_delete_map
                    total_to_delete = sum(len(v) for v in updated_delete_map.values())
                    if total_to_delete > 0:
                        st.error(f"⚠️ **{total_to_delete} duplicate deal(s) will be deleted** before sync runs.")
                    else:
                        st.info("No deals marked for deletion. Sync will run normally.")

        # Step 2: Confirm & Sync
        st.subheader("Step 2: Confirm & Sync")
        scan_done = st.session_state.dupe_scan_results is not None
        if not scan_done:
            st.info("👆 Scan for duplicates above, or click ⏩ Skip to sync immediately.")

        confirm = st.checkbox(f"I confirm I want to sync {total_selected} orders to HubSpot",
                             key="confirm_push", disabled=not scan_done)

        if st.button(f"🔄 SYNC {total_selected} ORDERS TO HUBSPOT (${total_selected_revenue:,.0f})",
                    type="primary", use_container_width=True,
                    disabled=not confirm or not hs_key or not scan_done):
            if not hs_key:
                st.error("Enter HubSpot API key in sidebar")
            else:
                headers_hs = get_headers(hs_key)
                dupes_to_delete = st.session_state.dupes_to_delete
                total_to_delete = sum(len(v) for v in dupes_to_delete.values())

                if total_to_delete > 0:
                    delete_progress = st.progress(0)
                    delete_status = st.empty()
                    deleted_count = delete_errors = 0
                    all_delete_ids = [did for ids in dupes_to_delete.values() for did in ids]
                    for i, did in enumerate(all_delete_ids):
                        delete_status.info(f"🗑️ Deleting duplicate {i + 1}/{total_to_delete}...")
                        try:
                            r = requests.delete(f"https://api.hubapi.com/crm/v3/objects/deals/{did}",
                                              headers=headers_hs, timeout=15)
                            if r.status_code == 204: deleted_count += 1
                            else: delete_errors += 1
                        except: delete_errors += 1
                        delete_progress.progress((i + 1) / total_to_delete)
                    delete_progress.empty(); delete_status.empty()
                    if delete_errors == 0:
                        st.success(f"✅ Deleted {deleted_count} duplicate deal(s). Proceeding with sync...")
                    else:
                        st.warning(f"⚠️ Deleted {deleted_count}, {delete_errors} deletion(s) failed. Proceeding with sync...")

                status_container = st.empty()
                progress_bar = st.progress(0)
                sync_state = {"current": 0, "total": total_selected}

                def update_progress(pct):
                    sync_state["current"] = int(pct * total_selected)
                    progress_bar.progress(pct)
                    status_container.info(f"🔄 **Syncing to HubSpot...** {sync_state['current']}/{total_selected} orders ({int(pct*100)}%)")

                try:
                    results = push_orders_to_hubspot(hs_key, selected_orders, update_progress,
                                                    cin7_username=cin7_user, cin7_api_key=cin7_key)
                    progress_bar.empty(); status_container.empty()

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

                    # Reset scan state and save timestamp
                    st.session_state.dupe_scan_results = None
                    st.session_state.dupe_scan_order_set = set()
                    st.session_state.dupes_to_delete = {}
                    save_last_sync({'last_sync_at': datetime.now().isoformat()})

                    if failed_count == 0:
                        st.balloons()

                except Exception as e:
                    progress_bar.empty(); status_container.empty()
                    st.error(f"❌ Error syncing to HubSpot: {str(e)}")
    else:
        st.warning("No orders selected. Check orders above to include them in the sync.")

    st.divider()

    # Section 3: Retail
    with st.expander(f"🛍️ Retail Orders ({len(retail_orders)}) — View Only"):
        st.caption("Retail orders are shown for reference only and cannot be imported")
        if retail_orders:
            st.dataframe(prepare_dataframe(retail_orders), use_container_width=True,
                        hide_index=True, column_config=get_column_config())
        else:
            st.info("No retail orders")

    # Section 4: Skipped
    with st.expander(f"⏭️ Skipped Orders ({len(other_skipped)}) — View Only"):
        st.caption("These orders were skipped due to filters (internal orders, wrong status, etc.)")
        if other_skipped:
            st.dataframe(prepare_dataframe(other_skipped, include_reason=True),
                        use_container_width=True, hide_index=True, column_config=get_column_config())
        else:
            st.info("No skipped orders")

    # Source & Status Breakdowns
    with st.expander("📊 Source Breakdown"):
        if orders:
            source_data = {}
            for o in orders:
                key = (o.get('source') or 'Unknown', o.get('_segment', 'Unknown'))
                if key not in source_data:
                    source_data[key] = {'Source': key[0], 'Segment': key[1], 'Count': 0, 'Revenue': 0}
                source_data[key]['Count'] += 1
                source_data[key]['Revenue'] += o.get('total', 0) or 0
            df_source = pd.DataFrame(source_data.values()).sort_values('Count', ascending=False)
            st.dataframe(df_source, use_container_width=True, hide_index=True,
                        column_config={'Revenue': st.column_config.NumberColumn('Revenue', format='$ %.2f')})
        else:
            st.info("No orders loaded")

    with st.expander("📋 Status Breakdown"):
        if orders:
            status_data = {}
            for o in orders:
                key = (o.get('stage') or o.get('status') or 'Unknown', o.get('_segment', 'Unknown'))
                if key not in status_data:
                    status_data[key] = {'Status': key[0], 'Segment': key[1], 'Count': 0, 'Revenue': 0}
                status_data[key]['Count'] += 1
                status_data[key]['Revenue'] += o.get('total', 0) or 0
            df_status = pd.DataFrame(status_data.values()).sort_values('Count', ascending=False)
            st.dataframe(df_status, use_container_width=True, hide_index=True,
                        column_config={'Revenue': st.column_config.NumberColumn('Revenue', format='$ %.2f')})
            st.caption("✅ **Importable statuses**: Approved, Dispatched, Voided")
            st.caption("❌ **Skipped statuses**: Draft, Pending, New, and all others")
        else:
            st.info("No orders loaded")

    with st.expander("📖 Filter Logic Reference"):
        st.markdown("""
        **Orders are processed in this order:**

        | Step | Condition | Result |
        |------|-----------|--------|
        | 1 | Company NOT on whitelist (group CM/TP/VL + wholesale stage) | ❌ Skip |
        | 2 | Company contains "vivant" | ⚠️ Review (internal) |
        | 3 | Shopify Retail source | ❌ Skip (if enabled) |
        | 4 | Status not Approved/Dispatched/Voided | ❌ Skip |
        | 5 | No email + Dispatched | ⚠️ Review (likely employee) |
        | 6 | $0 value + Has email | ✅ Import (client sample) |
        | 7 | $0 value + No email | ⚠️ Review (likely employee) |
        | 8 | Has value + Has email | ✅ Import |

        **Whitelist** is built live on every Fetch Orders click from Cin7 Contacts:
        `isActive = True` + `group in CM, TP, VL` + `stages contains wholesale`
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
