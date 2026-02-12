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
# BRANDING CONFIGURATION
# =============================================================================
def get_branding():
    """Load branding from Streamlit secrets or use defaults."""
    defaults = {
        "company_name": "OrderFloz",
        "logo_url": "",
        "primary_color": "#1a5276",
        "accent_color": "#2ecc71",
        "support_email": "support@orderfloz.com",
        "powered_by": True
    }
    
    try:
        if hasattr(st, 'secrets') and 'branding' in st.secrets:
            branding = dict(st.secrets.branding)
            # Merge with defaults for any missing keys
            for key, value in defaults.items():
                if key not in branding:
                    branding[key] = value
            return branding
    except:
        pass
    
    return defaults

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
# HUBSPOT API
# =============================================================================
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
        'Date': (order.get('createdDate') or '')[:10],
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
    # Header with branding
    col1, col2 = st.columns([1, 5])
    with col1:
        if BRANDING.get("logo_url"):
            st.image(BRANDING["logo_url"], width=80)
        else:
            st.markdown("### 🌊")
    with col2:
        st.title(BRANDING["company_name"])
    
    st.subheader("Cin7 → HubSpot Order Sync")
    st.info("This connects to real APIs and syncs wholesale orders to HubSpot as Closed Won deals.")
    
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
        
        # Branding Settings
        with st.expander("🎨 Branding Settings"):
            st.caption("Configure in Streamlit secrets under `[branding]`")
            st.markdown(f"""
            | Setting | Current Value |
            |---------|---------------|
            | Company Name | {BRANDING['company_name']} |
            | Logo URL | {'✅ Set' if BRANDING.get('logo_url') else '❌ Not set'} |
            | Primary Color | {BRANDING['primary_color']} |
            | Support Email | {BRANDING.get('support_email', 'Not set')} |
            | Show "Powered by" | {'Yes' if BRANDING.get('powered_by', True) else 'No'} |
            """)
            st.code("""# Add to Streamlit secrets:
[branding]
company_name = "Your Company"
logo_url = "https://..."
primary_color = "#1a5276"
support_email = "support@you.com"
powered_by = true""", language="toml")
    
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
                'Total': st.column_config.NumberColumn('Total', format='$ %.2f')
            },
            disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer', 'Email', 'Date', 'Status'],
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
                    'Reason': st.column_config.TextColumn('Reason', width='medium')
                },
                disabled=['Order #', 'Source', 'Segment', 'Total', 'Company', 'Customer', 'Email', 'Date', 'Status', 'Reason'],
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
    
    st.header("🚀 Push to HubSpot")
    
    if total_selected > 0:
        st.success(f"**{total_selected} orders selected** — Total: ${total_selected_revenue:,.2f}")
        
        # Confirmation checkbox
        confirm = st.checkbox(f"I confirm I want to push {total_selected} orders to HubSpot", key="confirm_push")
        
        if st.button(
            f"🚀 PUSH {total_selected} ORDERS TO HUBSPOT (${total_selected_revenue:,.0f})",
            type="primary",
            use_container_width=True,
            disabled=not confirm
        ):
            st.warning("🎭 **DEMO MODE** — No data was written. In production, this would create deals in HubSpot.")
            st.balloons()
    else:
        st.warning("No orders selected. Check orders above to include them in the push.")
    
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
    # Footer with branding
    footer_text = f"**{BRANDING['company_name']}** — Cin7 to HubSpot order sync"
    if BRANDING.get("powered_by", True):
        footer_text += " | Powered by OrderFloz"
    if BRANDING.get("support_email"):
        footer_text += f" | {BRANDING['support_email']}"
    st.caption(footer_text)

if __name__ == "__main__":
    main()
