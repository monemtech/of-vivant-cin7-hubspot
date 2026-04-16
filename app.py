"""
OrderFloz — Cin7 to HubSpot Connector  |  Vivant Skincare Wholesale

How it works:
  1. Build group map from ALL active Cin7 contacts (cached for session).
     Cin7 doesn't support filtering contacts by name or group via API,
     so a full scan is required. Runs once per session.
  2. Pre-filter orders: skip anything with no company, or clearly internal
     (VIVANT, RETAIL, SAMPLES). No retail logic — just skip internal.
  3. Match each order's company to the group map.
     Handles both Cin7 name formats:
       Full:     "1 (GA) - AYA MEDICAL SPA GEORGIA"
       On order: "1 (GA) - AYA MEDICAL SPA GEORGIA (6%)"  ← strips pricing tier
  4. Keep only orders where group is in qualifying list.
  5. Preview → Sync to HubSpot.
"""

import streamlit as st
import pandas as pd
import requests
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

st.set_page_config(page_title="OrderFloz — Vivant", page_icon="🌊", layout="wide")

IMPORTABLE_STATUSES = {'approved', 'dispatched', 'voided'}
DEFAULT_GROUPS      = ['CM', 'TP', 'VL']
PAID_STAGE_ID       = "closedwon"
UNPAID_STAGE_ID     = "qualifiedtobuy"

# Internal account keywords — skip without group lookup
INTERNAL_KEYWORDS = ['vivant', 'retail', 'samples', 'sample']

for k, v in [
    ('qualified_orders',  None),
    ('skipped_orders',    None),
    ('fetch_label',       ''),
    ('qualifying_groups', DEFAULT_GROUPS),
    ('group_map',         None),   # {NORMALIZED_NAME: group} — built once per session
]:
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────────────────────────────────────
# NAME NORMALIZATION
# Orders store company as: "1 (GA) - AYA MEDICAL SPA GEORGIA (6%)"
# Cin7 contacts store as:  "1 (GA) - AYA MEDICAL SPA GEORGIA"
# We normalize both sides the same way before matching.
# ─────────────────────────────────────────────────────────────────────────────
def normalize(s: str) -> str:
    """Uppercase, strip whitespace, remove pricing tier suffix e.g. (6%)."""
    s = str(s or '').strip().upper()
    s = re.sub(r'\s*\(\d+\.?\d*%\)\s*$', '', s)   # strip (6%), (10.5%) etc
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def strip_branch(s: str) -> str:
    """
    Strip Cin7 branch prefix: "1 (GA) - AYA MEDICAL SPA GEORGIA" → "AYA MEDICAL SPA GEORGIA"
    Returns the part after the first ' - ', or the original if no ' - '.
    """
    n = normalize(s)
    return n.split(' - ', 1)[1].strip() if ' - ' in n else n


def is_internal(company: str) -> bool:
    """Return True if company name looks internal/retail."""
    low = (company or '').lower()
    return any(kw in low for kw in INTERNAL_KEYWORDS)


# ─────────────────────────────────────────────────────────────────────────────
# GROUP MAP — full Cin7 contact scan, cached for session
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def build_group_map(username: str, api_key: str, qualifying_groups: tuple) -> dict:
    """
    Scan ALL active Cin7 contacts. Build a lookup dict:
      { NORMALIZED_NAME: group_code }
    Indexed by BOTH the full normalized name AND the branch-stripped name
    so matching works regardless of order format.
    Only includes contacts whose group is in qualifying_groups.
    Cached for 24 hours.
    """
    groups  = set(qualifying_groups)
    gmap    = {}
    page    = 1

    while True:
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
                group = str(c.get('group') or '').strip().upper()
                if group not in groups:
                    continue
                raw = str(c.get('name') or '').strip()
                if not raw:
                    continue
                full_key     = normalize(raw)
                stripped_key = strip_branch(raw)
                gmap[full_key]     = group
                gmap[stripped_key] = group

            if len(batch) < 250:
                break
            page += 1

        except Exception:
            break

    return gmap


def get_order_group(order: dict, gmap: dict) -> str:
    """Match order company to group map. Tries full name and branch-stripped name."""
    raw = str(order.get('company') or order.get('billingCompany') or '').strip()
    if not raw:
        return ''
    return gmap.get(normalize(raw)) or gmap.get(strip_branch(raw)) or ''


# ─────────────────────────────────────────────────────────────────────────────
# CIN7 — ORDERS
# ─────────────────────────────────────────────────────────────────────────────
def test_cin7(u, k):
    try:
        r = requests.get("https://api.cin7.com/api/v1/SalesOrders",
                         auth=(u, k), params={"rows": 1}, timeout=15)
        return (True, "Connected") if r.status_code == 200 else (False, f"Error {r.status_code}")
    except Exception as e:
        return False, str(e)


def fetch_orders(u, k, since, until):
    start = since.strftime("%Y-%m-%dT00:00:00Z")
    end   = until.strftime("%Y-%m-%dT23:59:59Z")
    orders, page = [], 1
    while True:
        r = requests.get(
            "https://api.cin7.com/api/v1/SalesOrders", auth=(u, k),
            params={"where": f"dispatchedDate >= '{start}' AND dispatchedDate <= '{end}'",
                    "page": page, "rows": 250}, timeout=60)
        if r.status_code != 200: break
        batch = r.json()
        if not batch: break
        orders.extend(batch)
        if len(batch) < 250: break
        page += 1
    return orders


def filter_orders(orders, gmap, qualifying_groups):
    groups    = set(qualifying_groups)
    to_import = []
    to_skip   = []

    for o in orders:
        company = str(o.get('company') or o.get('billingCompany') or '').strip()
        status  = str(o.get('stage') or o.get('status') or '').lower()

        # Skip internal/retail immediately — no group lookup needed
        if is_internal(company) or not company:
            o['_skip_reason'] = 'Internal/retail'
            to_skip.append(o)
            continue

        # Check group
        group = get_order_group(o, gmap)
        if group not in groups:
            o['_skip_reason'] = f'Group: {group or "none"}'
            to_skip.append(o)
            continue

        # Check status
        if status not in IMPORTABLE_STATUSES:
            o['_skip_reason'] = f'Status: {status}'
            to_skip.append(o)
            continue

        o['_group'] = group
        to_import.append(o)

    return to_import, to_skip


# ─────────────────────────────────────────────────────────────────────────────
# HUBSPOT
# ─────────────────────────────────────────────────────────────────────────────
def hdr(k): return {"Authorization": f"Bearer {k}", "Content-Type": "application/json"}

def test_hubspot(k):
    try:
        r = requests.get("https://api.hubapi.com/crm/v3/objects/contacts",
                         headers=hdr(k), params={"limit": 1}, timeout=15)
        return (True, "Connected") if r.status_code == 200 else (False, f"Error {r.status_code}")
    except Exception as e:
        return False, str(e)

def is_paid(o):
    paid  = str(o.get('paid') or '').lower()
    owing = float(o.get('totalOwing') or 0)
    terms = str(o.get('paymentTerms') or '').lower()
    if any(t in terms for t in ['net 30','net 60','net 90','net30','net60','net90']) and owing > 0:
        return False
    return '100%' in paid or owing == 0

def get_stage(o):
    return (PAID_STAGE_ID,"Closed Won") if is_paid(o) else (UNPAID_STAGE_ID,"Pending Payment")

def get_owners(k):
    try:
        r = requests.get("https://api.hubapi.com/crm/v3/owners",
                         headers=hdr(k), params={"limit": 100}, timeout=15)
        if r.status_code == 200:
            return {(o.get('email') or '').lower(): o.get('id')
                    for o in r.json().get('results', []) if o.get('email')}
    except: pass
    return {}

def search_deal(k, ref):
    try:
        body = {"filterGroups":[{"filters":[{"propertyName":"dealname",
                "operator":"CONTAINS_TOKEN","value":ref}]}],
                "properties":["dealname","dealstage","amount","closedate"],"limit":10}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search",
                          headers=hdr(k), json=body, timeout=15)
        if r.status_code == 200:
            for d in r.json().get('results', []):
                if ref in (d.get('properties',{}).get('dealname') or ''):
                    return d
    except: pass
    return None

def update_deal(k, did, props):
    try:
        return requests.patch(f"https://api.hubapi.com/crm/v3/objects/deals/{did}",
                              headers=hdr(k), json={"properties":props},
                              timeout=15).status_code == 200
    except: return False

def search_or_create_contact(k, email, first, last, company, phone):
    if not email: return None
    try:
        body = {"filterGroups":[{"filters":[{"propertyName":"email","operator":"EQ","value":email}]}],
                "properties":["email"],"limit":1}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/search",
                          headers=hdr(k), json=body, timeout=15)
        if r.status_code == 200:
            res = r.json().get('results',[])
            if res: return res[0]['id']
    except: pass
    try:
        props = {x:y for x,y in {"email":email,"firstname":first,"lastname":last,
                 "company":company,"phone":phone}.items() if y}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts",
                          headers=hdr(k), json={"properties":props}, timeout=15)
        return r.json().get('id') if r.status_code == 201 else None
    except: return None

def search_or_create_company(k, name, phone, address, city, state, zipcode, country, owner_id):
    if not name: return None
    try:
        body = {"filterGroups":[{"filters":[{"propertyName":"name","operator":"EQ","value":name}]}],
                "properties":["name"],"limit":1}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies/search",
                          headers=hdr(k), json=body, timeout=15)
        if r.status_code == 200:
            res = r.json().get('results',[])
            if res: return res[0]['id']
    except: pass
    try:
        props = {x:y for x,y in {"name":name,"phone":phone,"address":address,
                 "city":city,"state":state,"zip":zipcode,"country":country,
                 "hubspot_owner_id":owner_id}.items() if y}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies",
                          headers=hdr(k), json={"properties":props}, timeout=15)
        return r.json().get('id') if r.status_code == 201 else None
    except: return None

def create_deal_hs(k, order, contact_id, company_id, owner_id):
    stage_id, stage_label = get_stage(order)
    ref   = order.get('reference','')
    co    = order.get('company') or order.get('billingCompany') or ''
    total = order.get('total',0) or 0
    odate = order.get('orderDate') or order.get('createdDate') or ''
    props = {x:y for x,y in {
        "dealname": f"{co} - {ref}" if co else ref,
        "amount": str(total), "dealstage": stage_id, "pipeline": "default",
        "closedate": odate or None, "hubspot_owner_id": owner_id or None,
    }.items() if y}
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals",
                          headers=hdr(k), json={"properties":props}, timeout=15)
        if r.status_code == 201:
            did = r.json()['id']
            h = hdr(k)
            if contact_id:
                requests.put(f"https://api.hubapi.com/crm/v3/objects/deals/{did}/associations/contacts/{contact_id}/deal_to_contact", headers=h, timeout=10)
            if company_id:
                requests.put(f"https://api.hubapi.com/crm/v3/objects/deals/{did}/associations/companies/{company_id}/deal_to_company", headers=h, timeout=10)
            return did, stage_label
        return None, f"Error {r.status_code}"
    except Exception as e:
        return None, str(e)

def create_line_items(k, deal_id, order):
    items = None
    for f in ['lineItems','lines','salesOrderLines','orderLines','items']:
        if order.get(f): items = order[f]; break
    if not items: return 0, []
    inputs = []
    for i in items:
        name  = i.get('name') or i.get('productName') or i.get('description') or 'Product'
        sku   = i.get('code') or i.get('sku') or ''
        qty   = i.get('qty') or i.get('quantity') or 1
        price = i.get('unitPrice') or i.get('price') or 0
        inputs.append({
            "properties":{"name":f"{name} ({sku})" if sku else name,
                          "quantity":str(qty),"price":str(price),
                          "amount":str(float(qty)*float(price))},
            "associations":[{"to":{"id":deal_id},
                "types":[{"associationCategory":"HUBSPOT_DEFINED","associationTypeId":20}]}]
        })
    created, errors = 0, []
    for i in range(0, len(inputs), 100):
        try:
            r = requests.post("https://api.hubapi.com/crm/v3/objects/line_items/batch/create",
                              headers=hdr(k), json={"inputs":inputs[i:i+100]}, timeout=60)
            if r.status_code in [200,201]: created += len(r.json().get('results',[]))
            else: errors.append(f"Batch {r.status_code}")
        except Exception as e: errors.append(str(e))
    return created, errors

def sync_one(k, order, owners, cc, co_c, lock):
    ref   = order.get('reference','')
    email = order.get('email') or order.get('memberEmail') or ''
    first = order.get('firstName') or order.get('billingFirstName') or ''
    last  = order.get('lastName') or order.get('billingLastName') or ''
    co    = order.get('company') or order.get('billingCompany') or ''
    phone = order.get('phone') or order.get('billingPhone') or ''
    addr  = order.get('billingAddress1') or ''
    city  = order.get('billingCity') or ''
    state = order.get('billingState') or ''
    zipc  = order.get('billingPostCode') or ''
    ctry  = order.get('billingCountry') or ''
    rep   = (order.get('salesPersonEmail') or '').lower()
    owner = owners.get(rep) if rep else None
    res   = {"ref":ref,"success":False,"action":"none"}

    existing = search_deal(k, ref)
    if existing:
        did = existing['id']
        stage_id, _ = get_stage(order)
        updates = {}
        if existing.get('properties',{}).get('dealstage') != stage_id:
            updates['dealstage'] = stage_id
        odate = order.get('orderDate') or order.get('createdDate') or ''
        if odate and (existing.get('properties',{}).get('closedate') or '')[:10] != odate[:10]:
            updates['closedate'] = odate
        if updates: update_deal(k, did, updates); res['action'] = 'updated'
        else: res['action'] = 'skipped'
        res['success'] = True
        return res

    with lock: cid = cc.get(email.lower())
    if not cid:
        cid = search_or_create_contact(k, email, first, last, co, phone)
        if cid:
            with lock: cc[email.lower()] = cid

    with lock: company_id = co_c.get(co.lower())
    if not company_id:
        company_id = search_or_create_company(k, co, phone, addr, city, state, zipc, ctry, owner)
        if company_id:
            with lock: co_c[co.lower()] = company_id

    did, stage_or_err = create_deal_hs(k, order, cid, company_id, owner)
    if did:
        create_line_items(k, did, order)
        res.update({"success":True,"action":"created"})
    else:
        res['action'] = 'failed'
    return res

def batch_sync(k, orders, owners):
    counts = {"created":0,"updated":0,"skipped":0,"failed":0,"errors":[]}
    cc, co_c, lock = {}, {}, threading.Lock()
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(sync_one, k, o, owners, cc, co_c, lock): o for o in orders}
        for f in as_completed(futures):
            r = f.result()
            if r['success']: counts[r['action']] = counts.get(r['action'],0) + 1
            else: counts['failed'] += 1; counts['errors'].append(r['ref'])
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────────────
def main():
    with st.sidebar:
        st.title("⚙️ Connections")

        st.subheader("Cin7")
        cin7_user = st.text_input("Username", key="cin7_user")
        cin7_key  = st.text_input("API Key", type="password", key="cin7_key")
        if st.button("Test Cin7"):
            ok, msg = test_cin7(cin7_user, cin7_key)
            st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")

        st.divider()

        st.subheader("HubSpot")
        hs_key = st.text_input("Private App Token", type="password", key="hs_key")
        if st.button("Test HubSpot"):
            ok, msg = test_hubspot(hs_key)
            st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")

        st.divider()

        # Group map status
        gm = st.session_state.group_map
        if gm:
            unique = len(gm) // 2
            q = set(st.session_state.qualifying_groups)
            st.success(f"✅ {unique} accounts loaded")
            for g in sorted(q):
                c = sum(1 for k, v in gm.items() if v == g and ' - ' not in k)
                if c: st.caption(f"  {g}: {c}")
            if st.button("🔄 Reload", help="Re-fetch accounts from Cin7"):
                build_group_map.clear()
                st.session_state.group_map = None
                st.rerun()
        else:
            st.caption("Accounts not loaded yet")

        st.divider()

        # Qualifying groups
        with st.expander("🏷️ Qualifying Groups", expanded=False):
            st.caption("Only accounts with these Cin7 group codes will be imported.")
            current = st.session_state.qualifying_groups
            for i, g in enumerate(current):
                c1, c2 = st.columns([3,1])
                with c1:
                    st.code(g)
                with c2:
                    if st.button("✕", key=f"del_{i}", help=f"Remove {g}"):
                        st.session_state.qualifying_groups = [x for j,x in enumerate(current) if j!=i]
                        build_group_map.clear()
                        st.session_state.group_map = None
                        st.rerun()
            new_g = st.text_input("Add group code", placeholder="e.g. DI", key="new_grp").strip().upper()
            if st.button("➕ Add", use_container_width=True):
                if new_g and new_g not in st.session_state.qualifying_groups:
                    st.session_state.qualifying_groups.append(new_g)
                    build_group_map.clear()
                    st.session_state.group_map = None
                    st.rerun()
                elif new_g in st.session_state.qualifying_groups:
                    st.warning(f"{new_g} already in list")

    # ── Main ──────────────────────────────────────────────────────────────────
    active_groups = st.session_state.qualifying_groups
    groups_label  = ' · '.join(active_groups)

    st.title("🌊 Vivant Skincare — Cin7 → HubSpot")
    st.caption(f"Groups: {groups_label}  ·  Paid → Closed Won  ·  Unpaid → Pending Payment")

    if not cin7_user or not cin7_key:
        st.info("👈 Enter Cin7 credentials in the sidebar.")
        return

    # ── Load group map if needed ──────────────────────────────────────────────
    if not st.session_state.group_map:
        st.divider()
        st.info(f"⏳ Load qualifying accounts first. This scans Cin7 once and caches for 24 hours.")
        if st.button("▶️ Load Accounts", type="primary", use_container_width=True):
            with st.spinner(f"Scanning Cin7 contacts for groups {groups_label}..."):
                gm = build_group_map(cin7_user, cin7_key, tuple(active_groups))
            if gm:
                st.session_state.group_map = gm
                st.rerun()
            else:
                st.error("❌ Could not load accounts. Check Cin7 credentials.")
        return

    st.divider()
    st.subheader("📅 Select Date Range")
    st.caption("Fetches dispatched orders · matches company to pre-loaded group map")

    col1, col2 = st.columns(2)
    with col1: since_date = st.date_input("From", value=datetime.now() - timedelta(days=7))
    with col2: until_date = st.date_input("To",   value=datetime.now())

    if st.button("🔄 Fetch Orders", type="primary", use_container_width=True):
        since = datetime.combine(since_date, datetime.min.time())
        until = datetime.combine(until_date, datetime.max.time())

        with st.spinner("📦 Fetching orders from Cin7..."):
            all_orders = fetch_orders(cin7_user, cin7_key, since, until)

        if not all_orders:
            st.warning("No orders found in this date range.")
            st.session_state.qualified_orders = []
            st.session_state.skipped_orders   = []
            return

        to_import, to_skip = filter_orders(
            all_orders, st.session_state.group_map, active_groups)

        st.session_state.qualified_orders = to_import
        st.session_state.skipped_orders   = to_skip
        st.session_state.fetch_label      = f"{since_date} → {until_date}"

    # ── Preview ───────────────────────────────────────────────────────────────
    if st.session_state.qualified_orders is not None:
        to_import = st.session_state.qualified_orders
        to_skip   = st.session_state.skipped_orders or []

        st.divider()

        rev = sum(o.get('total',0) or 0 for o in to_import)
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("✅ Ready to Import", len(to_import), f"${rev:,.0f}")
        with c2: st.metric("⏭️ Skipped",         len(to_skip))
        with c3: st.metric("📦 Total Fetched",   len(to_import) + len(to_skip))

        st.divider()
        st.subheader(f"✅ Orders Ready to Import ({len(to_import)})")

        if to_import:
            rows = [{
                'Order #':    o.get('reference',''),
                'Company':    o.get('company') or o.get('billingCompany') or '',
                'Group':      o.get('_group',''),
                'Order Date': (o.get('orderDate') or o.get('createdDate') or '')[:10],
                'Total':      float(o.get('total',0) or 0),
                'Payment':    '✅ Paid' if is_paid(o) else '⏳ Unpaid',
                'Status':     o.get('stage') or o.get('status') or '',
            } for o in to_import]

            df = pd.DataFrame(rows).sort_values('Total', ascending=False)
            st.dataframe(df, use_container_width=True, hide_index=True,
                         column_config={'Total': st.column_config.NumberColumn('Total', format='$ %.2f')})

            paid   = sum(1 for o in to_import if is_paid(o))
            unpaid = len(to_import) - paid
            st.caption(f"Closed Won: **{paid}** · Pending Payment: **{unpaid}**")

            st.divider()
            st.subheader("🚀 Sync to HubSpot")

            if not hs_key:
                st.warning("Enter HubSpot Private App Token in the sidebar.")
            else:
                st.info(f"Will sync **{len(to_import)} orders** → deals, contacts, companies, line items.")
                if st.button("▶️ Start Sync", type="primary", use_container_width=True):
                    with st.spinner("Loading HubSpot owner list..."):
                        owners = get_owners(hs_key)
                    with st.spinner(f"Syncing {len(to_import)} orders..."):
                        counts = batch_sync(hs_key, to_import, owners)
                    st.success(
                        f"✅ Done — **{counts['created']}** created · "
                        f"**{counts['updated']}** updated · "
                        f"**{counts['skipped']}** unchanged · "
                        f"**{counts['failed']}** failed"
                    )
                    if counts['errors']:
                        with st.expander(f"❌ {len(counts['errors'])} errors"):
                            for e in counts['errors'][:20]: st.caption(f"• {e}")
                    if counts['failed'] == 0:
                        st.balloons()
        else:
            st.info(f"No qualifying orders (groups: {groups_label}) found in this date range.")

        if to_skip:
            with st.expander(f"⏭️ Skipped ({len(to_skip)})"):
                st.dataframe(pd.DataFrame([{
                    'Order #': o.get('reference',''),
                    'Company': o.get('company') or o.get('billingCompany') or '',
                    'Status':  o.get('stage') or o.get('status') or '',
                    'Reason':  o.get('_skip_reason',''),
                    'Total':   float(o.get('total',0) or 0),
                } for o in to_skip]), use_container_width=True, hide_index=True,
                column_config={'Total': st.column_config.NumberColumn('Total', format='$ %.2f')})


if __name__ == "__main__":
    main()
