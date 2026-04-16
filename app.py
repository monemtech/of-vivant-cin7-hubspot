"""
OrderFloz — Cin7 to HubSpot Connector
Vivant Skincare Wholesale

Architecture (lean — no whitelist scan):
  1. Fetch orders for selected date range only
  2. Extract unique companies from those orders (~20-50 names)
  3. Look up group for each company in parallel (targeted API calls)
  4. Session cache prevents re-lookups on subsequent fetches
  5. Keep orders where group is in qualifying list + status importable
  6. Preview → Sync to HubSpot
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

for k, v in [
    ('qualified_orders',  None),
    ('skipped_orders',    None),
    ('fetch_label',       ''),
    ('qualifying_groups', DEFAULT_GROUPS),
    ('group_cache',       {}),
]:
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────────────────────────────────────
# NAME HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def norm(s):
    return re.sub(r'\s+', ' ', str(s or '').strip().upper())

def strip_prefix(s):
    """Strip Cin7 branch prefix: '1 (FL) - VANITY HAUS' → 'VANITY HAUS'"""
    n = norm(s)
    return n.split(' - ', 1)[1].strip() if ' - ' in n else n


# ─────────────────────────────────────────────────────────────────────────────
# GROUP LOOKUP — targeted per company, not full scan
# ─────────────────────────────────────────────────────────────────────────────
def lookup_group(username, api_key, company_name):
    """
    Look up a single company's Cin7 group.
    Tries the exact name and the prefix-stripped name.
    Returns group string e.g. 'CM' or '' if not found.
    """
    candidates = list(dict.fromkeys([
        company_name.strip(),
        strip_prefix(company_name),
    ]))

    for name in candidates:
        if not name:
            continue
        try:
            r = requests.get(
                "https://api.cin7.com/api/v1/Contacts",
                auth=(username, api_key),
                params={"where": f"name='{name}'", "rows": 10, "isActive": "true"},
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    for c in data:
                        if not isinstance(c, dict):
                            continue
                        cin7_name = str(c.get('name') or '')
                        if (norm(cin7_name) == norm(name) or
                                strip_prefix(cin7_name) == norm(name)):
                            group = str(c.get('group') or '').strip().upper()
                            if group:
                                return group
        except Exception:
            pass
    return ''


def lookup_groups_for_companies(username, api_key, company_names, existing_cache):
    """
    Look up groups for a set of company names.
    Skips any already in the session cache.
    Returns updated cache dict.
    """
    cache     = dict(existing_cache)
    to_lookup = [n for n in company_names if norm(n) not in cache]

    if not to_lookup:
        return cache

    lock = threading.Lock()

    def fetch_one(name):
        group = lookup_group(username, api_key, name)
        with lock:
            cache[norm(name)] = group

    with ThreadPoolExecutor(max_workers=10) as ex:
        list(ex.map(fetch_one, to_lookup))

    return cache


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
    start  = since.strftime("%Y-%m-%dT00:00:00Z")
    end    = until.strftime("%Y-%m-%dT23:59:59Z")
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


def filter_orders(orders, group_cache, qualifying_groups):
    groups    = set(qualifying_groups)
    to_import = []
    to_skip   = []
    for o in orders:
        company = str(o.get('company') or o.get('billingCompany') or '').strip()
        status  = str(o.get('stage') or o.get('status') or '').lower()
        group   = group_cache.get(norm(company), '')

        if group not in groups:
            o['_skip_reason'] = (f'Group: {group}' if group
                                 else f'No group found: {company[:35]}')
            to_skip.append(o)
            continue

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
def hdr(k):
    return {"Authorization": f"Bearer {k}", "Content-Type": "application/json"}

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
    return (PAID_STAGE_ID, "Closed Won") if is_paid(o) else (UNPAID_STAGE_ID, "Pending Payment")

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
                              headers=hdr(k), json={"properties": props},
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
            res = r.json().get('results', [])
            if res: return res[0]['id']
    except: pass
    try:
        props = {x: y for x, y in {"email":email,"firstname":first,"lastname":last,
                 "company":company,"phone":phone}.items() if y}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/contacts",
                          headers=hdr(k), json={"properties": props}, timeout=15)
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
            res = r.json().get('results', [])
            if res: return res[0]['id']
    except: pass
    try:
        props = {x: y for x, y in {"name":name,"phone":phone,"address":address,
                 "city":city,"state":state,"zip":zipcode,"country":country,
                 "hubspot_owner_id":owner_id}.items() if y}
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies",
                          headers=hdr(k), json={"properties": props}, timeout=15)
        return r.json().get('id') if r.status_code == 201 else None
    except: return None

def create_deal_hs(k, order, contact_id, company_id, owner_id):
    stage_id, stage_label = get_stage(order)
    ref   = order.get('reference', '')
    co    = order.get('company') or order.get('billingCompany') or ''
    total = order.get('total', 0) or 0
    odate = order.get('orderDate') or order.get('createdDate') or ''
    props = {x: y for x, y in {
        "dealname": f"{co} - {ref}" if co else ref,
        "amount": str(total), "dealstage": stage_id, "pipeline": "default",
        "closedate": odate or None, "hubspot_owner_id": owner_id or None,
    }.items() if y}
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals",
                          headers=hdr(k), json={"properties": props}, timeout=15)
        if r.status_code == 201:
            did = r.json()['id']
            h = hdr(k)
            if contact_id:
                requests.put(f"https://api.hubapi.com/crm/v3/objects/deals/{did}/associations/contacts/{contact_id}/deal_to_contact",
                             headers=h, timeout=10)
            if company_id:
                requests.put(f"https://api.hubapi.com/crm/v3/objects/deals/{did}/associations/companies/{company_id}/deal_to_company",
                             headers=h, timeout=10)
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
            "properties": {"name": f"{name} ({sku})" if sku else name,
                           "quantity": str(qty), "price": str(price),
                           "amount": str(float(qty) * float(price))},
            "associations": [{"to": {"id": deal_id},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 20}]}]
        })
    created, errors = 0, []
    for i in range(0, len(inputs), 100):
        try:
            r = requests.post("https://api.hubapi.com/crm/v3/objects/line_items/batch/create",
                              headers=hdr(k), json={"inputs": inputs[i:i+100]}, timeout=60)
            if r.status_code in [200, 201]:
                created += len(r.json().get('results', []))
            else:
                errors.append(f"Batch {r.status_code}")
        except Exception as e:
            errors.append(str(e))
    return created, errors

def sync_one(k, order, owners, cc, co_c, lock):
    ref   = order.get('reference', '')
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
    res   = {"ref": ref, "success": False, "action": "none"}

    existing = search_deal(k, ref)
    if existing:
        did      = existing['id']
        stage_id, _ = get_stage(order)
        updates  = {}
        if existing.get('properties', {}).get('dealstage') != stage_id:
            updates['dealstage'] = stage_id
        odate = order.get('orderDate') or order.get('createdDate') or ''
        if odate and (existing.get('properties', {}).get('closedate') or '')[:10] != odate[:10]:
            updates['closedate'] = odate
        if updates:
            update_deal(k, did, updates)
            res['action'] = 'updated'
        else:
            res['action'] = 'skipped'
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
        res.update({"success": True, "action": "created"})
    else:
        res['action'] = 'failed'
    return res

def batch_sync(k, orders, owners):
    counts   = {"created": 0, "updated": 0, "skipped": 0, "failed": 0, "errors": []}
    cc, co_c = {}, {}
    lock     = threading.Lock()
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(sync_one, k, o, owners, cc, co_c, lock): o for o in orders}
        for f in as_completed(futures):
            r = f.result()
            if r['success']:
                counts[r['action']] = counts.get(r['action'], 0) + 1
            else:
                counts['failed'] += 1
                counts['errors'].append(r['ref'])
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

        # Qualifying groups
        with st.expander("🏷️ Qualifying Groups", expanded=False):
            st.caption("Orders are only imported for accounts whose Cin7 group matches one of these codes.")
            current = st.session_state.qualifying_groups

            for i, g in enumerate(current):
                c1, c2 = st.columns([3, 1])
                with c1:
                    st.text_input(f"Group {i+1}", value=g, key=f"grp_{i}",
                                  label_visibility="collapsed", disabled=True)
                with c2:
                    if st.button("✕", key=f"del_{i}", help=f"Remove {g}"):
                        st.session_state.qualifying_groups = [
                            x for j, x in enumerate(current) if j != i]
                        st.session_state.group_cache = {}
                        st.rerun()

            st.divider()
            new_g = st.text_input("New group code", placeholder="e.g. DI",
                                  key="new_grp").strip().upper()
            if st.button("➕ Add Group", use_container_width=True):
                if new_g and new_g not in st.session_state.qualifying_groups:
                    st.session_state.qualifying_groups.append(new_g)
                    st.session_state.group_cache = {}
                    st.rerun()
                elif new_g in st.session_state.qualifying_groups:
                    st.warning(f"{new_g} already exists.")

    # ── Main ──────────────────────────────────────────────────────────────────
    active_groups = st.session_state.qualifying_groups
    groups_label  = ' · '.join(active_groups)

    st.title("🌊 Vivant Skincare — Cin7 → HubSpot")
    st.caption(f"Groups: {groups_label}  ·  Paid → Closed Won  ·  Unpaid → Pending Payment")

    if not cin7_user or not cin7_key:
        st.info("👈 Enter Cin7 credentials in the sidebar.")
        return

    st.divider()
    st.subheader("📅 Select Date Range")
    st.caption("Fetches dispatched orders · checks group only for companies in that range")

    col1, col2 = st.columns(2)
    with col1: since_date = st.date_input("From", value=datetime.now() - timedelta(days=7))
    with col2: until_date = st.date_input("To",   value=datetime.now())

    if st.button("🔄 Fetch Orders", type="primary", use_container_width=True):
        since = datetime.combine(since_date, datetime.min.time())
        until = datetime.combine(until_date, datetime.max.time())

        # Step 1: fetch orders for date range
        with st.spinner("📦 Fetching orders from Cin7..."):
            all_orders = fetch_orders(cin7_user, cin7_key, since, until)

        if not all_orders:
            st.warning("No orders found in this date range.")
            st.session_state.qualified_orders = []
            st.session_state.skipped_orders   = []
            return

        # Step 2: unique companies from those orders
        companies = {
            str(o.get('company') or o.get('billingCompany') or '').strip()
            for o in all_orders
            if (o.get('company') or o.get('billingCompany') or '').strip()
        }

        # Step 3: look up group for each company
        # Session cache means already-known companies skip the API call
        cached      = st.session_state.group_cache
        new_cos     = [c for c in companies if norm(c) not in cached]
        n_cached    = len(companies) - len(new_cos)
        cache_note  = f" ({n_cached} from cache)" if n_cached else ""

        if new_cos:
            with st.spinner(f"🏢 Looking up {len(new_cos)} companies in Cin7{cache_note}..."):
                updated = lookup_groups_for_companies(
                    cin7_user, cin7_key, new_cos, cached)
                st.session_state.group_cache = updated
        else:
            st.info(f"✅ All {len(companies)} companies served from cache.")

        # Step 4: filter
        to_import, to_skip = filter_orders(
            all_orders, st.session_state.group_cache, active_groups)

        st.session_state.qualified_orders = to_import
        st.session_state.skipped_orders   = to_skip
        st.session_state.fetch_label      = f"{since_date} → {until_date}"

    # ── Preview ───────────────────────────────────────────────────────────────
    if st.session_state.qualified_orders is not None:
        to_import = st.session_state.qualified_orders
        to_skip   = st.session_state.skipped_orders or []

        st.divider()

        rev = sum(o.get('total', 0) or 0 for o in to_import)
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("✅ Ready to Import", len(to_import), f"${rev:,.0f}")
        with c2: st.metric("⏭️ Skipped",         len(to_skip))
        with c3: st.metric("📦 Total Fetched",   len(to_import) + len(to_skip))

        st.divider()
        st.subheader(f"✅ Orders Ready to Import ({len(to_import)})")

        if to_import:
            rows = [{
                'Order #':    o.get('reference', ''),
                'Company':    o.get('company') or o.get('billingCompany') or '',
                'Group':      o.get('_group', ''),
                'Order Date': (o.get('orderDate') or o.get('createdDate') or '')[:10],
                'Total':      float(o.get('total', 0) or 0),
                'Payment':    '✅ Paid' if is_paid(o) else '⏳ Unpaid',
                'Status':     o.get('stage') or o.get('status') or '',
            } for o in to_import]

            df = pd.DataFrame(rows).sort_values('Total', ascending=False)
            st.dataframe(df, use_container_width=True, hide_index=True,
                         column_config={
                             'Total': st.column_config.NumberColumn('Total', format='$ %.2f')
                         })

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
                            for e in counts['errors'][:20]:
                                st.caption(f"• {e}")
                    if counts['failed'] == 0:
                        st.balloons()
        else:
            st.info(f"No orders with group {groups_label} found in this date range.")

        if to_skip:
            with st.expander(f"⏭️ Skipped ({len(to_skip)})"):
                st.dataframe(pd.DataFrame([{
                    'Order #': o.get('reference', ''),
                    'Company': o.get('company') or o.get('billingCompany') or '',
                    'Status':  o.get('stage') or o.get('status') or '',
                    'Reason':  o.get('_skip_reason', ''),
                    'Total':   float(o.get('total', 0) or 0),
                } for o in to_skip]), use_container_width=True, hide_index=True,
                column_config={
                    'Total': st.column_config.NumberColumn('Total', format='$ %.2f')
                })


if __name__ == "__main__":
    main()
