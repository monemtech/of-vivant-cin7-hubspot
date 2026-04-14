"""
app.py — Vivant BI Wholesale Sales Dashboard
Reads from local SQLite. Zero API calls on filter/date changes.
Only syncs from Cin7 when user clicks "Sync Data".
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from database import (
    init_db, total_orders, last_sync_info, get_filter_options, get_conn,
    summary_kpis, revenue_by_period, revenue_by_rep, revenue_by_account,
    revenue_by_tier, rep_comparison, account_comparison, top_products,
    get_account_statuses, rep_status_summary,
    query_orders,
)

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Vivant BI · Wholesale",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

BLUE  = "#1B3A6B"
GOLD  = "#C9A84C"
GREEN = "#27AE60"
RED   = "#E74C3C"
GRAY  = "#7F8C8D"
LGRAY = "#CBD5E0"

st.markdown(f"""<style>
[data-testid="stAppViewContainer"] {{ background: #F7F9FC; }}
[data-testid="stSidebar"] {{ background: {BLUE}; }}
[data-testid="stSidebar"] * {{ color: white !important; }}
[data-testid="stSidebar"] label {{ color: #8EA8CC !important; font-size:.75rem; text-transform:uppercase; letter-spacing:.06em; }}
.kpi {{ background:white; border-radius:10px; padding:18px 22px; border-left:4px solid {BLUE}; box-shadow:0 2px 8px rgba(0,0,0,.06); }}
.kpi-label {{ font-size:.72rem; text-transform:uppercase; letter-spacing:.08em; color:{GRAY}; margin-bottom:4px; }}
.kpi-value {{ font-size:1.85rem; font-weight:800; color:{BLUE}; line-height:1.1; }}
.kpi-delta-pos {{ color:{GREEN}; font-size:.82rem; font-weight:600; }}
.kpi-delta-neg {{ color:{RED};   font-size:.82rem; font-weight:600; }}
.kpi-delta-neu {{ color:{GRAY};  font-size:.82rem; font-weight:600; }}
.sec {{ font-size:1rem; font-weight:700; color:{BLUE}; border-bottom:2px solid {GOLD}; padding-bottom:5px; margin-bottom:14px; }}
</style>""", unsafe_allow_html=True)

init_db()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def fc(v):
    if abs(v) >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if abs(v) >= 1_000:     return f"${v:,.2f}"
    return f"${v:.2f}"

def fp(v):
    return f"+{abs(v):.1f}%" if v is not None and not np.isnan(float(v)) and v >= 0 else (f"-{abs(v):.1f}%" if v is not None and not np.isnan(float(v)) else "—")

def delta_html(d, p, dollar=True):
    arrow = "▲" if d >= 0 else "▼"
    cls   = "kpi-delta-pos" if d >= 0 else "kpi-delta-neg"
    dstr  = fc(abs(d)) if dollar else f"{abs(d):.0f}"
    return f'<span class="{cls}">{arrow} {dstr} ({fp(p)})</span>'

def kpi(label, value, dhtml=""):
    return f'<div class="kpi"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>{"<div style=margin-top:5px>"+dhtml+"</div>" if dhtml else ""}</div>'

def sec(t):
    st.markdown(f'<div class="sec">{t}</div>', unsafe_allow_html=True)

def chart_layout(fig, height=300):
    fig.update_layout(
        height=height, plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", y=1.12),
    )
    return fig

def color_map(val):
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if val > 0: return f"color: {GREEN};"
        if val < 0: return f"color: {RED};"
    return ""

def style_df(df, dollar_cols=[], pct_cols=[], delta_cols=[]):
    fmt = {}
    for c in dollar_cols: fmt[c] = "${:,.2f}"
    for c in pct_cols:    fmt[c] = "{:+.1f}%"
    for c in delta_cols:  fmt[c] = "+${:,.2f}"
    s = df.style.format(fmt, na_rep="—")
    all_delta = delta_cols + pct_cols
    if all_delta:
        s = s.map(color_map, subset=[c for c in all_delta if c in df.columns])
    return s

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="text-align:center;padding:18px 0 24px">
      <div style="font-size:1.45rem;font-weight:900;color:white;letter-spacing:-.02em">
        VIVANT<span style="color:{GOLD}">·</span>BI
      </div>
      <div style="font-size:.68rem;color:#8EA8CC;letter-spacing:.15em;text-transform:uppercase;margin-top:3px">
        Wholesale Intelligence
      </div>
    </div>""", unsafe_allow_html=True)

    PAGE = st.radio("Nav", [
        "📊 Overview", "🏢 Accounts", "👤 Rep Performance",
    ], label_visibility="collapsed")

    st.markdown("---")

    # ── Date pickers ──
    st.markdown('<div style="font-size:.7rem;color:#8EA8CC;text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px">Current Period</div>', unsafe_allow_html=True)
    today = date.today()

    PRESET = st.selectbox("Period Preset", [
        "This Week", "Last Week", "This Month", "Last Month",
        "This Quarter", "Last Quarter", "YTD", "Last Year", "Custom Range",
    ], index=6, label_visibility="collapsed")

    def start_of_week(d):  return d - timedelta(days=d.weekday())
    def start_of_quarter(d): return date(d.year, ((d.month-1)//3)*3+1, 1)

    if PRESET == "This Week":
        D_FROM, D_TO = start_of_week(today), today
    elif PRESET == "Last Week":
        D_FROM = start_of_week(today) - timedelta(weeks=1)
        D_TO   = start_of_week(today) - timedelta(days=1)
    elif PRESET == "This Month":
        D_FROM, D_TO = today.replace(day=1), today
    elif PRESET == "Last Month":
        D_TO   = today.replace(day=1) - timedelta(days=1)
        D_FROM = D_TO.replace(day=1)
    elif PRESET == "This Quarter":
        D_FROM, D_TO = start_of_quarter(today), today
    elif PRESET == "Last Quarter":
        D_TO   = start_of_quarter(today) - timedelta(days=1)
        D_FROM = start_of_quarter(D_TO)
    elif PRESET == "YTD":
        D_FROM, D_TO = today.replace(month=1, day=1), today
    elif PRESET == "Last Year":
        D_FROM = date(today.year-1, 1, 1)
        D_TO   = date(today.year-1, 12, 31)
    else:
        D_FROM = st.date_input("From", value=today.replace(month=1, day=1))
        D_TO   = st.date_input("To",   value=today)

    st.caption(f"📅 {D_FROM:%b %d} → {D_TO:%b %d, %Y}")

    st.markdown('<div style="font-size:.7rem;color:#8EA8CC;text-transform:uppercase;letter-spacing:.1em;margin:10px 0 6px">Compare To</div>', unsafe_allow_html=True)
    COMPARE = st.selectbox("Compare", [
        "Previous Period", "Same Period Last Year", "Custom Range", "None",
    ], label_visibility="collapsed")

    span = (D_TO - D_FROM).days
    if COMPARE == "Previous Period":
        P_TO   = D_FROM - timedelta(days=1)
        P_FROM = P_TO - timedelta(days=span)
    elif COMPARE == "Same Period Last Year":
        P_FROM = date(D_FROM.year-1, D_FROM.month, D_FROM.day)
        P_TO   = date(D_TO.year-1,   D_TO.month,   D_TO.day)
    elif COMPARE == "Custom Range":
        st.markdown('<div style="font-size:.7rem;color:#8EA8CC;margin-bottom:4px">Compare period:</div>', unsafe_allow_html=True)
        P_FROM = st.date_input("From", value=D_FROM - timedelta(days=365), key="cmp_from")
        P_TO   = st.date_input("To",   value=D_TO   - timedelta(days=365), key="cmp_to")
    else:
        P_FROM = P_TO = None

    if P_FROM and COMPARE != "None":
        st.caption(f"📅 {P_FROM:%b %d} → {P_TO:%b %d, %Y}")

    st.markdown("---")

    # ── Filters ──
    opts = get_filter_options() if total_orders() > 0 else {"reps":[],"tiers":[],"stages":[]}
    st.markdown('<div style="font-size:.7rem;color:#8EA8CC;text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px">Filters</div>', unsafe_allow_html=True)
    F_REPS   = st.multiselect("Sales Rep",   opts["reps"],   placeholder="All reps")
    F_TIERS  = st.multiselect("Tier",        opts["tiers"],  placeholder="All tiers")
    F_STAGES = []

    st.markdown("---")

    # ── Sync ──
    sync_info = last_sync_info()
    n_orders  = total_orders()
    if sync_info:
        st.caption(f"Last sync: {sync_info.get('synced_at','')[:16]}")
        st.caption(f"{n_orders:,} orders in database")
    else:
        st.caption("No data synced yet")

    if st.button("🔄 Sync Data from Cin7", width='stretch'):
        st.session_state["do_sync"] = True

# ─────────────────────────────────────────────
# SMART SYNC CHECK — runs once on launch
# ─────────────────────────────────────────────

def get_sync_status() -> dict:
    """Check how stale the database is and estimate pending updates."""
    import sqlite3
    from pathlib import Path
    db = Path(__file__).parent / "vivant_orders.db"
    if not db.exists():
        return {"has_data": False}
    conn = sqlite3.connect(db)
    info = last_sync_info()
    latest = conn.execute("SELECT MAX(order_date) FROM orders WHERE source='Backend'").fetchone()[0]
    conn.close()
    if not info or not latest:
        return {"has_data": False}
    from datetime import date
    last_date = date.fromisoformat(latest)
    days_behind = (date.today() - last_date).days
    synced_at = info.get("synced_at", "")[:16]
    return {
        "has_data": True,
        "days_behind": days_behind,
        "latest_order": latest,
        "synced_at": synced_at,
    }

# Show smart sync banner if data is stale
if "sync_checked" not in st.session_state:
    st.session_state["sync_checked"] = True
    status = get_sync_status()
    if status["has_data"] and status["days_behind"] >= 1:
        st.session_state["sync_banner"] = status

if st.session_state.get("sync_banner"):
    status = st.session_state["sync_banner"]
    days = status["days_behind"]
    col_msg, col_btn = st.columns([5, 1])
    with col_msg:
        st.info(f"📬 **{days} day{'s' if days != 1 else ''} of new orders missing** — your report is current as of **{status['latest_order']}**")
    with col_btn:
        if st.button("🔄 Update Data", type="primary", use_container_width=True):
            st.session_state["sync_banner"] = None
            st.session_state["run_background_sync"] = True
            st.rerun()

# ─────────────────────────────────────────────
# BACKGROUND SYNC HANDLER
# ─────────────────────────────────────────────
if st.session_state.get("run_background_sync"):
    st.session_state["run_background_sync"] = False
    import subprocess, sys
    from pathlib import Path
    sync_script = Path(__file__).parent / "run_sync.py"
    with st.spinner("🔄 Updating data… this takes a few minutes. You can keep using the app."):
        try:
            result = subprocess.run(
                [sys.executable, str(sync_script)],
                capture_output=True, text=True, timeout=600,
                encoding="utf-8", errors="replace",
                env={**__import__("os").environ, "PYTHONIOENCODING": "utf-8"}
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")
                summary = next((l for l in reversed(lines) if "saved" in l or "✅" in l), "Sync complete.")
                st.success(f"✅ Data updated! {summary}")
                st.rerun()
            else:
                st.error(f"Sync issue: {result.stderr[-300:] if result.stderr else 'Unknown error'}")
        except subprocess.TimeoutExpired:
            st.warning("Sync is taking longer than expected — refresh in a few minutes.")
        except Exception as e:
            st.error(f"Could not run sync: {e}")

# ─────────────────────────────────────────────
# SIDEBAR SYNC BUTTON HANDLER
# ─────────────────────────────────────────────
if st.session_state.get("do_sync"):
    st.session_state["do_sync"] = False
    import subprocess, sys
    from pathlib import Path
    sync_script = Path(__file__).parent / "run_sync.py"
    with st.spinner("🔄 Syncing data from Cin7… this takes a few minutes."):
        try:
            result = subprocess.run(
                [sys.executable, str(sync_script)],
                capture_output=True, text=True, timeout=600,
                encoding="utf-8", errors="replace",
                env={**__import__("os").environ, "PYTHONIOENCODING": "utf-8"}
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")
                summary = next((l for l in reversed(lines) if "saved" in l or "✅" in l), "Sync complete.")
                st.success(f"✅ Data updated! {summary}")
                st.rerun()
            else:
                st.error(f"Sync issue: {result.stderr[-300:] if result.stderr else 'Unknown error'}")
        except subprocess.TimeoutExpired:
            st.warning("Sync is taking longer than expected — refresh in a few minutes.")
        except Exception as e:
            st.error(f"Could not run sync: {e}")

# ─────────────────────────────────────────────
# NO DATA STATE
# ─────────────────────────────────────────────
if total_orders() == 0:
    st.markdown(f"""
    <div style="text-align:center;padding:80px 40px">
      <div style="font-size:3rem">📭</div>
      <h2 style="color:{BLUE}">No data yet</h2>
      <p style="color:{GRAY}">Click <b>🔄 Sync Data from Cin7</b> in the sidebar to load your orders.<br>
      This only needs to happen once — after that, all filtering is instant.</p>
    </div>""", unsafe_allow_html=True)
    st.stop()

# ─────────────────────────────────────────────
# FILTER ARGS (passed to every query)
# ─────────────────────────────────────────────
fargs = dict(
    reps   = F_REPS   or None,
    tiers  = F_TIERS  or None,
    stages = F_STAGES or None,
)

cur_label = f"{D_FROM:%b %d} – {D_TO:%b %d, %Y}"
pri_label = f"{P_FROM:%b %d} – {P_TO:%b %d, %Y}" if P_FROM else "No comparison"

# ─────────────────────────────────────────────
# OVERVIEW PAGE
# ─────────────────────────────────────────────
if PAGE == "📊 Overview":
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;padding:12px 0 18px">
      <h1 style="font-size:1.5rem;font-weight:800;color:{BLUE};margin:0">Sales Overview</h1>
      <span style="background:{GOLD};color:white;font-size:.68rem;font-weight:700;
             padding:3px 10px;border-radius:20px;text-transform:uppercase;letter-spacing:.08em">
        {cur_label}
      </span>
    </div>""", unsafe_allow_html=True)

    cur = summary_kpis(D_FROM, D_TO, **fargs)
    pri = summary_kpis(P_FROM, P_TO, **fargs) if P_FROM else None

    def _delta(k):
        if not pri: return ""
        d = cur[k] - pri[k]
        p = (d/pri[k]*100) if pri[k] else (100 if cur[k] else 0)
        return delta_html(d, p, dollar=(k in ["revenue","aov"]))

    cols = st.columns(4)
    cols[0].markdown(kpi("Revenue",      fc(cur["revenue"]),       _delta("revenue")),  unsafe_allow_html=True)
    cols[1].markdown(kpi("Orders",       f'{cur["orders"]:,}',     _delta("orders")),   unsafe_allow_html=True)
    cols[2].markdown(kpi("Avg Order",    fc(cur["aov"]),           _delta("aov")),      unsafe_allow_html=True)
    cols[3].markdown(kpi("Accounts",     f'{cur["accounts"]:,}',   _delta("accounts")), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Determine best granularity
    span_days = (D_TO - D_FROM).days
    freq = "day" if span_days <= 31 else ("week" if span_days <= 90 else "month")

    col_l, col_r = st.columns([3, 2])

    with col_l:
        sec("Revenue Trend")
        trend_cur = revenue_by_period(D_FROM, D_TO, freq, **fargs)
        fig = go.Figure()
        if P_FROM:
            trend_pri = revenue_by_period(P_FROM, P_TO, freq, **fargs)
            fig.add_trace(go.Bar(name=pri_label, x=trend_pri["period"],
                                 y=trend_pri["revenue"], marker_color=LGRAY))
        fig.add_trace(go.Bar(name=cur_label, x=trend_cur["period"],
                             y=trend_cur["revenue"], marker_color=BLUE))
        fig.update_layout(**chart_layout(go.Figure(), 300).layout.to_plotly_json())
        fig.update_layout(barmode="group", yaxis_tickprefix="$", yaxis_tickformat=",.0f",
                          xaxis_title=freq.capitalize(), xaxis=dict(tickangle=-45, nticks=12),
                          plot_bgcolor="white", paper_bgcolor="white",
                          margin=dict(l=0,r=0,t=30,b=60), height=320,
                          legend=dict(orientation="h",y=1.12))
        st.plotly_chart(fig, width='stretch')

    with col_r:
        sec("Revenue by Tier")
        tier_cur = revenue_by_tier(D_FROM, D_TO, **fargs)
        if not tier_cur.empty:
            fig2 = px.pie(tier_cur, values="revenue", names="tier", hole=0.45,
                          color_discrete_map={"HA":BLUE,"10%":GOLD,"6%":"#5DADE2","Unknown":GRAY})
            fig2.update_traces(textinfo="label+percent")
            fig2.update_layout(height=300, margin=dict(l=0,r=0,t=30,b=0),
                               showlegend=False, plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig2, width='stretch')
        else:
            st.info("No tier data")

    # ── Account Health Strip ──────────────────────────────
    if P_FROM:
        try:
            growth_overview = account_comparison(D_FROM, D_TO, P_FROM, P_TO, **fargs)
            if not growth_overview.empty:
                sc = growth_overview["status"].value_counts()
                h1,h2,h3,h4,h5 = st.columns(5)
                h1.metric("📈 Growing",   int(sc.get("📈 Growth",   0)), help="Higher revenue than prior period")
                h2.metric("📉 Declining", int(sc.get("📉 Decline",  0)), help="Lower revenue than prior period")
                h3.metric("🔄 Returning", int(sc.get("🔄 Returning",0)), help="Active this period, skipped prior")
                h4.metric("💰 $500-Min",  int(sc.get("💰 $500-Min", 0)), help="No order in 6-12 months")
                h5.metric("⚠️ At Risk",   int(sc.get("⚠️ At Risk",  0)), help="No order in 12+ months")
        except: pass

    sec("Top Reps")
    col1, col2 = st.columns(2)
    with col1:
        reps_cur = revenue_by_rep(D_FROM, D_TO, **fargs).head(10)
        if not reps_cur.empty:
            fig3 = px.bar(reps_cur, x="revenue", y="rep", orientation="h",
                          color_discrete_sequence=[BLUE],
                          text=reps_cur["revenue"].apply(fc))
            fig3.update_traces(textposition="auto", textfont=dict(size=11))
            fig3.update_layout(height=320, yaxis=dict(autorange="reversed"),
                               xaxis_tickprefix="$", xaxis_tickformat=",.0f",
                               xaxis=dict(range=[0, reps_cur["revenue"].max() * 1.25]),
                               margin=dict(l=0,r=100,t=10,b=0),
                               plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig3, width='stretch')

    with col2:
        sec("Top Accounts")
        accts_cur = revenue_by_account(D_FROM, D_TO, **fargs).head(10)
        if not accts_cur.empty:
            fig4 = px.bar(accts_cur, x="revenue", y="company", orientation="h",
                          color_discrete_sequence=[GOLD],
                          text=accts_cur["revenue"].apply(fc))
            fig4.update_traces(textposition="auto", textfont=dict(size=11))
            fig4.update_layout(height=320, yaxis=dict(autorange="reversed"),
                               xaxis_tickprefix="$", xaxis_tickformat=",.0f",
                               xaxis=dict(range=[0, accts_cur["revenue"].max() * 1.25]),
                               margin=dict(l=0,r=100,t=10,b=0),
                               plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig4, width='stretch')

# ─────────────────────────────────────────────
# REP PERFORMANCE PAGE
# ─────────────────────────────────────────────
elif PAGE == "👤 Rep Performance":
    st.markdown(f'''<h1 style="font-size:1.5rem;font-weight:800;color:{BLUE};padding:12px 0 4px">
        Rep Account Performance</h1>''', unsafe_allow_html=True)

    if P_FROM:
        rep_df = rep_comparison(D_FROM, D_TO, P_FROM, P_TO, **fargs)
    else:
        rep_df = revenue_by_rep(D_FROM, D_TO, **fargs)

    if rep_df.empty:
        st.info("No rep data for this period.")
        st.stop()

    # ── 1. Rep Summary Table ──────────────────────────────
    sec("Rep Summary")

    if P_FROM and "revenue_cur" in rep_df.columns:
        disp = rep_df.rename(columns={
            "rep": "Rep",
            "revenue_cur": f"Rev {cur_label}",
            "orders_cur":  f"Orders {cur_label}",
            "revenue_pri": f"Rev {pri_label}",
            "orders_pri":  f"Orders {pri_label}",
            "rev_delta":   "$ Change",
            "rev_pct":     "% Change",
            "aov_cur":     f"AOV {cur_label}",
            "aov_pri":     f"AOV {pri_label}",
        })
        for col in [f"Orders {cur_label}", f"Orders {pri_label}"]:
            if col in disp.columns:
                disp[col] = pd.to_numeric(disp[col], errors="coerce").fillna(0).astype(int)
        # Merge At Risk / $500-Min counts into disp BEFORE building styled
        try:
            status_counts_rep = rep_status_summary()
            if not status_counts_rep.empty:
                disp = disp.merge(status_counts_rep, left_on="Rep", right_on="rep", how="left").drop(columns=["rep"], errors="ignore")
                disp["⚠️ At Risk"]  = disp["⚠️ At Risk"].fillna(0).astype(int)
                disp["💰 $500-Min"] = disp["💰 $500-Min"].fillna(0).astype(int)
        except: pass
        fmt = {
            f"Rev {cur_label}":    "${:,.2f}",
            f"Rev {pri_label}":    "${:,.2f}",
            "$ Change":            "+${:,.2f}",
            "% Change":            "{:+.1f}%",
            f"AOV {cur_label}":    "${:,.2f}",
            f"AOV {pri_label}":    "${:,.2f}",
            f"Orders {cur_label}": "{:,}",
            f"Orders {pri_label}": "{:,}",
        }
        if "⚠️ At Risk"  in disp.columns: fmt["⚠️ At Risk"]  = "{:,}"
        if "💰 $500-Min" in disp.columns: fmt["💰 $500-Min"] = "{:,}"
        styled = disp.style.format(
            {k: v for k, v in fmt.items() if k in disp.columns}, na_rep="—"
        ).map(color_map, subset=[c for c in ["$ Change","% Change"] if c in disp.columns])
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        disp = rep_df.rename(columns={"rep":"Rep","revenue":"Revenue","orders":"Orders","aov":"AOV"})
        st.dataframe(disp.style.format({"Revenue":"${:,.2f}","AOV":"${:,.2f}"}),
                     width="stretch", hide_index=True)

    st.markdown("---")

    # ── 2. Rep Drill-Down ─────────────────────────────────
    sec("Rep Drill-Down")

    all_reps = sorted(rep_df["rep"].dropna().unique().tolist()) if "rep" in rep_df.columns else []
    if not all_reps:
        st.info("No reps found.")
        st.stop()

    selected_rep = st.selectbox("Select Rep", all_reps)

    rep_fargs = dict(reps=[selected_rep], tiers=F_TIERS or None, stages=None)

    cur_accts = revenue_by_account(D_FROM, D_TO, **rep_fargs)
    pri_accts = revenue_by_account(P_FROM, P_TO, **rep_fargs) if P_FROM else pd.DataFrame()

    # KPIs for selected rep
    cur_rev   = cur_accts["revenue"].sum() if not cur_accts.empty else 0
    pri_rev   = pri_accts["revenue"].sum() if not pri_accts.empty else 0
    cur_ords  = int(cur_accts["orders"].sum()) if not cur_accts.empty else 0
    cur_acct_n = len(cur_accts)
    delta_rev = cur_rev - pri_rev
    delta_pct = (delta_rev / pri_rev * 100) if pri_rev else None

    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(kpi("Revenue", fc(cur_rev),
        delta_html(delta_rev, delta_pct) if delta_pct is not None else ""), unsafe_allow_html=True)
    k2.markdown(kpi("Orders",   f"{cur_ords:,}"), unsafe_allow_html=True)
    k3.markdown(kpi("Accounts", f"{cur_acct_n:,}"), unsafe_allow_html=True)
    k4.markdown(kpi("Avg Order", fc(cur_rev / cur_ords) if cur_ords else fc(0)), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 3. Account Status Table for this rep ─────────────
    sec("Account Activity")

    if not cur_accts.empty or not pri_accts.empty:
        cur_set = set(cur_accts["company"].tolist()) if not cur_accts.empty else set()
        pri_set = set(pri_accts["company"].tolist()) if not pri_accts.empty else set()

        cur_rev_map  = cur_accts.set_index("company")["revenue"].to_dict() if not cur_accts.empty else {}
        pri_rev_map  = pri_accts.set_index("company")["revenue"].to_dict() if not pri_accts.empty else {}
        cur_ord_map  = cur_accts.set_index("company")["orders"].to_dict()  if not cur_accts.empty else {}
        last_ord_map = cur_accts.set_index("company")["last_order"].to_dict() if not cur_accts.empty else {}

        all_companies = cur_set | pri_set
        acct_statuses = get_account_statuses(list(all_companies))
        rows = []
        for co in sorted(all_companies):
            c_rev = float(cur_rev_map.get(co, 0))
            p_rev = float(pri_rev_map.get(co, 0))
            delta = c_rev - p_rev
            pct   = (delta / p_rev * 100) if p_rev else None
            # Use shared status logic (days-based for $500-Min/At Risk)
            shared_status = acct_statuses.get(co)
            if shared_status in ("⚠️ At Risk", "💰 $500-Min"):
                status = shared_status
            elif co in cur_set and co not in pri_set:
                status = "🔄 Returning"
            elif co not in cur_set and co in pri_set:
                status = "⚠️ At Risk"
            elif delta > 0:
                status = "📈 Growing"
            elif delta < 0:
                status = "📉 Declining"
            else:
                status = "➡️ Flat"
            rows.append({
                "Account":         co,
                "Status":          status,
                cur_label:         c_rev,
                pri_label:         p_rev,
                "$ Change":        delta,
                "% Change":        round(pct, 1) if pct is not None else None,
                "Orders":          int(cur_ord_map.get(co, 0)),
                "Last Ordered":    last_ord_map.get(co, "—"),
            })

        acct_df = pd.DataFrame(rows).sort_values("$ Change", ascending=False)

        # Tab view: All | Returning | At Risk | Growing | Declining
        tab_all, tab_ret, tab_grow, tab_dec, tab_500, tab_risk = st.tabs([
            "All", "🔄 Returning", "📈 Growing", "📉 Declining", "💰 $500-Min", "⚠️ At Risk"
        ])

        def _rep_acct_table(df):
            if df.empty:
                st.info("No accounts in this category.")
                return
            fmt = {
                cur_label:  "${:,.2f}",
                pri_label:  "${:,.2f}",
                "$ Change": "+${:,.2f}",
                "% Change": "{:+.1f}%",
                "Orders":   "{:,}",
            }
            styled = df.style.format(
                {k: v for k, v in fmt.items() if k in df.columns}, na_rep="—"
            ).map(color_map, subset=[c for c in ["$ Change","% Change"] if c in df.columns])
            st.dataframe(styled, width="stretch", hide_index=True)

        with tab_all:  _rep_acct_table(acct_df)
        with tab_ret:  _rep_acct_table(acct_df[acct_df["Status"]=="🔄 Returning"])
        with tab_grow: _rep_acct_table(acct_df[acct_df["Status"]=="📈 Growing"])
        with tab_dec:  _rep_acct_table(acct_df[acct_df["Status"]=="📉 Declining"])
        with tab_500:  _rep_acct_table(acct_df[acct_df["Status"]=="💰 $500-Min"])
        with tab_risk: _rep_acct_table(acct_df[acct_df["Status"]=="⚠️ At Risk"])

        # ── 4. At-Risk callout ───────────────────────────
        needs_attn = acct_df[acct_df["Status"].isin(["⚠️ At Risk","💰 $500-Min"])]
        if not needs_attn.empty:
            st.markdown("<br>", unsafe_allow_html=True)
            sec("🚨 Needs Attention")
            at_risk_n = len(needs_attn[needs_attn["Status"]=="⚠️ At Risk"])
            min500_n  = len(needs_attn[needs_attn["Status"]=="💰 $500-Min"])
            parts = []
            if at_risk_n: parts.append(f"<b>{at_risk_n} ⚠️ At Risk</b> (no order in 12+ months — will be dropped without intervention)")
            if min500_n:  parts.append(f"<b>{min500_n} 💰 $500-Min</b> (no order in 6-12 months — needs re-engagement)")
            st.markdown(
                f'''<div style="background:#FFF3CD;border-left:4px solid #F39C12;
                border-radius:8px;padding:14px 18px;margin-bottom:12px">
                {"<br>".join(parts)}<br><br>
                These accounts should be a priority for <b>{selected_rep}</b> to contact.
                </div>''', unsafe_allow_html=True)
            _rep_acct_table(needs_attn[["Account","Status","Last Ordered"]])

# ─────────────────────────────────────────────
# ACCOUNTS PAGE
# ─────────────────────────────────────────────
elif PAGE == "🏢 Accounts":
    st.markdown(f'<h1 style="font-size:1.5rem;font-weight:800;color:{BLUE};padding:12px 0 4px">Account Analysis</h1>', unsafe_allow_html=True)

    if P_FROM:
        growth = account_comparison(D_FROM, D_TO, P_FROM, P_TO, **fargs)
        status_counts = growth["status"].value_counts() if not growth.empty else {}
        total_accts = len(growth) if not growth.empty else 0
        c0,c1,c2,c3,c4 = st.columns(5)
        c0.metric("Total Accounts", f"{total_accts:,}", help="All accounts active in either period")
        c1.metric("📈 Growing",   int(status_counts.get("📈 Growth",  0)), help="Higher revenue than prior period")
        c2.metric("📉 Declining", int(status_counts.get("📉 Decline", 0)), help="Lower revenue than prior period")
        c3.metric("🔄 Returning",       int(status_counts.get("🔄 Returning",     0)), help="First-time buyers this period")
        c4.metric("⚠️ At Risk",   int(status_counts.get("⚠️ At Risk", 0)), help="Ordered before, silent this period")
        st.markdown("<br>", unsafe_allow_html=True)

        tab_all, tab_grow, tab_dec, tab_ret, tab_500, tab_risk = st.tabs([
            "All", "📈 Growing", "📉 Declining", "🔄 Returning", "💰 $500-Min", "⚠️ At Risk"
        ])

        def _acct_table(df):
            if df.empty:
                st.info("No data")
                return
            disp = df.rename(columns={
                "company":         "Account",
                "revenue_cur":     cur_label,
                "revenue_pri":     pri_label,
                "delta":           "$ Change",
                "pct":             "% Change",
                "status":          "Status",
                "orders_cur":      f"Orders {cur_label}",
                "orders_pri":      f"Orders {pri_label}",
                "last_order_cur":  "Last Ordered",
            })
            show_cols = [c for c in [
                "Account", "Last Ordered",
                cur_label, pri_label, "$ Change", "% Change", "Status",
                f"Orders {cur_label}", f"Orders {pri_label}",
            ] if c in disp.columns]
            # Round all float columns to 2dp before display
            for col in [cur_label, pri_label, "$ Change"]:
                if col in disp.columns:
                    disp[col] = pd.to_numeric(disp[col], errors="coerce").round(2)
            if "% Change" in disp.columns:
                disp["% Change"] = pd.to_numeric(disp["% Change"], errors="coerce").round(1)
            for col in [f"Orders {cur_label}", f"Orders {pri_label}"]:
                if col in disp.columns:
                    disp[col] = pd.to_numeric(disp[col], errors="coerce").fillna(0).astype(int)
            fmt = {
                cur_label:              "${:,.2f}",
                pri_label:              "${:,.2f}",
                "$ Change":             "+${:,.2f}",
                "% Change":             "{:+.1f}%",
                f"Orders {cur_label}":  "{:,}",
                f"Orders {pri_label}":  "{:,}",
            }
            styled = disp[show_cols].style.format(
                {k: v for k, v in fmt.items() if k in disp[show_cols].columns},
                na_rep="—"
            ).map(color_map, subset=[c for c in ["$ Change", "% Change"] if c in disp[show_cols].columns])
            st.dataframe(styled, width="stretch", hide_index=True)

        def _tab_info(msg):
            st.markdown(f'<div style="font-size:0.82rem;color:#444;background:#EEF3FB;border-left:3px solid #1B3A6B;padding:7px 12px;border-radius:0 6px 6px 0;margin-bottom:10px">{msg}</div>', unsafe_allow_html=True)

        with tab_all:
            _tab_info("All accounts — sorted by revenue change vs prior period.")
            _acct_table(growth)
        with tab_grow:
            _tab_info("📈 <b>Growing</b> — ordered in both periods with higher revenue this period.")
            _acct_table(growth[growth["status"]=="📈 Growth"])
        with tab_dec:
            _tab_info("📉 <b>Declining</b> — ordered in both periods but spent less this period.")
            _acct_table(growth[growth["status"]=="📉 Decline"])
        with tab_ret:
            _tab_info("🔄 <b>Returning</b> — active this period but had no orders in the prior period.")
            _acct_table(growth[growth["status"]=="🔄 Returning"])
        with tab_500:
            _tab_info("💰 <b>$500-Min</b> — no order in 6-12 months. Needs re-engagement before they become At Risk.")
            _acct_table(growth[growth["status"]=="💰 $500-Min"])
        with tab_risk:
            _tab_info("⚠️ <b>At Risk</b> — no order in over 12 months. Will be dropped without intervention.")
            _acct_table(growth[growth["status"]=="⚠️ At Risk"])
    else:
        accts = revenue_by_account(D_FROM, D_TO, **fargs)
        # KPI cards even without comparison
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(kpi("Revenue",  fc(accts["revenue"].sum()) if not accts.empty else "$0"), unsafe_allow_html=True)
        c2.markdown(kpi("Orders",   f'{int(accts["orders"].sum()):,}' if not accts.empty else "0"), unsafe_allow_html=True)
        c3.markdown(kpi("Accounts", f'{len(accts):,}' if not accts.empty else "0"), unsafe_allow_html=True)
        c4.markdown(kpi("Avg Order", fc(accts["revenue"].sum()/accts["orders"].sum()) if not accts.empty and accts["orders"].sum() > 0 else "$0"), unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        sec("Accounts by Revenue")
        if not accts.empty:
            disp = accts.rename(columns={
                "company":    "Account",
                "rep":        "Rep",
                "tier":       "Tier",
                "revenue":    "Revenue",
                "orders":     "Orders",
                "aov":        "Avg Order",
                "last_order": "Last Ordered",
            })
            disp["Orders"] = disp["Orders"].astype(int)
            st.dataframe(
                disp[["Account","Last Ordered","Revenue","Orders","Avg Order","Rep","Tier"]].style.format({
                    "Revenue":   "${:,.2f}",
                    "Avg Order": "${:,.2f}",
                    "Orders":    "{:,}",
                }), width="stretch", hide_index=True
            )

    # Account drill-down
    st.markdown("---")
    sec("Account Drill-Down")
    acct_list = [r for r in get_filter_options().get("reps",[]) ] # placeholder
    with get_conn() as conn:
        all_accounts = [r[0] for r in conn.execute(
            "SELECT DISTINCT company FROM orders WHERE source='Backend' ORDER BY company"
        ).fetchall()]
    search = st.text_input("Search account", "")
    filtered_accounts = [a for a in all_accounts if search.lower() in a.lower()] if search else all_accounts
    if filtered_accounts:
        sel_acct = st.selectbox("Account", filtered_accounts)
        acct_orders = query_orders(D_FROM, D_TO, **fargs)
        acct_orders = acct_orders[acct_orders["company"] == sel_acct] if not acct_orders.empty else acct_orders
        if not acct_orders.empty:
            cc1,cc2,cc3 = st.columns(3)
            cc1.metric("Revenue", fc(acct_orders["total"].sum()))
            cc2.metric("Orders",  len(acct_orders))
            cc3.metric("Rep",     acct_orders["rep"].iloc[0] if "rep" in acct_orders.columns else "—")
            show = [c for c in ["reference","order_date","rep","tier","stage","total"] if c in acct_orders.columns]
            st.dataframe(acct_orders[show].sort_values("order_date",ascending=False)
                         .style.format({"total":"${:,.2f}"}), width="stretch", hide_index=True)
        else:
            st.info("No orders for this account in the selected period.")

