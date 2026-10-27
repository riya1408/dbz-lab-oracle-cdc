"""Lumen Bank (fictional) - live Customer 360 dashboard.

Reads the MongoDB collections maintained by consumer_360.py:
  customer_360   one document per customer (profile, accounts, derived fields, alerts)
  transactions   recent transactions (90-day hot window)
"""
import datetime as dt
import os
import time

import pandas as pd
import streamlit as st
from pymongo import MongoClient

try:
    from dotenv import load_dotenv
    load_dotenv()                      # local: reads .env (cloud: real environment variables)
except ImportError:
    pass

st.set_page_config(page_title="Lumen Bank - Customer 360", page_icon="🏦", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient(os.environ["MONGO_URI"])[os.getenv("MONGO_DB", "lumen_bank")]


def now_utc():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)   # pymongo returns naive UTC


def inr(x):
    """Format rupees the Indian way, e.g. Rs 12,34,567."""
    if x is None or pd.isna(x):
        return "-"
    sign, x = ("-" if x < 0 else ""), abs(round(x))
    s = str(int(x))
    if len(s) > 3:
        head, tail = s[:-3], s[-3:]
        head = ",".join([head[max(i - 2, 0):i] for i in range(len(head), 0, -2)][::-1])
        s = head + "," + tail
    return f"{sign}Rs {s}"


def ago(ts):
    if ts is None:
        return "-"
    secs = int((now_utc() - ts).total_seconds())
    if secs < 60:
        return f"{max(secs, 0)}s ago"
    if secs < 3600:
        return f"{secs // 60} min ago"
    if secs < 86400:
        return f"{secs // 3600} h ago"
    return f"{secs // 86400} d ago"


# ---------------- data loading (cached for 5 s) ----------------

@st.cache_data(ttl=5)
def load_overview():
    db = get_db()
    customers = pd.DataFrame(list(db.customer_360.find(
        {}, {"name": 1, "city": 1, "segment": 1, "kyc_status": 1, "_deleted": 1,
             "net_position": 1, "accounts": 1, "alerts": 1})))
    deposits = loans = 0.0
    for accs in customers.get("accounts", pd.Series(dtype=object)).dropna():
        for a in accs.values():
            if a.get("_deleted") or a.get("balance") is None:
                continue
            if a["balance"] >= 0:
                deposits += a["balance"]
            else:
                loans += -a["balance"]
    since = now_utc() - dt.timedelta(days=30)
    spend = pd.DataFrame(list(db.transactions.aggregate([
        {"$match": {"_deleted": False, "direction": "DR", "ts": {"$gte": since}}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}}])))
    hour_ago = now_utc() - dt.timedelta(hours=1)
    last_hour = db.transactions.count_documents({"ts": {"$gte": hour_ago}})
    latest = list(db.transactions.find({"_deleted": False}).sort("ts", -1).limit(15))
    return customers, deposits, loans, spend, last_hour, latest


@st.cache_data(ttl=5)
def load_customer(cid):
    return get_db().customer_360.find_one({"_id": int(cid)})


# ---------------- header ----------------

st.title("🏦 Lumen Bank · Live Customer 360")
st.caption("A **fictional** bank with synthetic data. Oracle → Debezium → Kafka → Customer 360 consumer → "
           "MongoDB Atlas. Every number below is computed from CDC events, live.")

customers, deposits, loans, spend, last_hour, latest = load_overview()
if customers.empty:
    st.warning("No customers yet. Run the consumer: python consumer_360.py")
    st.stop()

customers["_deleted"] = customers.get("_deleted", False)
customers["_deleted"] = customers["_deleted"].fillna(False).astype(bool)
active = customers[~customers["_deleted"]]
ALERT_WINDOW = dt.timedelta(hours=24)


def live_alerts(alerts):
    """Only alerts from the last 24 hours (defence in depth: the consumer also expires them)."""
    if not isinstance(alerts, list):
        return []
    return [a for a in alerts if now_utc() - a["detected_at"] <= ALERT_WINDOW]


active = active.copy()
active["live_alerts"] = active["alerts"].apply(live_alerts) if "alerts" in active else [[]] * len(active)
has_alert = active[active["live_alerts"].apply(len) > 0]
names = dict(zip(customers["_id"], customers["name"]))

tab_overview, tab_alerts, tab_customer = st.tabs(["📊 Bank overview", "🚨 Alerts", "👤 Customer lookup"])

# ---------------- tab 1: overview ----------------

with tab_overview:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active customers", f"{len(active):,}")
    c2.metric("Deposits", inr(deposits))
    c3.metric("Loans + card dues", inr(loans))
    c4.metric("Transactions (last hour)", f"{last_hour:,}")
    c5.metric("Active alerts", len(has_alert))

    freshness = latest[0]["ts"] if latest else None
    st.caption(f"Latest transaction applied: **{ago(freshness)}**")

    left, right = st.columns(2)
    with left:
        st.subheader("Spend by category (30 days)")
        if not spend.empty:
            st.bar_chart(spend.set_index("_id")["total"].sort_values(ascending=False) / 100000,
                         y_label="Rs lakh")
    with right:
        st.subheader("Customers by segment")
        st.bar_chart(active["segment"].value_counts())

    st.subheader("Live transaction feed")
    if latest:
        feed = pd.DataFrame([{
            "When": ago(t["ts"]), "Customer": f'{names.get(t["customer_id"], "?")} ({t["customer_id"]})',
            "Amount": inr(t["amount"]), "In/Out": "⬆️ in" if t["direction"] == "CR" else "⬇️ out",
            "Category": t.get("category"), "Merchant": t.get("merchant"), "City": t.get("city"),
            "Channel": t.get("channel")} for t in latest])
        st.dataframe(feed, hide_index=True, width="stretch")

# ---------------- tab 2: alerts ----------------

with tab_alerts:
    st.subheader("Customers with active fraud alerts")
    st.caption("Rule: 5+ card payments across 3+ cities within 10 minutes (last 24 hours).")
    if has_alert.empty:
        st.success("No active alerts. Try: python simulate_bank.py fraud")
    else:
        rows = []
        for _, r in has_alert.iterrows():
            for a in r["live_alerts"]:
                rows.append({"Customer ID": r["_id"], "Name": r["name"], "Home city": r["city"],
                             "Alert": a["detail"], "Cities": ", ".join(a["cities"]),
                             "Amount": inr(a["total"]), "Detected": ago(a["detected_at"])})
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        st.info("Open the **Customer lookup** tab and enter a Customer ID to see the full picture.")

# ---------------- tab 3: customer lookup ----------------

with tab_customer:
    default = int(has_alert["_id"].iloc[0]) if not has_alert.empty else 1
    cid = st.number_input("Customer ID", min_value=1, value=default, step=1)
    doc = load_customer(cid)
    if not doc:
        st.warning("No such customer.")
    else:
        status = "🗑️ deleted" if doc.get("_deleted") else "active"
        st.markdown(f"### {doc.get('name', '(name not received yet)')}  ·  {doc.get('city', '-')}  ·  {status}")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Segment", doc.get("segment", "-"))
        kyc = doc.get("kyc_status", "-")
        k2.metric("KYC", ("✅ " if kyc == "VERIFIED" else "⚠️ ") + kyc)
        k3.metric("Risk tier", doc.get("risk_tier", "-"))
        k4.metric("Net position", inr(doc.get("net_position")))

        for a in live_alerts(doc.get("alerts", [])):
            st.error(f"🚨 {a['detail']} - {', '.join(a['cities'])} - {inr(a['total'])} ({ago(a['detected_at'])})")

        left, right = st.columns([3, 2])
        with left:
            st.markdown("**Accounts**")
            accs = [{"Account": k, "Type": v.get("type"), "Balance": inr(v.get("balance")),
                     "Status": "closed" if v.get("_deleted") else v.get("status")}
                    for k, v in (doc.get("accounts") or {}).items()]
            st.dataframe(pd.DataFrame(accs), hide_index=True, width="stretch")
        with right:
            st.markdown("**Spend in the last 30 days**")
            s = doc.get("spend_30d") or {}
            if s:
                st.bar_chart(pd.Series(s).sort_values(ascending=False))
            else:
                st.caption("No spending in the last 30 days.")

        st.markdown(f"**Last 10 transactions** ({doc.get('txn_count_30d', 0)} in the last 30 days)")
        rt = [{"When": t["ts"].strftime("%d %b %H:%M"), "Amount": inr(t["amount"]),
               "In/Out": "⬆️ in" if t["direction"] == "CR" else "⬇️ out", "Category": t.get("category"),
               "Merchant": t.get("merchant"), "City": t.get("city"), "Channel": t.get("channel")}
              for t in doc.get("recent_transactions", [])]
        st.dataframe(pd.DataFrame(rt), hide_index=True, width="stretch")

# ---------------- refresh ----------------

st.divider()
auto = st.toggle("Auto-refresh every 5 seconds", value=True)
st.caption(f"Last loaded {time.strftime('%H:%M:%S')} · Lumen Bank is fictional; all data is synthetic.")
if auto:
    time.sleep(5)
    st.rerun()
