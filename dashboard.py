import os
import time
import pandas as pd
import streamlit as st
from pymongo import MongoClient

try:
    from dotenv import load_dotenv
    load_dotenv()                      # local: reads .env  (cloud: real env vars)
except ImportError:
    pass

st.set_page_config(page_title="Live CDC Dashboard", layout="wide")


@st.cache_resource
def get_collection():
    client = MongoClient(os.environ["MONGO_URI"])
    return client[os.getenv("MONGO_DB", "cdc_lab")][os.getenv("MONGO_COLLECTION", "customers")]


@st.cache_data(ttl=5)                  # re-read MongoDB at most every 5 seconds
def load_customers():
    docs = list(get_collection().find({}))
    df = pd.DataFrame(docs)
    if df.empty:
        return df
    if "_deleted" not in df.columns:
        df["_deleted"] = False
    df["_deleted"] = df["_deleted"].fillna(False).astype(bool)
    df["ANNUAL_INCOME"] = pd.to_numeric(df.get("ANNUAL_INCOME"), errors="coerce")
    df["_last_txseq"] = df.get("_last_txseq", 0)
    df["_last_txseq"] = df["_last_txseq"].fillna(0).astype(int)
    return df


st.title("Live CDC Dashboard")
st.caption("Oracle -> Debezium -> Kafka -> Python consumer -> MongoDB Atlas. "
           "Change a row in Oracle and it shows up here within seconds.")

df = load_customers()
if df.empty:
    st.warning("No customers found in MongoDB yet.")
    st.stop()

active = df[~df["_deleted"]]
deleted = df[df["_deleted"]]

# --- Top numbers ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Active customers", f"{len(active):,}")
c2.metric("Deleted (soft)", f"{len(deleted):,}")
avg_income = active["ANNUAL_INCOME"].mean()
c3.metric("Avg annual income", f"Rs {avg_income/100000:.1f} L" if pd.notna(avg_income) else "-")
c4.metric("Latest SCN applied", f"{int(df['_last_scn'].max()):,}")

# --- Charts ---
left, right = st.columns(2)
with left:
    st.subheader("Active customers by city")
    st.bar_chart(active["CITY"].value_counts().sort_index())
with right:
    st.subheader("Active customers by segment")
    st.bar_chart(active["SEGMENT"].value_counts().sort_index())

st.subheader("Average annual income by segment (Rs lakh)")
st.bar_chart((active.groupby("SEGMENT")["ANNUAL_INCOME"].mean() / 100000).round(1))

# --- Latest changes ---
st.subheader("Latest changes from the pipeline")
recent = df.sort_values(["_last_scn", "_last_txseq"], ascending=False).head(15).copy()
recent["STATUS"] = recent["_deleted"].map({True: "Deleted", False: "Active"})
st.dataframe(
    recent[["_id", "NAME", "CITY", "SEGMENT", "ANNUAL_INCOME", "STATUS", "_last_scn", "_last_txseq"]]
    .rename(columns={"_id": "CUSTOMER_ID", "_last_scn": "SCN", "_last_txseq": "TXSEQ"}),
    hide_index=True, width="stretch",
)

# --- Refresh controls ---
st.divider()
auto = st.checkbox("Auto-refresh every 5 seconds", value=True)
st.caption(f"Last loaded: {time.strftime('%H:%M:%S')}")
if auto:
    time.sleep(5)
    st.rerun()
