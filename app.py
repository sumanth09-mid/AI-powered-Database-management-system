import os
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="Query Logs Dashboard",
    layout="wide"
)

st.title("📊 Query Logs Dashboard")

# -----------------------------
# Load Data from Data Lake
# -----------------------------
DATA_LAKE_PATH = "datalake/query_logs"

def load_data():
    if not os.path.exists(DATA_LAKE_PATH):
        return pd.DataFrame()

    tables = []
    for root, dirs, files in os.walk(DATA_LAKE_PATH):
        for file in files:
            if file.endswith(".parquet"):
                table = pq.read_table(os.path.join(root, file))
                tables.append(table.to_pandas())

    if not tables:
        return pd.DataFrame()

    return pd.concat(tables, ignore_index=True)

df = load_data()

if df.empty:
    st.warning("No logs found in data lake yet.")
    st.stop()

# -----------------------------
# Data Preparation
# -----------------------------
df["timestamp"] = pd.to_datetime(df["timestamp"])

# -----------------------------
# METRICS
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Queries", len(df))
col2.metric("ADMIN Queries", len(df[df["role"] == "ADMIN"]))
col3.metric("USER Queries", len(df[df["role"] == "USER"]))
col4.metric("Failed Queries", len(df[df["status"] == "FAILED"]))

st.divider()

# -----------------------------
# Charts
# -----------------------------
st.subheader("📌 Queries by Status")
st.bar_chart(df["status"].value_counts())

st.subheader("👥 Queries by Role")
st.bar_chart(df["role"].value_counts())

st.subheader("🕒 Queries Over Time")
time_series = df.set_index("timestamp").resample("1Min").size()
st.line_chart(time_series)

st.divider()

# -----------------------------
# Recent Queries Table
# -----------------------------
st.subheader("🧾 Recent Queries")
st.dataframe(
    df.sort_values("timestamp", ascending=False)[
        ["timestamp", "role", "english_query", "status"]
    ].head(20),
    use_container_width=True
)
