import streamlit as st
import pandas as pd
import plotly.express as px
from confluent_kafka import Consumer, KafkaException
import json
import base64
from decimal import Decimal
import time
import os

# Helper to decode Debezium's base64 decimal format
def decode_debezium_decimal(data, scale=2):
    if data is None:
        return 0.0
    decoded_bytes = base64.b64decode(data)
    val = int.from_bytes(decoded_bytes, byteorder='big', signed=True)
    return float(Decimal(val) / Decimal(10**scale))

# Helper to format timestamps
def format_timestamp(ts):
    if ts is None:
        return ""
    # Debezium Postgres timestamps are typically microseconds since epoch
    if isinstance(ts, int):
        # Convert microseconds to seconds
        return pd.to_datetime(ts, unit='us').strftime('%Y-%m-%d %H:%M:%S')
    return ts

st.set_page_config(page_title="Bank Real-time Dashboard", layout="wide")
st.title("🏦 Virtual Bank Real-time Command Center")

# Initialize Session State for data
if 'transactions' not in st.session_state:
    st.session_state.transactions = []
if 'alerts' not in st.session_state:
    st.session_state.alerts = []

# Kafka Consumer Setup
@st.cache_resource
def get_consumers():
    broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
    conf = {
        'bootstrap.servers': broker,
        'group.id': 'streamlit-dashboard',
        'auto.offset.reset': 'earliest'
    }
    tx_consumer = Consumer(conf)
    tx_consumer.subscribe(['dbserver1.public.transactions'])
    
    alert_consumer = Consumer(conf)
    alert_consumer.subscribe(['fraud-alerts'])
    
    return tx_consumer, alert_consumer

tx_consumer, alert_consumer = get_consumers()

# --- SIDEBAR CONTROLS ---
st.sidebar.header("🕹️ Dashboard Controls")
filter_type = st.sidebar.multiselect(
    "Filter by Transaction Type",
    options=["deposit", "withdrawal", "transfer"],
    default=["deposit", "withdrawal", "transfer"]
)

search_account = st.sidebar.text_input("🔍 Search Account ID", "")

if st.sidebar.button("🗑️ Clear Alerts"):
    st.session_state.alerts = []
    # No st.rerun() to avoid session reset

st.sidebar.divider()
st.sidebar.info("Dashboard updates live every 2s. Use filters to narrow down view.")

# --- MAIN LAYOUT ---
# KPI Row
kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
total_val_metric = kpi_col1.empty()
tx_count_metric = kpi_col2.empty()
alert_count_metric = kpi_col3.metric("Active Alerts", 0) # Placeholder for fragment update
alert_count_metric = kpi_col3.empty()
avg_tx_metric = kpi_col4.empty()

st.divider()

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📈 Live Transaction Stream")
    chart_placeholder = st.empty()
    
    st.subheader("📑 Recent Activity")
    table_placeholder = st.empty()

with col2:
    st.subheader("🚨 Live Fraud Alerts")
    alerts_placeholder = st.empty()
    
    st.subheader("📊 Transaction Distribution")
    dist_placeholder = st.empty()

# Function to poll Kafka
def poll_kafka():
    # Poll up to 50 messages to catch up faster
    for _ in range(50):
        msg = tx_consumer.poll(0.0)
        if msg is not None and not msg.error():
            data = json.loads(msg.value().decode('utf-8'))
            after = data.get('payload', {}).get('after')
            if after:
                after['amount'] = decode_debezium_decimal(after.get('amount'))
                st.session_state.transactions.append(after)
                if len(st.session_state.transactions) > 300: # Larger history
                    st.session_state.transactions.pop(0)
        else:
            break

    # Also catch up alerts faster
    for _ in range(20):
        amsg = alert_consumer.poll(0.0)
        if amsg is not None and not amsg.error():
            alert_data = json.loads(amsg.value().decode('utf-8'))
            st.session_state.alerts.append(alert_data)
            if len(st.session_state.alerts) > 50:
                st.session_state.alerts.pop(0)
        else:
            break

@st.fragment(run_every=2)
def update_dashboard():
    poll_kafka()

    if st.session_state.transactions:
        all_df = pd.DataFrame(st.session_state.transactions)
        
        # Apply formatting to the created_at column
        if 'created_at' in all_df.columns:
            all_df['created_at_fmt'] = all_df['created_at'].apply(format_timestamp)
        
        # Apply Interactive Filters
        df = all_df[all_df['type'].isin(filter_type)]
        if search_account:
            df = df[df['account_id'].str.contains(search_account, case=False)]

        # --- UPDATE KPIs ---
        total_val = df['amount'].sum()
        tx_count = len(df)
        alert_count = len(st.session_state.alerts)
        avg_tx = total_val / tx_count if tx_count > 0 else 0

        total_val_metric.metric("Total Volume", f"${total_val:,.2f}")
        tx_count_metric.metric("TX Count", tx_count)
        alert_count_metric.metric("Active Alerts", alert_count, delta_color="inverse")
        avg_tx_metric.metric("Avg Transaction", f"${avg_tx:,.2f}")

        # --- UPDATE CHARTS ---
        if not df.empty:
            # 1. Line Chart - Use formatted time on X-axis
            # Create a display copy for the chart to use formatted time
            chart_df = df.copy()
            
            fig = px.line(chart_df, x='Time' if 'Time' in chart_df.columns else chart_df.index, 
                          y='amount', color='type',
                          title='Transaction Amounts over Time',
                          template="plotly_dark",
                          labels={"x": "Time", "amount": "Amount ($)"},
                          color_discrete_map={"deposit": "#00CC96", "withdrawal": "#EF553B", "transfer": "#636EFA"})
            
            # Improve X-axis visibility
            fig.update_xaxes(tickangle=45)
            chart_placeholder.plotly_chart(fig, use_container_width=True, key="tx_line_chart")
            
            # 2. Recent Table - Use formatted date
            display_df = df.tail(15).copy()
            if 'created_at_fmt' in display_df.columns:
                # Reorder to show formatted date and drop the raw one for display
                cols = ['created_at_fmt'] + [c for c in display_df.columns if c not in ['created_at', 'created_at_fmt']]
                display_df = display_df[cols].rename(columns={'created_at_fmt': 'Time'})
            
            # Explicitly drop the raw 'created_at' if it still exists in display_df
            if 'created_at' in display_df.columns:
                display_df = display_df.drop(columns=['created_at'])
            
            table_placeholder.dataframe(display_df, use_container_width=True)
            
            # 3. Distribution Pie Chart
            type_counts = df['type'].value_counts().reset_index()
            type_counts.columns = ['type', 'count']
            fig_pie = px.pie(type_counts, values='count', names='type', 
                             hole=0.4, template="plotly_dark",
                             color='type',
                             color_discrete_map={"deposit": "#00CC96", "withdrawal": "#EF553B", "transfer": "#636EFA"})
            dist_placeholder.plotly_chart(fig_pie, use_container_width=True, key="tx_pie_chart")

    # --- UPDATE ALERTS ---
    with alerts_placeholder.container(height=400):
        if not st.session_state.alerts:
            st.write("✅ No active alerts")
        else:
            for alert in reversed(st.session_state.alerts):
                # Filter alerts too if an account is searched
                if not search_account or search_account.upper() in alert['account_id'].upper():
                    time_str = format_timestamp(alert.get('timestamp'))
                    st.error(f"**{alert['reason']}**\n\nAcc: `{alert['account_id']}` | **${alert['amount']:,.2f}**\n\n*Time: {time_str}*")

# Start the fragment loop
update_dashboard()
