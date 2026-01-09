import streamlit as st
from clickhouse_driver import Client
import pandas as pd

from .config import HOST, PORT, USER, PASSWORD, TABLE_NAME

# Connect to ClickHouse
client = Client(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD
)

st.title("Merchant Intelligence Dashboard")

# ----------------------
# Top 20 Products by Engagement Score
# ----------------------
top_engagement = client.execute(f"""
    SELECT date, product_id, ctr, addtocart/impressions AS addtocart_rate,
           transaction/impressions AS conversion_rate,
           0.5 * (clicks/impressions) + 0.3 * (addtocart/impressions) + 0.2 * (transaction/impressions) AS engagement_score
    FROM {TABLE_NAME}
    ORDER BY engagement_score DESC
    LIMIT 20
""")
top_engagement_df = pd.DataFrame(top_engagement, columns=[
    "date", "product_id", "ctr", "addtocart_rate", "conversion_rate", "engagement_score"
])
st.subheader("Top 20 Products by Engagement Score")
st.dataframe(top_engagement_df)

# ----------------------
# High CTR Products
# ----------------------
high_ctr = client.execute(f"""
    SELECT * FROM {TABLE_NAME}
    WHERE clicks/impressions > 0.8
    ORDER BY clicks/impressions DESC
    LIMIT 20
""")
high_ctr_df = pd.DataFrame(high_ctr, columns=[
    "date", "product_id", "addtocart", "transaction", "view", "impressions", "clicks", "ctr"
])
st.subheader("Products with High CTR")
st.dataframe(high_ctr_df)

# ----------------------
# Low Engagement Products
# ----------------------
low_engagement = client.execute(f"""
    SELECT * FROM {TABLE_NAME}
    WHERE 0.5*(clicks/impressions)+0.3*(addtocart/impressions)+0.2*(transaction/impressions) < 0.1
    ORDER BY date DESC
    LIMIT 20
""")
low_engagement_df = pd.DataFrame(low_engagement, columns=[
    "date", "product_id", "addtocart", "transaction", "view", "impressions", "clicks", "ctr"
])
st.subheader("Products with Low Engagement")
st.dataframe(low_engagement_df)

# ----------------------
# Daily Engagement Trends per Product
# ----------------------
daily_trends = client.execute(f"""
    SELECT product_id, date, 
           0.5*(clicks/impressions)+0.3*(addtocart/impressions)+0.2*(transaction/impressions) AS engagement_score
    FROM {TABLE_NAME}
    ORDER BY product_id, date
""")
daily_trends_df = pd.DataFrame(daily_trends, columns=[
    "product_id", "date", "engagement_score"
])
st.subheader("Daily Engagement Trends per Product")
st.dataframe(daily_trends_df)

# ----------------------
# Top Performing Products by AddToCart/Transaction Ratio
# ----------------------
top_ratio = client.execute(f"""
    SELECT product_id, SUM(addtocart)/SUM(transaction) AS add_to_transaction_ratio
    FROM {TABLE_NAME}
    GROUP BY product_id
    ORDER BY add_to_transaction_ratio DESC
    LIMIT 20
""")
top_ratio_df = pd.DataFrame(top_ratio, columns=[
    "product_id", "add_to_transaction_ratio"
])
st.subheader("Top Performing Products by AddToCart/Transaction Ratio")
st.dataframe(top_ratio_df)

# ----------------------
# Low Engagement Alerts
# ----------------------
low_alerts = client.execute(f"""
    SELECT product_id, date
    FROM {TABLE_NAME}
    WHERE (0.5*(clicks/impressions)+0.3*(addtocart/impressions)+0.2*(transaction/impressions)) < 0.05
""")
low_alerts_df = pd.DataFrame(low_alerts, columns=["product_id", "date"])
st.subheader("Low Engagement Alerts")
st.dataframe(low_alerts_df)
