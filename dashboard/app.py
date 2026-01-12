import streamlit as st
from clickhouse_driver import Client
import pandas as pd
import altair as alt
from datetime import timedelta


from config import HOST, PORT, USER, PASSWORD, TABLE_NAME

# Connect to ClickHouse
client = Client(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD
)

st.title("Merchant Intelligence Dashboard")


tabs = st.tabs([
    "Top 20 Products by Engagement",
    "High CTR Products",
    "Low Engagement Products",
    "Daily Engagement Trends",
    "Top AddToCart/Transaction Ratio",
    "Low Engagement Alerts"
])

# ----------------------
# Top 20 Products by Engagement Score
# ----------------------
with tabs[0]:
    top_engagement = client.execute(f"""
        SELECT date, product_id, ctr, 
        CASE WHEN impressions = 0 THEN 0 ELSE addtocart/impressions END AS addtocart_rate,
        CASE WHEN impressions = 0 THEN 0 ELSE transaction/impressions END AS conversion_rate,
        CASE WHEN impressions = 0 THEN 0 ELSE 0.5 * (clicks/impressions) + 0.3 * (addtocart/impressions) + 0.2 * (transaction/impressions) END AS engagement_score
        FROM {TABLE_NAME}
        ORDER BY engagement_score DESC
        LIMIT 20
    """)
    top_engagement_df = pd.DataFrame(top_engagement, columns=[
        "date", "product_id", "ctr", "addtocart_rate", "conversion_rate", "engagement_score"
    ])
    st.dataframe(top_engagement_df)

# ----------------------
# High CTR Products
# ----------------------
with tabs[1]:
    high_ctr = client.execute(f"""
        SELECT * FROM {TABLE_NAME}
        WHERE clicks/impressions > 0.8
        ORDER BY clicks/impressions DESC
        LIMIT 20
    """)
    high_ctr_df = pd.DataFrame(high_ctr, columns=[
        "date", "product_id", "addtocart", "transaction", "view", "impressions", "clicks", "ctr"
    ])
    st.dataframe(high_ctr_df)

# ----------------------
# Low Engagement Products
# ----------------------
with tabs[2]:
    low_engagement = client.execute(f"""
        SELECT * FROM {TABLE_NAME}
        WHERE 0.5*(clicks/impressions)+0.3*(addtocart/impressions)+0.2*(transaction/impressions) < 0.1
        ORDER BY date DESC
        LIMIT 20
    """)
    low_engagement_df = pd.DataFrame(low_engagement, columns=[
        "date", "product_id", "addtocart", "transaction", "view", "impressions", "clicks", "ctr"
    ])
    st.dataframe(low_engagement_df)

# ----------------------
# Daily Engagement Trends per Product
# ----------------------
with tabs[3]:
    
    max_date_result = client.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
    max_date = pd.to_datetime(max_date_result[0][0])

    
    three_months_ago = (max_date - timedelta(days=90)).strftime('%Y-%m-%d')

    
    daily_trends = client.execute(f"""
        SELECT product_id, date, 
        CASE 
            WHEN impressions = 0 THEN 0 
            ELSE 0.5*(clicks/impressions)+0.3*(addtocart/impressions)+0.2*(transaction/impressions) 
        END AS engagement_score
        FROM {TABLE_NAME}
        WHERE date >= '{three_months_ago}'
        ORDER BY date ASC
    """)


    daily_trends_df = pd.DataFrame(daily_trends, columns=[
        "product_id", "date", "engagement_score"
    ])
    daily_trends_df['date'] = pd.to_datetime(daily_trends_df['date'])

    
    avg_engagement_df = daily_trends_df.groupby('date', as_index=False)['engagement_score'].mean()

    
    line_chart = alt.Chart(avg_engagement_df).mark_line().encode(
        x='date:T',
        y='engagement_score:Q',
        tooltip=['date', 'engagement_score']
    ).properties(
        title='Daily Average Engagement (All Products)'
    ).interactive()

    st.altair_chart(line_chart, width = "stretch")

# ----------------------
# Top Performing Products by AddToCart/Transaction Ratio
# ----------------------
with tabs[4]:
    top_ratio = client.execute(f"""
        SELECT product_id, 
        CASE WHEN SUM(transaction) = 0 THEN 0 ELSE SUM(addtocart)/SUM(transaction) END AS add_to_transaction_ratio
        FROM {TABLE_NAME}
        GROUP BY product_id
        ORDER BY add_to_transaction_ratio DESC
        LIMIT 20
    """)
    top_ratio_df = pd.DataFrame(top_ratio, columns=[
        "product_id", "add_to_transaction_ratio"
    ])
    st.dataframe(top_ratio_df)

# ----------------------
# Low Engagement Alerts
# ----------------------
with tabs[5]:
    low_alerts = client.execute(f"""
        SELECT product_id, date
        FROM {TABLE_NAME}
        WHERE (0.5*(clicks/impressions)+0.3*(addtocart/impressions)+0.2*(transaction/impressions)) < 0.05
    """)
    low_alerts_df = pd.DataFrame(low_alerts, columns=["product_id", "date"])
    st.dataframe(low_alerts_df)
