import streamlit as st
import redis
import pandas as pd
import json
import time

st.set_page_config(page_title="TikTok Trends EG", page_icon="🎵", layout="wide")
st.title("🎵 TikTok Trending Sounds: Egypt 🇪🇬")
st.caption("Data source: Tokchart | Updates Daily")

try:
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
except:
    st.error("Redis connection failed")

placeholder = st.empty()

def load_data():
    try:
        # ZRANGE: بيجيب من الصغير للكبير (يعني 1 ثم 2 ثم 3)
        data = r.zrange("tiktok_trends_eg", 0, -1, withscores=True)
        rows = []
        for val_json, score in data:
            item = json.loads(val_json)
            rows.append({
                "Rank": int(score),
                "Sound Name": item['name'],
                "UGC Count": item['ugc'],
                "Growth": item['growth'],
                "Fetched At": item['fetched_at'],
                "Listen": item['link']
            })
        return pd.DataFrame(rows)
    except: return pd.DataFrame()

while True:
    with placeholder.container():
        df = load_data()
        
        if df.empty:
            st.info("⏳ Waiting for daily scrape... (This might take a few minutes)")
            time.sleep(1)
            continue
        
        # عرض آخر تحديث
        last_update = df.iloc[0]['Fetched At']
        st.success(f"✅ Last Updated: {last_update}")

        st.dataframe(
            df,
            column_config={
                "Rank": st.column_config.NumberColumn(format="#%d"),
                "Listen": st.column_config.LinkColumn("TikTok Link"), # لينك قابل للضغط
                "Fetched At": st.column_config.DatetimeColumn(format="D MMM, HH:mm")
            },
            hide_index=True,
            use_container_width=True
        )

    time.sleep(10)