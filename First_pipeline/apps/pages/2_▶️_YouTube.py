import streamlit as st
import redis
import pandas as pd
import time
import altair as alt
import json

st.set_page_config(page_title="YouTube Trends EG", page_icon="▶️", layout="wide")
st.title("▶️ YouTube Trending: Egypt 🇪🇬")

try:
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
except Exception as e:
    st.error(f"Redis Error: {e}")

placeholder = st.empty()

def load_category_stats():
    """تحميل إحصائيات الكاتيجوري"""
    try:
        data = r.zrevrange("youtube_category_views", 0, -1, withscores=True)
        df = pd.DataFrame(data, columns=['Category', 'Total Views'])
        if not df.empty:
            df['Total Views'] = df['Total Views'].astype(int)
        return df
    except:
        return pd.DataFrame(columns=['Category', 'Total Views'])

def load_top_videos():
    """(الجديد) تحميل الفيديو التريند لكل كاتيجوري"""
    try:
        # بنجيب كل الداتا اللي في الهاش
        data_map = r.hgetall("youtube_category_top_video")
        
        rows = []
        for category, details_json in data_map.items():
            details = json.loads(details_json)
            rows.append({
                "Category": category,
                "Trending Video": details['title'],
                "Channel": details['channel'],
                "Video Views": int(details['views']),
                "Link": details['url']
            })
        
        df = pd.DataFrame(rows)
        # نرتبهم حسب المشاهدات
        if not df.empty:
            df = df.sort_values(by="Video Views", ascending=False)
        return df
    except Exception as e:
        return pd.DataFrame()

while True:
    with placeholder.container():
        df_stats = load_category_stats()
        df_videos = load_top_videos() # الداتا الجديدة
        
        if df_stats.empty:
            st.info("⏳ Waiting for data... (Refresh every 5 mins)")
            time.sleep(1)
            continue

        # --- الصف الأول: الرسوم البيانية ---
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("🍩 Market Share by Category")
            base = alt.Chart(df_stats).encode(theta=alt.Theta("Total Views", stack=True))
            pie = base.mark_arc(outerRadius=120, innerRadius=80).encode(
                color=alt.Color("Category"),
                order=alt.Order("Total Views", sort="descending"),
                tooltip=["Category", "Total Views"]
            )
            st.altair_chart(pie, use_container_width=True)

        with col2:
            st.subheader("🔢 Total Views Ranking")
            st.dataframe(
                df_stats,
                column_config={"Total Views": st.column_config.NumberColumn(format="%d 👁️")},
                hide_index=True,
                use_container_width=True
            )
            
        # --- الصف الثاني: الجديد (جدول الفيديوهات التريند) ---
        st.divider()
        st.subheader("🔥 Top Trending Video per Category")
        
        if not df_videos.empty:
            st.dataframe(
                df_videos,
                column_config={
                    "Link": st.column_config.LinkColumn("Watch"), # لينك قابل للضغط
                    "Video Views": st.column_config.ProgressColumn(
                        "Views", 
                        format="%d", 
                        min_value=0, 
                        max_value=int(df_videos['Video Views'].max())
                    )
                },
                hide_index=True,
                use_container_width=True
            )

    time.sleep(5)