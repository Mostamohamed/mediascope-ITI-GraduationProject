import streamlit as st
import redis
import pandas as pd
import json
import time
import altair as alt

st.set_page_config(page_title="Twitch Analytics", page_icon="💜", layout="wide")
st.title("💜 Twitch Live: Top Games & Streamers")

try:
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
except:
    st.error("Redis connection failed")

placeholder = st.empty()

def load_streamers():
    try:
        rank_data = r.zrevrange("twitch_streamers_rank", 0, 9, withscores=True)
        details_map = r.hgetall("twitch_streamers_details")
        rows = []
        for user, views in rank_data:
            det = json.loads(details_map.get(user, "{}"))
            rows.append({
                "Streamer": user,
                "Views": int(views),
                "Game": det.get('game', '-'),
                "Title": det.get('title', '-'),
                "Thumbnail": det.get('thumb', '')
            })
        return pd.DataFrame(rows)
    except: return pd.DataFrame()

def load_games():
    try:
        rank_data = r.zrevrange("twitch_games_rank", 0, 9, withscores=True)
        details_map = r.hgetall("twitch_games_details")
        rows = []
        for game, views in rank_data:
            det = json.loads(details_map.get(game, "{}"))
            rows.append({
                "Game": game,
                "Total Views": int(views),
                "Box Art": det.get('img', '')
            })
        return pd.DataFrame(rows)
    except: return pd.DataFrame()

while True:
    with placeholder.container():
        df_streamers = load_streamers()
        df_games = load_games()

        if df_streamers.empty or df_games.empty:
            st.info("⏳ Waiting for Twitch data... (Updates every 60s)")
            time.sleep(1)
            continue

        tab1, tab2 = st.tabs(["🎮 Top Games", "📹 Top Streamers"])

        with tab1:
            col1, col2 = st.columns([2, 1])
            with col1:
                # رسم بياني للألعاب
                chart = alt.Chart(df_games).mark_bar().encode(
                    x=alt.X('Total Views', title='Viewers'),
                    y=alt.Y('Game', sort='-x'),
                    color=alt.Color('Total Views', scale=alt.Scale(scheme='purpleblue')),
                    tooltip=['Game', 'Total Views']
                ).properties(height=400)
                st.altair_chart(chart, use_container_width=True)
            
            with col2:
                # عرض صور الألعاب
                st.subheader("Box Art")
                for _, row in df_games.head(3).iterrows():
                    st.image(row['Box Art'], caption=f"{row['Game']} ({row['Total Views']})", width=100)

        with tab2:
            st.subheader("🔥 Live Channels Now")
            # عرض الاستريمرز في جدول مع صور
            st.dataframe(
                df_streamers,
                column_config={
                    "Thumbnail": st.column_config.ImageColumn("Preview"),
                    "Views": st.column_config.ProgressColumn(
                        "Viewers", format="%d", min_value=0, max_value=int(df_streamers['Views'].max())
                    )
                },
                use_container_width=True
            )

    time.sleep(5)