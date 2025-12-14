import streamlit as st

st.set_page_config(
    page_title="Social Pulse Analytics",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Media Scope")
st.markdown("""
### Welcome to Media Scope! 🎓
This platform aggregates real-time trends from multiple social media**.

#### 👈 Please select a platform from the sidebar to view live analytics:

- **🤖 Reddit:** Trending Posts.
- **▶️ YouTube:** Trending videos & category analysis.
- **💜 Twitch:** Top live streamers & games.
- **🎵 TikTok:** Viral sounds & UGC trends.
""")