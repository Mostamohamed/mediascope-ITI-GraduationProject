# import streamlit as st
# import redis
# import pandas as pd
# import time
# import altair as alt

# # 1. إعدادات الصفحة
# st.set_page_config(
#     page_title="Reddit Live Analytics",
#     page_icon="🤖",
#     layout="wide"
# )

# st.title("🤖 Reddit Live: Top Active Communities")
# st.markdown("Real-time aggregation from Kafka & Spark Stream")

# # 2. الاتصال بـ Redis
# try:
#     # لاحظ: بنستخدم اسم الكونتينر 'redis'
#     r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
# except Exception as e:
#     st.error(f"Failed to connect to Redis: {e}")

# placeholder = st.empty()

# def load_data():
#     try:
#         # بنجيب أعلى 15 صب (Subreddit)
#         # المفتاح ده هو اللي اتفقنا عليه في كود Spark
#         key = "reddit_subreddits_rank"
#         data = r.zrevrange(key, 0, 15, withscores=True)
        
#         df = pd.DataFrame(data, columns=['Subreddit', 'Total Score'])
        
#         if not df.empty:
#             df['Total Score'] = df['Total Score'].astype(int)
        
#         return df
#     except Exception:
#         return pd.DataFrame(columns=['Subreddit', 'Total Score'])

# # 3. حلقة العرض المستمر
# while True:
#     with placeholder.container():
#         df = load_data()

#         # لو مفيش داتا (لسه البروديوسر مشتغلش)
#         if df.empty:
#             st.info("⏳ Waiting for data... (Remember: Reddit Producer runs every 60s)")
#             time.sleep(1)
#             continue

#         # تقسيم الشاشة
#         col1, col2 = st.columns([1, 1.5])
        
#         with col1:
#             st.subheader("🏆 Leaderboard")
#             # عرض الجدول مع شريط التقدم المرئي
#             st.dataframe(
#                 df,
#                 column_config={
#                     "Subreddit": "Community Name",
#                     "Total Score": st.column_config.ProgressColumn(
#                         "Engagement Score",
#                         format="%d 🔥",
#                         min_value=0,
#                         # التعديل هنا: حطينا int() حول القيمة
#                         max_value=int(df['Total Score'].max()) if not df.empty else 100
#                     ),
#                 },
#                 use_container_width=True,
#                 hide_index=True
#             )
            
#         with col2:
#             st.subheader("📊 Engagement Visualization")
            
#             # رسم بياني (Bar Chart)
#             chart = alt.Chart(df).mark_bar(cornerRadius=5).encode(
#                 x=alt.X('Total Score:Q', title='Total Upvotes'),
#                 y=alt.Y('Subreddit:N', sort='-x', title=None),
#                 color=alt.Color('Total Score:Q', scale=alt.Scale(scheme='orangered')),
#                 tooltip=['Subreddit', 'Total Score']
#             ).properties(height=500)
            
#             st.altair_chart(chart, use_container_width=True)
            
#         # عدادات إجمالية
#         total_score = df['Total Score'].sum()
#         active_subs = len(df)
        
#         m1, m2 = st.columns(2)
#         m1.metric("Total Analyzed Score", f"{total_score:,}")
#         m2.metric("Active Communities", active_subs)

#     # تحديث كل ثانية
#     time.sleep(2)


# import streamlit as st
# import pandas as pd
# import redis
# import json
# import time

# # ==========================================
# # 1. الاتصال بـ Redis مباشرة
# # ==========================================
# # تأكد إن الـ host والـ port مظبوطين (الافتراضي localhost:6379)
# try:
#     client = redis.Redis(host='redis', port=6379, decode_responses=True)
# except Exception as e:
#     st.error(f"مش عارف اتصل بـ Redis: {e}")

# st.set_page_config(layout="wide", page_title="Reddit Live")
# st.title("🔥 Reddit Real-Time Dashboard (From Redis)")

# # ==========================================
# # 2. دالة لجلب الداتا وفك الـ JSON
# # ==========================================
# def get_data_from_redis():
#     try:
#         # بنقرا الـ Hash اللي اسمه ظاهر في الصورة عندك
#         # reddit_subreddits_details
#         data = client.hgetall("reddit_subreddits_details")
        
#         rows = []
#         for subreddit_name, json_str in data.items():
#             try:
#                 # فك الـ JSON string اللي جوه الـ Value
#                 # شكل الداتا عندك: {"best_title": "...", "best_score": ...}
#                 details = json.loads(json_str)
                
#                 # ضيف اسم الـ Subreddit للداتا
#                 details['subreddit'] = subreddit_name
#                 rows.append(details)
#             except json.JSONDecodeError:
#                 continue
                
#         return pd.DataFrame(rows)
#     except Exception as e:
#         st.error(f"Error reading Redis: {e}")
#         return pd.DataFrame()

# # ==========================================
# # 3. عرض الداتا
# # ==========================================
# placeholder = st.empty()

# while True:
#     df = get_data_from_redis()
    
#     with placeholder.container():
#         if not df.empty:
#             # ترتيب الداتا حسب الـ best_score لو موجود، أو سيبها زي ما هي
#             if 'best_score' in df.columns:
#                 df['best_score'] = df['best_score'].astype(int)
#                 df = df.sort_values(by='best_score', ascending=False)

#             # عرض الكروت
#             for index, row in df.iterrows():
#                 # تجهيز المتغيرات عشان لو فيه حاجة ناقصة متضربش ايرور
#                 total_score = row.get('best_score', 0)
#                 sub_name = row.get('subreddit', 'Unknown')
                
#                 high_title = row.get('best_title', 'No Title')
#                 high_url = row.get('best_url', '#')
                
#                 # new_title = row.get('new_title', 'No Title')
#                 # new_url = row.get('new_url', '#')

#                 # لغينا الـ columns عشان تاخد العرض كله
#                 st.success(f"**🏆 Highest Voted Post (Score: {total_score:,})**")
#                 st.markdown(f"### {high_title}")
                    
#                 if high_url and high_url != '#':
#                     # زرار شيك يوديك للينك
#                     st.link_button("🔗 View on Reddit", high_url)
                    
#                     # # Newest Post
#                     # with c2:
#                     #     st.success("**🆕 Newest Post**")
#                     #     st.write(f"**Title:** {new_title}")
#                     #     if new_url and new_url != '#':
#                     #         st.markdown(f"[View Post]({new_url})")
#         else:
#             st.warning("⚠️ Waiting for data...")
            
#     # تحديث كل ثانية
#     time.sleep(2)
#     # st.rerun() # في النسخ الحديثة التحديث بيحصل تلقائي مع اللوب، لو معلق شيل الكومنت



# import streamlit as st
# import pandas as pd
# import redis
# import json
# import time

# # ==========================================
# # 1. الاتصال بـ Redis
# # ==========================================
# st.set_page_config(layout="wide", page_title="Top Reddit Posts")
# st.title("🏆 Highest Voted Post per Subreddit")

# # ضبط الاتصال (تأكد من الهوست حسب بيئتك: localhost أو redis)
# try:
#     client = redis.Redis(host='redis', port=6379, decode_responses=True)
#     # لو شغال دوكر ممكن تحتاج: host='redis'
# except Exception as e:
#     st.error(f"Redis Connection Error: {e}")

# # ==========================================
# # 2. دالة جلب البيانات
# # ==========================================
# def get_data():
#     try:
#         # بنجيب كل الداتا المتخزنة في الهاش
#         raw_data = client.hgetall("reddit_subreddits_details")
        
#         parsed_rows = []
#         for subreddit, json_str in raw_data.items():
#             try:
#                 # فك التشفير من JSON لـ Python Dict
#                 details = json.loads(json_str)
                
#                 # بنركز بس على بيانات الـ Best/Highest
#                 row = {
#                     "subreddit": subreddit,
#                     "title": details.get("best_title", "No Title"),
#                     "score": int(details.get("best_score", 0)),
#                     "url": details.get("best_url", "#")
#                 }
#                 parsed_rows.append(row)
#             except:
#                 continue
                
#         return pd.DataFrame(parsed_rows)
#     except Exception as e:
#         return pd.DataFrame()

# # ==========================================
# # 3. عرض البيانات (Highest Post Only)
# # ==========================================
# placeholder = st.empty()

# while True:
#     df = get_data()
    
#     with placeholder.container():
#         if not df.empty:
#             # 1. ترتيب الـ Subreddits حسب السكور من الأعلى للأقل
#             df = df.sort_values(by="score", ascending=False)
            
#             # 2. اللوب ده هو اللي بيعرض "كل" الـ Subreddits
#             for index, row in df.iterrows():
                
#                 # تصميم الكارت
#                 with st.expander(f"🔥 r/{row['subreddit']} (Top Score: {row['score']:,})", expanded=True):
                    
#                     st.markdown(f"### {row['title']}")
#                     st.caption(f"**Score:** {row['score']:,} ⬆️")
                    
#                     if row['url'] and row['url'] != "#":
#                         st.link_button("🔗 View Post on Reddit", row['url'])
                        
#                         # لو اللينك صورة، اعرضها جوه الداشبورد (اختياري)
#                         if row['url'].endswith(('.jpg', '.png', '.jpeg', '.gif')):
#                             st.image(row['url'], use_container_width=True)

#         else:
#             st.warning("⏳ Waiting for data in Redis key: 'reddit_subreddits_details'...")
            
#     time.sleep(2)


import streamlit as st
import pandas as pd
import redis
import json
import time

st.set_page_config(layout="wide", page_title="Reddit Grid View")
st.title("🏆 Top Reddit Posts (Grid View)")

# ==========================================
# 1. الاتصال بـ Redis
# ==========================================
try:
    # لو انت Docker استخدم 'redis'، لو local استخدم 'localhost'
    client = redis.Redis(host='redis', port=6379, decode_responses=True)
except Exception as e:
    st.error(f"Redis Connection Error: {e}")

# ==========================================
# 2. دالة جلب البيانات
# ==========================================
def get_data():
    try:
        raw_data = client.hgetall("reddit_subreddits_details")
        parsed_rows = []
        for subreddit, json_str in raw_data.items():
            try:
                details = json.loads(json_str)
                row = {
                    "subreddit": subreddit,
                    "title": details.get("best_title", "No Title"),
                    "score": int(details.get("best_score", 0)),
                    "url": details.get("best_url", "#")
                }
                parsed_rows.append(row)
            except:
                continue
        return pd.DataFrame(parsed_rows)
    except:
        return pd.DataFrame()

# ==========================================
# 3. عرض البيانات (Grid Layout)
# ==========================================
placeholder = st.empty()

while True:
    df = get_data()
    
    with placeholder.container():
        if not df.empty:
            df = df.sort_values(by="score", ascending=False)
            
            # تحويل الداتا لقائمة عشان نعرف نقسمها
            data_list = df.to_dict('records')
            
            # --- اللوجيك الجديد: كل لفة بناخد بوستين ---
            # الرقم 2 هنا هو عدد الأعمدة، لو عايز 3 غيره لـ 3
            COLS_PER_ROW = 1
            
            for i in range(0, len(data_list), COLS_PER_ROW):
                # بنعمل صف جديد فيه عمودين
                cols = st.columns(COLS_PER_ROW)
                
                # بناخد الشريحة (Batch) اللي عليها الدور
                batch = data_list[i : i + COLS_PER_ROW]
                
                # بنلف جوه الشريحة عشان نحط كل واحد في عموده
                for j, row in enumerate(batch):
                    with cols[j]:
                        # تصميم الكارت
                        with st.expander(f"🔥 r/{row['subreddit']}", expanded=True):
                            st.metric("Score", f"{row['score']:,}")
                            st.markdown(f"**{row['title']}**")
                            
                            if row['url'] and row['url'] != "#":
                                st.link_button("🔗 View Post", row['url'])
                                # لو عايز تعرض الصورة
                                if row['url'].endswith(('.jpg', '.png', '.jpeg')):
                                    st.image(row['url'], use_container_width=True)
        else:
            st.warning("Waiting for data...")
            
    time.sleep(2)