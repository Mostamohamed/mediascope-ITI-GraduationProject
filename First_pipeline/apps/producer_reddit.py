import time
import json
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

# إعدادات كافكا
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

URL = "https://www.reddit.com/r/all/hot.json?limit=50" # قللنا الليمت شوية عشان السرعة
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

def run():
    print("🚀 Reddit Producer Started...")
    
    while True:
        try:
            # 1. سحب الداتا من Reddit
            response = requests.get(URL, headers=HEADERS)
            
            if response.status_code == 200:
                children = response.json()["data"]["children"]
                
                # وقت السحب الحالي (Ingestion Time)
                # بنسجله مرة واحدة للباتش دي كلها
                ingestion_time = datetime.now(timezone.utc).isoformat()
                
                print(f"📥 Fetched {len(children)} posts from Reddit at {ingestion_time}")

                for post in children:
                    d = post["data"]
                    
                    # تحويل وقت البوست لتنسيق مقروء
                    post_time = datetime.fromtimestamp(d["created_utc"], tz=timezone.utc).isoformat()
                    
                    # تجهيز الرسالة
                    payload = {
                        "subreddit": d["subreddit"],
                        "score": d["score"],          # عدد الفوتس
                        "num_comments": d["num_comments"], # ضفتلك عدد الكومنتات كمان، مهم جداً
                        "title": d["title"],
                        "url": d["url"],              # لينك البوست
                        "post_timestamp": post_time,  # وقت نشر البوست
                        "ingestion_timestamp": ingestion_time # وقت سحبنا للداتا
                    }
                    
                    # إرسال لـ Kafka Topic اسمه 'reddit_data'
                    producer.send('reddit_data', value=payload)
                
                print("✅ Data sent to Kafka. Sleeping for 60 seconds...")
            
            elif response.status_code == 429:
                print("⚠️ Rate Limited by Reddit. Waiting longer...")
                time.sleep(120)
                continue
            else:
                print(f"❌ Error: {response.status_code}")

            # استنى دقيقة قبل السحب الجاي
            time.sleep(60)

        except Exception as e:
            print(f"❌ Connection Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run()

