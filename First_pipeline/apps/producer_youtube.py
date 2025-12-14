import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

# 1. إعدادات API وكافكا
API_KEY = "AIzaSyAKJKObE_IJ8kZWSSzKq5eCIpIVOeBET7U" # المفتاح بتاعك
REGION = "EG"
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_category_map():
    """هات أسماء الكاتيجوريز مرة واحدة في البداية"""
    try:
        url = f"https://www.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode={REGION}&key={API_KEY}"
        response = requests.get(url)
        data = response.json()
        return {item["id"]: item["snippet"]["title"] for item in data.get("items", [])}
    except Exception as e:
        print(f"⚠️ Failed to fetch categories: {e}")
        return {}

def run():
    print("🚀 YouTube Producer Started...")
    
    # حملنا الخريطة مرة واحدة
    category_map = get_category_map()
    print(f"✅ Loaded {len(category_map)} categories.")

    while True:
        try:
            # 2. سحب التريند
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={REGION}&maxResults=50&key={API_KEY}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                ingestion_time = datetime.now().isoformat()
                
                print(f"📥 Fetched {len(items)} trending videos at {ingestion_time}")

                for video in items:
                    snippet = video["snippet"]
                    stats = video.get("statistics", {})

                    # تنظيف وتجهيز الداتا
                    payload = {
                        "video_id": video["id"],
                        "title": snippet["title"],
                        "channel": snippet["channelTitle"],
                        "views": int(stats.get("viewCount", 0)),
                        "likes": int(stats.get("likeCount", 0)), # زودنا اللايكات
                        "category": category_map.get(snippet.get("categoryId"), "Other"),
                        "published_at": snippet["publishedAt"],
                        "video_url": f"https://www.youtube.com/watch?v={video['id']}",
                        "ingestion_timestamp": ingestion_time
                    }
                    
                    # إرسال لـ Kafka
                    producer.send('youtube_data', value=payload)

                print("✅ Data sent to Kafka. Sleeping for 5 minutes (to save API Quota)...")
                # بننام 5 دقايق عشان الكوتة
                time.sleep(3600) 
            
            else:
                print(f"❌ API Error: {response.status_code} - {response.text}")
                time.sleep(60)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run()
