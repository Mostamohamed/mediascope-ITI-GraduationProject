import requests
import json
import time
from collections import defaultdict
from datetime import datetime
from kafka import KafkaProducer

# ====== إعدادات الـ API وكافكا ======
CLIENT_ID = ""
CLIENT_SECRET = ""

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_access_token():
    """توليد توكن جديد"""
    try:
        auth_url = "https://id.twitch.tv/oauth2/token"
        auth_params = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
        resp = requests.post(auth_url, params=auth_params)
        return resp.json()['access_token']
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        return None

def run():
    print("💜 Twitch Producer Started...")
    token = get_access_token()
    
    while True:
        try:
            headers = {
                "Client-ID": CLIENT_ID,
                "Authorization": f"Bearer {token}"
            }
            
            # 1. جلب الـ Streams
            streams_url = "https://api.twitch.tv/helix/streams"
            streams_params = {"first": 100} 
            resp = requests.get(streams_url, headers=headers, params=streams_params)
            
            if resp.status_code == 401: # التوكن خلص
                print("🔄 Refreshing Token...")
                token = get_access_token()
                continue
            
            streams_data = resp.json().get('data', [])
            fetched_at = datetime.utcnow().isoformat()
            
            # 2. جلب تفاصيل الألعاب
            game_ids = list({s['game_id'] for s in streams_data if s['game_id']})
            games_info = {}
            if game_ids:
                games_url = "https://api.twitch.tv/helix/games"
                # Twitch بيسمح بـ 100 id في الريكويست الواحد
                for i in range(0, len(game_ids), 100):
                    chunk = game_ids[i:i+100]
                    g_params = [('id', gid) for gid in chunk] # الطريقة الصح لبعت مصفوفة في الـ params
                    g_resp = requests.get(games_url, headers=headers, params=g_params)
                    for g in g_resp.json().get('data', []):
                        games_info[g['id']] = g

            # 3. حساب المشاهدات لكل لعبة
            viewer_count_per_game = defaultdict(int)
            
            # --- إرسال داتا الاستريمرز (Topic 1) ---
            print(f"📥 Fetched {len(streams_data)} streams at {fetched_at}")
            for stream in streams_data:
                gid = stream.get('game_id')
                viewer_count_per_game[gid] += stream['viewer_count']
                
                # تجهيز الرسالة
                stream_payload = {
                    "user_name": stream['user_name'],
                    "game_name": stream['game_name'],
                    "title": stream['title'],
                    "viewer_count": stream['viewer_count'],
                    "thumbnail_url": stream['thumbnail_url'].replace("{width}x{height}", "320x180"),
                    "fetched_at": fetched_at,
                    "type": "stream"
                }
                producer.send('twitch_streams', value=stream_payload)

            # --- إرسال داتا الألعاب (Topic 2) ---
            # هنا بنبعت الخلاصة (اسم اللعبة، صورتها، مجموع المشاهدات)
            for gid, total_viewers in viewer_count_per_game.items():
                g_info = games_info.get(gid, {})
                game_payload = {
                    "game_name": g_info.get('name', 'Unknown'),
                    "box_art_url": g_info.get('box_art_url', '').replace("{width}x{height}", "285x380"),
                    "total_viewers": total_viewers,
                    "fetched_at": fetched_at,
                    "type": "game"
                }
                producer.send('twitch_games', value=game_payload)

            print("✅ Data sent to BOTH topics. Sleeping 60s...")
            time.sleep(60)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run()

