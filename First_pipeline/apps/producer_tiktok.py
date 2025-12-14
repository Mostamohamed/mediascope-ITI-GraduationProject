import requests
from bs4 import BeautifulSoup
import time
import re
import json
from urllib.parse import urljoin
from datetime import datetime
from kafka import KafkaProducer

# إعدادات كافكا
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_tiktok_url_from_detail_page(tokchart_sound_url):
    """سحب رابط تيك توك المباشر"""
    try:
        response = requests.get(tokchart_sound_url, timeout=10)
        if response.status_code != 200: return None
    except: return None

    soup = BeautifulSoup(response.content, 'html.parser')
    tiktok_link = soup.find('a', href=re.compile("tiktok\.com/music/"))
    
    if tiktok_link:
        return tiktok_link['href']
    else:
        tiktok_link_text = soup.find('a', string="View on TikTok")
        if tiktok_link_text and 'href' in tiktok_link_text.attrs:
             return tiktok_link_text['href']
        return None


# --- دوال التنظيف الجديدة ---
def clean_ugc(text):
    """تنظيف رقم الـ UGC واختيار الرقم الدقيق"""
    # النص بيجي كده: "1K\n 1,247"
    # هنقسم النص مسافات وناخد آخر جزء لأنه غالباً الرقم الدقيق
    parts = text.split()
    if not parts: return "0"
    return parts[-1] # هيرجع 1,247

# def clean_growth(text):
#     """تنظيف نسبة النمو واختيار النسبة المئوية"""
#     # النص بيجي كده: "+238\n +30.87\n %"
#     parts = text.split()
#     if not parts: return "0%"
    
#     # محاولة تجميع النسبة المئوية لو مفصولة
#     if parts[-1] == '%':
#         return f"{parts[-2]}%" # هيرجع +30.87%
    
#     # لو النسبة لازقة في الرقم
#     for p in parts:
#         if '%' in p: return p
        
#     return parts[-1]

def clean_growth(text):
    """
    سحب رقم النمو اليومي (الرقم الأول) وتجاهل النسبة المئوية
    Input Example: "+6,500\n +8.13 %"
    Output: "+6,500"
    """
    # تقسيم النص بناءً على المسافات والأسطر الجديدة
    parts = text.split()
    
    if not parts: 
        return "0"
    
    # احنا عايزين الرقم اللي فوق (أول واحد في الليست)
    return parts[0]


def scrape_and_send(num_pages=1):
    """سحب الداتا وإرسالها لـ Kafka مباشرة"""
    base_url = 'https://tokchart.com/dashboard/tiktok-trending-sounds/EG'
    
    print(f"🎵 Starting TikTok Scrape for {num_pages} pages...")

    for page_num in range(1, num_pages + 1):
        page_url = f"{base_url}?page={page_num}"
        try:
            response = requests.get(page_url)
            if response.status_code != 200: continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            if not table: continue
            
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 7:
                    sound_cell = cells[1]
                    sound_link = sound_cell.find('a', href=True)
                    
                    if sound_link:
                        relative_url = sound_link['href']
                        tokchart_detail_url = urljoin("https://tokchart.com", relative_url)
                        
                        direct_link = get_tiktok_url_from_detail_page(tokchart_detail_url)
                        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # هنا التغيير: استخدام دوال التنظيف
                        raw_ugc = cells[2].text.strip()
                        raw_growth = cells[3].text.strip()

                        payload = {
                            "rank": cells[0].text.strip(),
                            "sound_name": sound_cell.get_text(strip=True),
                            "ugc_count": clean_ugc(raw_ugc),      # تنظيف الـ UGC
                            "growth": clean_growth(raw_growth),   # تنظيف الـ Growth
                            "author_country": cells[4].text.strip(),
                            "tokchart_url": tokchart_detail_url,
                            "tiktok_direct_url": direct_link if direct_link else "Not Found",
                            "fetch_timestamp": fetch_time
                        }
                        
                        producer.send('tiktok_data', value=payload)
                        print(f"Sent: {payload['sound_name']} | UGC: {payload['ugc_count']} | Growth: {payload['growth']}")
                        
                        time.sleep(0.5)
            
            print(f"✅ Page {page_num} finished.")
            time.sleep(1)

        except Exception as e:
            print(f"❌ Error on page {page_num}: {e}")

# def scrape_and_send(num_pages=5):
#     """سحب الداتا وإرسالها لـ Kafka مباشرة"""
#     base_url = 'https://tokchart.com/dashboard/tiktok-trending-sounds/EG'
    
#     print(f"🎵 Starting TikTok Scrape for {num_pages} pages...")

#     for page_num in range(1, num_pages + 1):
#         page_url = f"{base_url}?page={page_num}"
#         try:
#             response = requests.get(page_url)
#             if response.status_code != 200: continue
            
#             soup = BeautifulSoup(response.content, 'html.parser')
#             table = soup.find('table')
#             if not table: continue
            
#             rows = table.find('tbody').find_all('tr')
            
#             for row in rows:
#                 cells = row.find_all('td')
#                 if len(cells) > 7:
#                     sound_cell = cells[1]
#                     sound_link = sound_cell.find('a', href=True)
                    
#                     if sound_link:
#                         relative_url = sound_link['href']
#                         tokchart_detail_url = urljoin("https://tokchart.com", relative_url)
                        
#                         # سحب الرابط المباشر
#                         direct_link = get_tiktok_url_from_detail_page(tokchart_detail_url)
                        
#                         # وقت السحب (مهم جداً)
#                         fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#                         # تجهيز الداتا
#                         payload = {
#                             "rank": cells[0].text.strip(),
#                             "sound_name": sound_cell.get_text(strip=True),
#                             "ugc_count": cells[2].text.strip(),
#                             "growth": cells[3].text.strip(),
#                             "author_country": cells[4].text.strip(),
#                             "tokchart_url": tokchart_detail_url,
#                             "tiktok_direct_url": direct_link if direct_link else "Not Found",
#                             "fetch_timestamp": fetch_time # الوقت اللي طلبته
#                         }
                        
#                         # إرسال لـ Kafka
#                         producer.send('tiktok_data', value=payload)
#                         print(f"Sent: {payload['sound_name']}")
                        
#                         time.sleep(0.5) # احترام الموقع
            
#             print(f"✅ Page {page_num} finished.")
#             time.sleep(1)

#         except Exception as e:
#             print(f"❌ Error on page {page_num}: {e}")

def run_scheduler():
    while True:
        print(f"⏰ Starting Daily Job at {datetime.now()}")
        
        # شغل السكرابينج (صفحتين كفاية للتجربة)
        scrape_and_send(num_pages=1)
        
        print("💤 Job finished. Sleeping for 24 hours...")
        # هنا هينام يوم كامل (86400 ثانية)
        # لو عايز تجرب، غير الرقم ده لـ 60 (دقيقة)
        time.sleep(86400)

if __name__ == "__main__":
    run_scheduler()