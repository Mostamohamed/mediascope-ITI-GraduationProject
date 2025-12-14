import json
import time
import uuid  # مكتبة لإنشاء معرفات فريدة لمنع تكرار أسماء الملفات
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
from collections import defaultdict

# 1. إعدادات الاتصال
# نستخدم المنفذ 29092 لأنه المنفذ الداخلي لشبكة الدوكر
KAFKA_SERVER = 'kafka:29092'
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'

# قائمة المواضيع التي سيتم سحبها
TOPICS = ['reddit_data', 'youtube_data', 'twitch_streams', 'tiktok_data']

def get_hdfs_client():
    print("⏳ Connecting to HDFS...")
    while True:
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.list('/') # اختبار سريع للاتصال
            print("✅ Connected to HDFS NameNode!")
            return client
        except Exception as e:
            print(f"⚠️ HDFS not ready... retrying. ({e})")
            time.sleep(5)

def save_to_hdfs(client, topic, data_list):
    if not data_list: return
    try:
        now = datetime.now()
        # مسار التخزين: يتم تقسيم البيانات حسب التوبيك ثم التاريخ (سنة/شهر/يوم)
        # مثال: /datalake/youtube_data/2025/12/04
        folder_path = f"/datalake/{topic}/{now.year}/{now.month:02d}/{now.day:02d}"
        
        # ==================================================================
        # الحل الجذري لمشكلة (File already exists)
        # نضيف كود عشوائي (UUID) لاسم الملف لضمان عدم تكراره أبداً
        # ==================================================================
        unique_id = uuid.uuid4().hex[:8]
        file_name = f"{topic}_{int(time.time())}_{unique_id}.json"
        
        full_path = f"{folder_path}/{file_name}"
        
        # تحويل قائمة البيانات إلى نص JSON (كل صف في سطر منفصل)
        json_content = "\n".join([json.dumps(record) for record in data_list])
        
        # الكتابة الفعلية في HDFS
        with client.write(full_path, encoding='utf-8') as writer:
            writer.write(json_content)
            
        print(f"💾 [HDFS] Saved {len(data_list)} records to: {full_path}")
        
    except Exception as e:
        print(f"❌ Write Error: {e}")

def run():
    print("🚀 Starting Permanent Data Warehouse Ingestion...")
    
    # انتظار بسيط لضمان أن الخدمات الأخرى تعمل
    time.sleep(5)
    
    hdfs_client = get_hdfs_client()
    
    print(f"🎧 Connecting to Kafka at{KAFKA_SERVER}...")
    
    # إعداد المستهلك (Consumer)
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_SERVER],
        # 1. جلب البيانات من البداية (Earliest) إذا لم يكن هناك سجل سابق
        auto_offset_reset='earliest', 
        # 2. تفعيل الحفظ التلقائي للمكان الذي توقفنا عنده
        enable_auto_commit=True, 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # 3. اسم مجموعة جديد لضمان سحب كل البيانات القديمة والجديدة
        group_id='social_pulse_warehouse_final_v6' 
    )

    # مخزن مؤقت (Buffer) لكل توبيك على حدة
    buffers = defaultdict(list)
    
    # حجم الدفعة: يتم الحفظ كل 10 رسائل (لتحقيق توازن بين السرعة والأداء)
    BATCH_SIZE = 10 
    
    print(f"🎧 Connected to Kafka. Monitoring Topics: {TOPICS}")
    
    for message in consumer:
        data = message.value
        topic = message.topic
        
        # إضافة توقيت الأرشفة للبيانات
        data['ingested_at'] = datetime.now().isoformat()
        
        # إضافة الرسالة للمخزن المؤقت الخاص بالتوبيك
        buffers[topic].append(data)
        print(f"📥 [{topic}] received msg...")
        # إذا امتلأ المخزن المؤقت لهذا التوبيك، قم بالحفظ وتفريغه
        if len(buffers[topic]) >= BATCH_SIZE:
            save_to_hdfs(hdfs_client, topic, buffers[topic])
            buffers[topic] = [] 

if __name__ == "__main__":
    run()