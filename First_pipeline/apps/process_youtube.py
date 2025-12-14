from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType, StructField
import redis
import json

# إعدادات Redis
REDIS_HOST = 'redis'
REDIS_PORT = 6379
# مفتاحين: واحد للمجموع، وواحد لتفاصيل الفيديو التريند
REDIS_KEY_VIEWS = "youtube_category_views"
REDIS_KEY_TOP_VIDEOS = "youtube_category_top_video"

def update_redis(df, epoch_id):
    pdf = df.toPandas()
    
    if not pdf.empty:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline()
        
        # 1. تحديث مجموع المشاهدات لكل كاتيجوري (الكود القديم)
        # بنجمع باستخدام Pandas عشان الدقة
        category_sums = pdf.groupby('category')['views'].sum().reset_index()
        
        # # مسح القديم للتحديث
        # r.delete(REDIS_KEY_VIEWS)
        for _, row in category_sums.iterrows():
            pipe.zadd(REDIS_KEY_VIEWS, {row['category']: row['views']})
            
        # 2. (الجديد) تحديد الفيديو التريند لكل كاتيجوري
        # بنرتبهم تنازلي حسب المشاهدات، وناخد أول واحد في كل كاتيجوري
        top_videos = pdf.sort_values('views', ascending=False).drop_duplicates(['category'])
        
        # هنخزنهم في Hash Map: المفتاح هو الكاتيجوري، والقيمة هي تفاصيل الفيديو JSON
        for _, row in top_videos.iterrows():
            video_details = {
                "title": row['title'],
                "channel": row['channel'],
                "views": row['views'],
                "url": row['video_url']
            }
            # hset: بيحدث قيمة مفتاح معين جوه الهاش
            pipe.hset(REDIS_KEY_TOP_VIDEOS, row['category'], json.dumps(video_details))
        
        # --- إضافة مهمة ---
        # ضبط صلاحية البيانات لتختفي بعد 24 ساعة (86400 ثانية)
        # عشان لو بطلنا نبعت داتا، الداشبورد تفضى تاني يوم
        pipe.expire(REDIS_KEY_VIEWS, 86400)
        pipe.expire(REDIS_KEY_TOP_VIDEOS, 86400)
            
        pipe.execute()
        print(f"Batch {epoch_id}: Updated Stats & Top Videos.")

# --- باقي الكود زي ما هو ---
spark = SparkSession.builder \
    .appName("YouTubeTrendsProcessor") \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("video_id", StringType()),
    StructField("title", StringType()),
    StructField("channel", StringType()),
    StructField("views", LongType()),
    StructField("likes", LongType()),
    StructField("category", StringType()),
    StructField("published_at", StringType()),
    StructField("video_url", StringType()),
    StructField("ingestion_timestamp", StringType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "youtube_data") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# لاحظ: شلنا الـ Aggregation من هنا وخليناه جوه الـ Pandas
# عشان نعرف نوصل لتفاصيل الفيديو (العنوان والقناة)
query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(update_redis) \
    .start()

query.awaitTermination()