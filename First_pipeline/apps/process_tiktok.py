from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
import redis
import json

# Redis Config
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_KEY = "tiktok_trends_eg"

def write_to_redis(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline()
        

        
        for _, row in pdf.iterrows():
            try:
                rank_score = int(row['rank'].replace('#', ''))
            except:
                rank_score = 999

            val = {
                "name": row['sound_name'],
                "ugc": row['ugc_count'],
                "growth": row['growth'],
                "link": row['tiktok_direct_url'],
                "fetched_at": row['fetch_timestamp']
            }
            
            # ZADD: لو الأغنية موجودة هيحدثها، لو مش موجودة هيضيفها
            pipe.zadd(REDIS_KEY, {json.dumps(val): rank_score})
            
        # بنخلي الداتا تنتهي صلاحيتها بعد 24 ساعة (عشان بكرة يبدأ على نظافة)
        pipe.expire(REDIS_KEY, 86400)
        
        pipe.execute()
        print(f"Batch {epoch_id}: Added/Updated {len(pdf)} songs to Redis.")

# Spark Session
spark = SparkSession.builder \
    .appName("TikTokTrends") \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("rank", StringType()),
    StructField("sound_name", StringType()),
    StructField("ugc_count", StringType()),
    StructField("growth", StringType()),
    StructField("author_country", StringType()),
    StructField("tokchart_url", StringType()),
    StructField("tiktok_direct_url", StringType()),
    StructField("fetch_timestamp", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "tiktok_data") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_redis) \
    .start()

query.awaitTermination()