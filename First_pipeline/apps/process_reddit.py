# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, sum as _sum
# from pyspark.sql.types import StructType, StringType, LongType, IntegerType, StructField
# import redis

# # إعدادات Redis
# REDIS_HOST = 'redis'
# REDIS_PORT = 6379
# REDIS_KEY = "reddit_subreddits_rank"


# def update_redis(df, epoch_id):
#     # بنحول النتيجة لـ Pandas
#     pdf = df.toPandas()
    
#     if not pdf.empty:
#         r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
#         pipe = r.pipeline()
        
#         # بنفضي الجدول القديم عشان نعرض حالة "اللحظة دي" بس
#         # لو عايز تراكمي (تاريخي) شيل السطر ده
#         r.delete(REDIS_KEY) 
        
#         for index, row in pdf.iterrows():
#             subreddit = row['subreddit']
#             total_score = row['total_score']
            
#             # بنخزن في Sorted Set
#             pipe.zadd(REDIS_KEY, {subreddit: total_score})
            
#         pipe.execute()
#         print(f"Batch {epoch_id}: Updated Reddit Rankings for {len(pdf)} subreddits.")

# # 1. إعداد Spark
# spark = SparkSession.builder \
#     .appName("RedditTrendsProcessor") \
#     .master("spark://spark-master:7077") \
#     .config("spark.cores.max", "1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # 2. تعريف الـ Schema (لازم تطابق الـ Producer)
# schema = StructType([
#     StructField("subreddit", StringType()),
#     StructField("score", LongType()),
#     StructField("num_comments", IntegerType()),
#     StructField("title", StringType()),
#     StructField("url", StringType()),
#     StructField("post_timestamp", StringType()),
#     StructField("ingestion_timestamp", StringType())
# ])

# # 3. قراءة من Kafka
# raw_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "reddit_data") \
#     .option("startingOffsets", "latest") \
#     .load()

# # 4. تنظيف الداتا
# parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # 5. المعالجة (Business Logic)
# # هنجمع الـ Score لكل Subreddit
# # يعني لو فيه 5 بوستات من r/funny، هنجمع الفوتس بتاعتهم كلهم
# agg_df = parsed_df.groupBy("subreddit") \
#     .agg(_sum("score").alias("total_score")) \
#     .orderBy(col("total_score").desc())

# # 6. الكتابة لـ Redis
# query = agg_df.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(update_redis) \
#     .start()

# query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, sum, max, struct
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# import os
# import shutil

# # تنظيف الفولدرات القديمة عشان نبدأ على نظافة (اختياري)
# # try:
# #     shutil.rmtree("data/reddit_output")
# #     shutil.rmtree("/tmp/reddit_checkpoint")
# # except:
# #     pass

# spark = SparkSession.builder \
#     .appName("RedditAggregatorBackend") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# # تعريف السكيما
# schema = StructType([
#     StructField("subreddit", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("score", IntegerType(), True),
#     StructField("url", StringType(), True),
#     StructField("created_utc", DoubleType(), True)
# ])

# # قراءة من Kafka
# df_kafka = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "reddit_data") \
#     .option("startingOffsets", "earliest") \
#     .load()

# df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# # المعالجة (Aggregation)
# aggregated_df = df_parsed.groupBy("subreddit").agg(
#     sum("score").alias("total_score"),
#     max(struct(col("score"), col("title"), col("url"))).alias("top_post"),
#     max(struct(col("created_utc"), col("title"), col("url"))).alias("newest_post")
# )

# final_df = aggregated_df.select(
#     col("subreddit"),
#     col("total_score"),
#     col("top_post.title").alias("high_title"),
#     col("top_post.score").alias("high_score"),
#     col("top_post.url").alias("high_url"),
#     col("newest_post.title").alias("new_title"),
#     col("newest_post.url").alias("new_url")
# )

# # --- دالة للكتابة بطريقة Overwrite ---
# def write_batch_to_parquet(batch_df, batch_id):
#     # بنكتب الداتا الحالية ونمسح القديم (Overwrite)
#     # ده بيخلي الداشبورد دايماً شايفة أحدث أرقام بس
#     batch_df.write \
#         .mode("overwrite") \
#         .parquet("data/reddit_output")

# print("🚀 Spark Job Started... Writing live updates to data/reddit_output")

# # استخدام foreachBatch هو الحل
# query = final_df.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(write_batch_to_parquet) \
#     .option("checkpointLocation", "/tmp/reddit_checkpoint_v2") \
#     .trigger(processingTime="5 seconds") \
#     .start()

# query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, sum as _sum
# from pyspark.sql.types import StructType, StringType, LongType, IntegerType, StructField
# import redis

# # إعدادات Redis
# REDIS_HOST = 'redis'
# REDIS_PORT = 6379
# REDIS_KEY = "reddit_subreddits_rank"

# def update_redis(df, epoch_id):
#     pdf = df.toPandas()
#     if not pdf.empty:
#         try:
#             r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
#             pipe = r.pipeline()
#             # r.delete(REDIS_KEY) # اختياري: لو عايز تمسح القديم كل مرة
#             for index, row in pdf.iterrows():
#                 pipe.zadd(REDIS_KEY, {row['subreddit']: row['total_score']})
#             pipe.execute()
#             print(f"✅ Batch {epoch_id}: Updated Redis with {len(pdf)} subreddits.")
#         except Exception as e:
#             print(f"❌ Redis Error: {e}")

# spark = SparkSession.builder \
#     .appName("RedditTrendsProcessor") \
#     .master("spark://spark-master:7077") \
#     .config("spark.cores.max", "1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # نفس هيكل الداتا اللي في Producer
# schema = StructType([
#     StructField("subreddit", StringType()),
#     StructField("score", LongType()),
#     StructField("num_comments", IntegerType()),
#     StructField("title", StringType()),
#     StructField("url", StringType()),
#     StructField("post_timestamp", StringType()),
#     StructField("ingestion_timestamp", StringType())
# ])

# # هنا التصحيح المهم: kafka:29092 واسم التوبيك reddit_data
# raw_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "reddit_data") \
#     .option("startingOffsets", "earliest") \
#     .load()

# parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# agg_df = parsed_df.groupBy("subreddit") \
#     .agg(_sum("score").alias("total_score")) \
#     .orderBy(col("total_score").desc())

# query = agg_df.writeStream \
#     .outputMode("complete") \
#     .foreachBatch(update_redis) \
#     .start()

# query.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, max, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import redis

# ==========================================
# 1. إعدادات الاتصال
# ==========================================
# لو شغال Docker، استخدم "kafka:29092" و "redis"
# لو شغال Local، استخدم "localhost:9092" و "localhost"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092" 
REDIS_HOST = "redis"
REDIS_PORT = 6379

# ==========================================
# 2. تشغيل Spark Session
# ==========================================
spark = SparkSession.builder \
    .appName("RedditSparkProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==========================================
# 3. تعريف السكيما (نفس شكل الداتا اللي جاية من البروديوسر)
# ==========================================
# البروديوسر بيبعت: subreddit, score, num_comments, title, url, post_timestamp
schema = StructType([
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("post_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])

# ==========================================
# 4. قراءة الداتا من Kafka
# ==========================================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "reddit_data") \
    .option("startingOffsets", "earliest") \
    .load()

# فك الـ JSON وتحويله لأعمدة
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# ==========================================
# 5. المعالجة (المنطق الرئيسي)
# ==========================================
# عايزين لكل subreddit نطلع حاجتين:
# 1. أعلى بوست (بناءً على score)
# 2. أجدد بوست (بناءً على post_timestamp)

aggregated_df = df_parsed.groupBy("subreddit").agg(
    # بنعمل struct يربط السكور بباقي التفاصيل عشان لما ناخد الماكس ناخدهم معاه
    max(struct(col("score"), col("title"), col("url"))).alias("best_post_data"),
    
    # ونفس الكلام للوقت
    max(struct(col("post_timestamp"), col("title"), col("url"))).alias("new_post_data")
)

# تجهيز الشكل النهائي للـ Redis (JSON String)
# الداشبورد مستنية مفاتيح اسمها: best_title, best_score, best_url
final_df = aggregated_df.select(
    col("subreddit"),
    to_json(struct(
        col("best_post_data.title").alias("best_title"),
        col("best_post_data.score").alias("best_score"),
        col("best_post_data.url").alias("best_url"),
        # col("new_post_data.title").alias("new_title"),
        # col("new_post_data.url").alias("new_url")
    )).alias("json_value") # ده اللي هيتخزن جوه الريديس
)

# ==========================================
# 6. الكتابة في Redis
# ==========================================
def write_to_redis(batch_df, batch_id):
    # بنحول الباتش لـ Pandas عشان نكتبه بسرعة في Redis (أسهل طريقة للتعامل مع الـ Hashes)
    # ملاحظة: لو الداتا ضخمة جداً يفضل استخدام foreachPartition، بس هنا الداتا ملمومة
    data = batch_df.collect()
    
    if data:
        try:
            # فتح اتصال مع Redis
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            
            # الكتابة داخل Pipeline لتحسين الأداء
            pipe = r.pipeline()
            
            for row in data:
                # Key: reddit_subreddits_details
                # Field: اسم الـ Subreddit
                # Value: الـ JSON اللي جهزناه
                pipe.hset("reddit_subreddits_details", row['subreddit'], row['json_value'])
            
            pipe.execute()
            print(f"✅ Batch {batch_id}: Updated {len(data)} subreddits in Redis.")
        except Exception as e:
            print(f"❌ Error writing to Redis: {e}")

# تشغيل الـ Query
query = final_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()