from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, md5, lower, trim, date_format, concat, coalesce, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# 1. إعداد Spark Session
spark = SparkSession.builder \
    .appName("DEBUG_VERSION_TEST_123") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# ---------------------------------------------------------
# 2. إعدادات Snowflake
# ---------------------------------------------------------
sf_options = {
    "sfUrl": "VPWKLWI-IEB69001.snowflakecomputing.com",
    "sfUser": "YOUSSEFATALLAH",
    "sfPassword": "Youssef123456789",
    "sfDatabase": "SOCIAL_TRENDS",
    "sfSchema": "GALAXY_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "encoding": "utf-8",        # إجبار الكونيكتور على استخدام UTF-8
    "sfCompress": "off"
}

def write_batch_to_snowflake(df, table_name):
    if df.count() > 0:
        print(f"--> Writing {df.count()} new rows to {table_name}...")
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()

# ==============================================================================
# تعريف SKs ثابتة للمنصات (عشان نضمن انها متعمدش على الاسم بس تفضل ثابتة)
# ==============================================================================
# هنا استخدمنا UUIDs عشوائية بس ثبتناها في الكود عشان التناسق
PLATFORM_UUIDS = {
    "Reddit":  "550e8400-e29b-41d4-a716-446655440000",
    "TikTok":  "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "YouTube": "7d444840-9dc0-11d1-b245-5ffdce74fad2",
    "Twitch":  "2c5ea4c0-4067-11e9-8bad-9b1deb4d9857"
}

# ==============================================================================
# 0. تهيئة DIM_PLATFORM
# ==============================================================================
def initialize_dim_platform():
    print("--- Initializing Dim_Platform with UUIDs ---")
    
    # لاحظ هنا الـ SK مكتوب يدوياً (Random) ومش معتمد على الـ Hash
    platforms_data = [
        (PLATFORM_UUIDS["Reddit"], "Reddit"), 
        (PLATFORM_UUIDS["TikTok"], "TikTok"), 
        (PLATFORM_UUIDS["YouTube"], "YouTube"), 
        (PLATFORM_UUIDS["Twitch"], "Twitch")
    ]
    
    schema = StructType([
        StructField("platform_sk", StringType(), False),
        StructField("platform_name", StringType(), True)
    ])
    
    df_platform = spark.createDataFrame(platforms_data, schema)
    
    df_platform.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", "Dim_Platform") \
        .mode("ignore") \
        .save()

initialize_dim_platform()

# ==============================================================================
# 1. معالجة REDDIT (UUID Implementation)
# ==============================================================================
reddit_schema = spark.read.json("hdfs://namenode:9000/datalake/reddit_data/2025/*/*/*.json").schema

def process_reddit_batch(df, batch_id):
    print(f"--- Processing New Reddit Files (Batch: {batch_id}) ---")
    df.cache()

    # خطوة 1: تجهيز الـ SK للـ Dimensions (لازم نعمل Distinct عشان نوحد الـ ID لنفس السب-ريديت جوه الباتش)
    # بنعمل Normalization للاسم (lower/trim) عشان نضمن التجميع الصح
    subreddit_dim_df = df.select(lower(trim(col("subreddit"))).alias("join_key_subreddit"), col("subreddit").alias("original_name")) \
                         .dropDuplicates(["join_key_subreddit"]) \
                         .withColumn("subreddit_sk", expr("uuid()")) # هنا بنخلق كود عشوائي

    # خطوة 2: دمج الـ SK مع الداتا الأصلية
    # بنعمل Broadcast Join لأن عدد السب-ريديتس في الباتش الواحد غالباً صغير
    df_joined = df.withColumn("join_key_subreddit", lower(trim(col("subreddit")))) \
                  .join(subreddit_dim_df, "join_key_subreddit", "left")

    # خطوة 3: التحويل النهائي
    reddit_transformed = df_joined.select(
        col("subreddit_sk"),
        expr("uuid()").alias("post_sk"), # الـ Post هو الحدث نفسه، فممكن ياخد UUID مباشر لكل سطر
        lit(PLATFORM_UUIDS["Reddit"]).alias("platform_sk"),
        date_format(col("ingested_at"), "yyyyMMdd").cast(IntegerType()).alias("ingested_date_sk"),
        date_format(col("ingested_at"), "HHmm").cast(StringType()).alias("ingested_time_sk"),
        col("original_name").alias("subreddit_name"),
        col("title").alias("post_title"),
        col("url").alias("post_url"),
        col("post_timestamp"),
        col("score"),
        col("num_comments"),
        col("ingested_at")
    )

    # التوزيع على الجداول
    write_batch_to_snowflake(reddit_transformed.select("subreddit_sk", "subreddit_name").dropDuplicates(["subreddit_sk"]), "Dim_Subreddit")
    
    write_batch_to_snowflake(reddit_transformed.select(
        col("post_sk"), col("post_title").alias("title"), col("post_url").alias("url"), col("post_timestamp")
    ).dropDuplicates(["post_sk"]), "Dim_Post")

    write_batch_to_snowflake(reddit_transformed.select(
        "post_sk", "subreddit_sk", "platform_sk", "ingested_date_sk", "ingested_time_sk",
        "score", "num_comments", "ingested_at"
    ), "Fact_Redit_Activity")
    
    df.unpersist()

spark.readStream \
    .schema(reddit_schema) \
    .option("encoding", "UTF-8") \
    .json("hdfs://namenode:9000/datalake/reddit_data/2025/*/*/*.json") \
    .writeStream \
    .foreachBatch(process_reddit_batch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/reddit_job_uuid") \
    .trigger(availableNow=True) \
    .start() \
    .awaitTermination()


# ==============================================================================
# 2. معالجة TIKTOK (UUID Implementation)
# ==============================================================================
tiktok_schema = spark.read.json("hdfs://namenode:9000/datalake/tiktok_data/2025/*/*/*.json").schema

def process_tiktok_batch(df, batch_id):
    print(f"--- Processing New TikTok Files (Batch: {batch_id}) ---")
    df.cache()

    # تجهيز SKs للـ Sounds والـ Countries
    sound_dim = df.select("sound_name").distinct().withColumn("sound_sk", expr("uuid()"))
    country_dim = df.select("author_country").distinct().withColumn("country_sk", expr("uuid()"))

    # عمل Joins
    df_joined = df.join(sound_dim, "sound_name", "left") \
                  .join(country_dim, "author_country", "left")

    tiktok_transformed = df_joined.select(
        col("sound_sk"),
        col("country_sk"),
        lit(PLATFORM_UUIDS["TikTok"]).alias("platform_sk"),
        date_format(col("ingested_at"), "yyyyMMdd").cast(IntegerType()).alias("ingested_date_sk"),
        date_format(col("ingested_at"), "HHmm").cast(StringType()).alias("ingested_time_sk"),
        col("sound_name"),
        col("author_country").alias("country_code_iso"),
        col("rank"), col("ugc_count"), col("growth"), col("fetch_timestamp"), col("ingested_at")
    )

    write_batch_to_snowflake(tiktok_transformed.select("sound_sk", "sound_name").dropDuplicates(["sound_sk"]), "Dim_Sound")
    write_batch_to_snowflake(tiktok_transformed.select("country_sk", "country_code_iso").dropDuplicates(["country_sk"]), "Dim_Country")
    
    # في الـ Fact، الـ performance_sk ده للحدث، فممكن نعمله UUID جديد
    write_batch_to_snowflake(tiktok_transformed.select(
        expr("uuid()").alias("performance_sk"), 
        "sound_sk", "country_sk", "platform_sk", "ingested_date_sk", "ingested_time_sk",
        "rank", "ugc_count", "growth", "fetch_timestamp", "ingested_at"
    ), "Fact_Tiktok_Performance")
    
    df.unpersist()

spark.readStream \
    .schema(tiktok_schema) \
    .option("encoding", "UTF-8") \
    .json("hdfs://namenode:9000/datalake/tiktok_data/2025/*/*/*.json") \
    .writeStream \
    .foreachBatch(process_tiktok_batch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/tiktok_job_uuid") \
    .trigger(availableNow=True) \
    .start() \
    .awaitTermination()


# ==============================================================================
# 3. معالجة YOUTUBE (UUID Implementation)
# ==============================================================================
youtube_schema = spark.read.json("hdfs://namenode:9000/datalake/youtube_data/2025/*/*/*.json").schema

def process_youtube_batch(df, batch_id):
    print(f"--- Processing New YouTube Files (Batch: {batch_id}) ---")
    df.cache()

    # تجهيز SKs للـ Channels والـ Categories
    # Video ID يعتبر NK، فهنعمله SK عشوائي برضه
    video_dim = df.select("video_id").distinct().withColumn("video_sk", expr("uuid()"))
    channel_dim = df.select("channel").distinct().withColumn("channel_sk", expr("uuid()"))
    category_dim = df.select("category").distinct().withColumn("category_sk", expr("uuid()"))

    df_joined = df.join(video_dim, "video_id", "left") \
                  .join(channel_dim, "channel", "left") \
                  .join(category_dim, "category", "left")

    yt_transformed = df_joined.select(
        col("video_sk"),
        col("channel_sk"),
        col("category_sk"),
        lit(PLATFORM_UUIDS["YouTube"]).alias("platform_sk"),
        date_format(col("ingested_at"), "yyyyMMdd").cast(IntegerType()).alias("ingested_date_sk"),
        date_format(col("ingested_at"), "HHmm").cast(StringType()).alias("ingested_time_sk"),
        col("video_id"), col("title").alias("video_title"), col("video_url"), col("published_at"),
        col("channel").alias("channel_name"), col("category").alias("category_name"),
        col("views"), col("likes"), col("ingestion_timestamp"), col("ingested_at")
    )

    write_batch_to_snowflake(yt_transformed.select("video_sk", col("video_id").alias("video_id_nk"), "video_url", "video_title", "published_at").dropDuplicates(["video_sk"]), "Dim_Video")
    write_batch_to_snowflake(yt_transformed.select("channel_sk", col("channel_name").alias("channel_name_nk")).dropDuplicates(["channel_sk"]), "Dim_Channel")
    write_batch_to_snowflake(yt_transformed.select("category_sk", col("category_name").alias("category_name_nk")).dropDuplicates(["category_sk"]), "Dim_Category")
    
    write_batch_to_snowflake(yt_transformed.select(
        expr("uuid()").alias("performance_sk"),
        "video_sk", "channel_sk", "category_sk", "platform_sk", "ingested_date_sk", "ingested_time_sk",
        "views", "likes", "ingestion_timestamp", "ingested_at"
    ), "Fact_Youtube_Performance")
    
    df.unpersist()

spark.readStream \
    .schema(youtube_schema) \
    .option("encoding", "UTF-8") \
    .json("hdfs://namenode:9000/datalake/youtube_data/2025/*/*/*.json") \
    .writeStream \
    .foreachBatch(process_youtube_batch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/youtube_job_uuid") \
    .trigger(availableNow=True) \
    .start() \
    .awaitTermination()


# ==============================================================================
# 4. معالجة TWITCH (UUID Implementation)
# ==============================================================================
twitch_schema = spark.read.json("hdfs://namenode:9000/datalake/twitch_streams/2025/*/*/*.json").schema

def process_twitch_batch(df, batch_id):
    print(f"--- Processing New Twitch Files (Batch: {batch_id}) ---")
    df.cache()

    # تجهيز SKs
    game_dim = df.select("game_name").distinct().withColumn("game_sk", expr("uuid()"))
    user_dim = df.select("user_name").distinct().withColumn("user_sk", expr("uuid()"))
    
    # Stream SK: بما إن الستريم ممكن يتكرر في كذا لقطة (snapshot)، هنعمله SK مميز يعتمد على الـ NK المركب بتاعه بس بـ UUID
    # هنا هنعمل UUID مباشر للحدث لأن الجدول Dim_Stream مفروض يخزن البث كحدث فريد
    # لكن لتوحيد الـ ID لنفس الستريم جوه الباتش، هنعمل distinct
    # (هنفترض هنا اننا بنعرف الستريم بـ user_name + fetched_at كـ unique event)
    df = df.withColumn("stream_nk_temp", concat(col("user_name"), col("fetched_at").cast("string")))
    stream_dim = df.select("stream_nk_temp").distinct().withColumn("stream_sk", expr("uuid()"))

    df_joined = df.join(game_dim, "game_name", "left") \
                  .join(user_dim, "user_name", "left") \
                  .join(stream_dim, "stream_nk_temp", "left")

    twitch_transformed = df_joined.select(
        col("stream_sk"),
        col("game_sk"),
        col("user_sk"),
        lit(PLATFORM_UUIDS["Twitch"]).alias("platform_sk"),
        date_format(col("ingested_at"), "yyyyMMdd").cast(IntegerType()).alias("ingested_date_sk"),
        date_format(col("ingested_at"), "HHmm").cast(StringType()).alias("ingested_time_sk"),
        col("user_name"), col("game_name"), col("title").alias("stream_title"),
        col("viewer_count"), col("thumbnail_url"), col("type"),
        col("fetched_at").alias("fetched_timestamp"), col("ingested_at")
    )

    write_batch_to_snowflake(twitch_transformed.select("game_sk", col("game_name").alias("game_name_nk")).dropDuplicates(["game_sk"]), "Dim_Game")
    write_batch_to_snowflake(twitch_transformed.select("stream_sk", "stream_title", "thumbnail_url", "type","user_name").dropDuplicates(["stream_sk"]), "Dim_Stream")
    
    write_batch_to_snowflake(twitch_transformed.select(
        expr("uuid()").alias("performance_sk"),
        "stream_sk", "user_sk", "game_sk", "platform_sk", "ingested_date_sk", "ingested_time_sk",
        "viewer_count", "fetched_timestamp", "ingested_at"
    ), "Fact_Twitch_Performance")
    
    df.unpersist()

spark.readStream \
    .schema(twitch_schema) \
    .option("encoding", "UTF-8") \
    .json("hdfs://namenode:9000/datalake/twitch_streams/2025/*/*/*.json") \
    .writeStream \
    .foreachBatch(process_twitch_batch) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/twitch_job_uuid") \
    .trigger(availableNow=True) \
    .start() \
    .awaitTermination()

print("All tasks finished successfully.")