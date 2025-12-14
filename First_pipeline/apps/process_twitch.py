from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
import redis
import json

# Redis Config
REDIS_HOST = 'redis'
REDIS_PORT = 6379

# Keys
KEY_STREAMERS_RANK = "twitch_streamers_rank"
KEY_STREAMERS_DETAILS = "twitch_streamers_details"
KEY_GAMES_RANK = "twitch_games_rank"
KEY_GAMES_DETAILS = "twitch_games_details"

def write_streamers_to_redis(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline()
        
        # ترتيب وتصفية التكرار (ناخد احدث لقطة للاستريمر)
        pdf = pdf.sort_values('viewer_count', ascending=False).drop_duplicates(['user_name'])
        
        r.delete(KEY_STREAMERS_RANK) # مسح القديم
        
        for _, row in pdf.iterrows():
            # 1. الترتيب
            pipe.zadd(KEY_STREAMERS_RANK, {row['user_name']: row['viewer_count']})
            # 2. التفاصيل
            details = {
                "game": row['game_name'],
                "title": row['title'],
                "thumb": row['thumbnail_url'],
                "views": row['viewer_count']
            }
            pipe.hset(KEY_STREAMERS_DETAILS, row['user_name'], json.dumps(details))
        
        pipe.execute()

def write_games_to_redis(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline()
        
        pdf = pdf.sort_values('total_viewers', ascending=False).drop_duplicates(['game_name'])
        
        r.delete(KEY_GAMES_RANK)
        
        for _, row in pdf.iterrows():
            pipe.zadd(KEY_GAMES_RANK, {row['game_name']: row['total_viewers']})
            details = {
                "img": row['box_art_url'],
                "views": row['total_viewers']
            }
            pipe.hset(KEY_GAMES_DETAILS, row['game_name'], json.dumps(details))
            
        pipe.execute()

# --- Spark Setup ---
spark = SparkSession.builder \
    .appName("TwitchMultiStream") \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Schema 1: Streams ---
schema_streams = StructType([
    StructField("user_name", StringType()),
    StructField("game_name", StringType()),
    StructField("title", StringType()),
    StructField("viewer_count", IntegerType()),
    StructField("thumbnail_url", StringType()),
    StructField("fetched_at", StringType())
])

# --- Schema 2: Games ---
schema_games = StructType([
    StructField("game_name", StringType()),
    StructField("box_art_url", StringType()),
    StructField("total_viewers", IntegerType()),
    StructField("fetched_at", StringType())
])

# --- Logic 1: Process Streams ---
df_streams = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "twitch_streams").load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_streams).alias("data")).select("data.*")

query1 = df_streams.writeStream.foreachBatch(write_streamers_to_redis).start()

# --- Logic 2: Process Games ---
df_games = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "twitch_games").load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_games).alias("data")).select("data.*")

query2 = df_games.writeStream.foreachBatch(write_games_to_redis).start()

# استنى الاتنين يخلصوا (عمرهم ما هيخلصوا)
spark.streams.awaitAnyTermination()