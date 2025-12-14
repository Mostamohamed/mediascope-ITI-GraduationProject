#!/bin/bash

echo "🔴 KILLING old processes..."
pkill -f python3
pkill -f spark-submit
pkill -f streamlit

# ==========================================
# 1. (الخطوة اللي كانت ناقصة) تسطيب المكتبات
# ==========================================
echo "📦 INSTALLING Python Libraries (Necessary after restart)..."
pip3 install kafka-python requests redis pandas streamlit altair watchdog matplotlib beautifulsoup4 > /dev/null 2>&1

echo "🧹 CLEANING corrupted cache..."
rm -rf /root/.ivy2/

echo "⬇️ DOWNLOADING Spark Dependencies..."
# تحميل مكتبات سبارك بشكل آمن
/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_*.jar 1 > /dev/null 2>&1

if [ -d "/root/.ivy2/jars" ]; then
    echo "✅ Dependencies Downloaded Successfully!"
else
    echo "❌ Download Failed! Check Internet Connection."
    exit 1
fi

echo "🚀 STARTING Social Pulse Project..."

# 2. Start Producers
echo "📡 Starting Producers..."
python3 /opt/spark/apps/producer_reddit.py > /dev/null 2>&1 &
python3 /opt/spark/apps/producer_youtube.py > /dev/null 2>&1 &
python3 /opt/spark/apps/producer_twitch.py > /dev/null 2>&1 &
python3 /opt/spark/apps/producer_tiktok.py > /dev/null 2>&1 &

# 3. Start Spark Jobs
echo "⚙️ Starting Spark Processors..."

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/apps/process_reddit.py > /opt/spark/apps/log_reddit.txt 2>&1 &

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/apps/process_youtube.py > /opt/spark/apps/log_youtube.txt 2>&1 &

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/apps/process_twitch.py > /opt/spark/apps/log_twitch.txt 2>&1 &

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/apps/process_tiktok.py > /opt/spark/apps/log_tiktok.txt 2>&1 &

echo "⏳ Waiting 15 seconds for warmup..."
sleep 15

# 4. Start Dashboard
echo "📊 Starting Dashboard..."
echo "✅ GO TO: http://localhost:8501"

streamlit run /opt/spark/apps/Home.py --server.port 8501 --server.address 0.0.0.0