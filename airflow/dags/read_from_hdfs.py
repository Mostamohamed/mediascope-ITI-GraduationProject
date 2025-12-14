from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow"}

# ---------------------------------------------------------
# التعديل هنا:
# schedule_interval='0 0,12 * * *'
# معناها: اشتغل في الدقيقة 0، عند الساعة 0 (منتصف الليل) والساعة 12 (الظهر)
# ---------------------------------------------------------

with DAG(
    dag_id="spark_hdfs_cleaning",
    start_date=datetime(2025, 12, 6),
    schedule_interval='0 0,12 * * *',  # <--- التعديل هنا (مرتين يومياً)
    default_args=default_args,
    catchup=False,
) as dag:

    # 1. Spark Task
    spark_task = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark/apps/read_from_hdfs.py",
        conn_id="spark_default", 
        verbose=True,
        # تأكد إن النسخ دي متوافقة مع نسخة Spark اللي عندك
        packages="net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.29",
       
    )


    spark_task 