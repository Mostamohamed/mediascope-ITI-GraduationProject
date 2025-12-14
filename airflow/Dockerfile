FROM apache/airflow:2.9.3-python3.11

# تثبيت OpenJDK 17 Headless، و ant، وأدوات الشبكة الأساسية، و wget (كـ root)
# تم إضافة wget هنا لحل مشكلة "command not found"
USER root
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk-headless \
        ant \
        procps \
        net-tools \
        wget \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ===== إضافة Spark Client (3.5.3) لتوافق الإصدارات =====
# ده بيضمن أن Airflow بيستخدم Spark 3.5.3 عشان يتكلم مع Spark Master
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
# ===== نهاية إضافة Spark Client =====


# تعيين JAVA_HOME ومتغير PATH
# المسار ده هو المسار الافتراضي لتثبيت Java 17 في Debian Bookworm (اللي Airflow مبني عليها)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}:${SPARK_HOME}/bin" 

# تعيين متغيرات البيئة لـ HADOOP/YARN
RUN mkdir -p /etc/hadoop/conf
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV YARN_CONF_DIR=/etc/hadoop/conf

# التحويل إلى مستخدم airflow قبل تثبيت المتطلبات
USER airflow

# تثبيت متطلبات Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# التحقق من إصدار Java
RUN java -version