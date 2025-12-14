# استخدام صورة Spark الرسمية
FROM apache/spark:3.5.3

USER root

# 1. تثبيت أدوات البناء و Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless \
    wget build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev curl \
    libncursesw5-dev xz-utils tk-dev libxml2-dev \
    libxmlsec1-dev libffi-dev liblzma-dev && \
    rm -rf /var/lib/apt/lists/*

# 2. تحميل وبناء Python 3.11 من المصدر (لأن المستودعات لا تدعم النسخة القديمة)
WORKDIR /tmp
RUN wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz && \
    tar xvf Python-3.11.9.tgz && \
    cd Python-3.11.9 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.11.9 Python-3.11.9.tgz

# 3. إعداد الروابط (Symlinks) لتوجيه النظام لاستخدام النسخة الجديدة
# نستخدم python3.11 بدلاً من python3.8 القديم
RUN rm -f /usr/bin/python3 && \
    ln -s /usr/local/bin/python3.11 /usr/bin/python3 && \
    ln -s /usr/local/bin/python3.11 /usr/bin/python

# إعداد متغيرات البيئة لـ Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# العودة لمستخدم Spark
USER spark