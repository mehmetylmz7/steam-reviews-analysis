FROM eclipse-temurin:11-jdk-jammy

# Temel araçları kur
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Python aliası
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Spark 3.4.1 indir ve kur
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3

# Python bağımlılıkları
RUN pip3 install --no-cache-dir \
    pyspark==3.4.1 \
    delta-spark==2.4.0

WORKDIR /app
