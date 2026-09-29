FROM apache/airflow:2.9.3-python3.11

ARG SPARK_VERSION=3.5.1
ARG HADOOP_PROFILE=3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    openjdk-17-jre-headless \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Spark client нужен Airflow для SparkSubmitOperator. Сам кластер при этом
# продолжает работать в отдельных spark-master/spark-worker контейнерах.
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz

RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.8.1 \
    apache-airflow-providers-postgres==5.12.0 \
    boto3==1.34.144 \
    requests==2.32.3
