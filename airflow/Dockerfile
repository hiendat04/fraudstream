FROM apache/airflow:3.3.0-python3.12

USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.0" \
    "pyspark==4.1.2"
