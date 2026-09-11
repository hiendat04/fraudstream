# Python 3.12 matches the Airflow image, so the same Feast/PySpark versions
# run in both places. The JRE is here because the offline half of every push
# is an Iceberg write executed by a local Spark session.
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONPATH=/opt/fraudstream/src:/opt/fraudstream/feature_store/src
ENV FEAST_REPO_PATH=/opt/fraudstream/feature_store/feature_repo

RUN pip install --no-cache-dir \
    "feast[spark,postgres,redis]==0.66.0" \
    "pyspark==4.1.2" \
    "confluent-kafka>=2.6,<3"

WORKDIR /opt/fraudstream
CMD ["python", "-m", "fraudstream_feast.stream_push"]
