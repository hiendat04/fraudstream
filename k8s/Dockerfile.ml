# One image for every step of the training pipeline. It carries the training
# package plus a Spark that can read the Iceberg lakehouse, because the first
# step pulls features out of Feast before anything can be trained.
#
# Build from the repository root:
#   docker build -f k8s/Dockerfile.ml -t fraudstream-ml:dev .
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONPATH=/opt/fraudstream/ml/src
ENV FEAST_REPO_PATH=/opt/fraudstream/feature_store/feature_repo

# XGBoost drags in NVIDIA's GPU collective libraries, which are close to 300MB
# and useless here -- this cluster has no GPU and the training runs on CPU.
# Dropped in the same step as the install so the layer never carries them.
RUN pip install --no-cache-dir \
    "feast[spark,postgres,redis]==0.66.0" \
    "pyspark==4.1.2" \
    "xgboost>=2.1,<4" \
    "scikit-learn>=1.5,<2" \
    "pandas>=2.2,<3" \
    "joblib>=1.4,<2" \
    "kubeflow>=0.1" \
    && pip freeze | grep -i '^nvidia-' | cut -d= -f1 | xargs -r pip uninstall --yes

# Fetch the Spark jars now, while the build still has a network. A fresh pod
# starts with an empty Ivy cache, so without this every run would spend its
# first minutes pulling these from Maven -- and would fail outright on a node
# that cannot reach the internet.
RUN python -c "\
from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').appName('jar-prefetch') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.4,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0') \
    .getOrCreate().stop()"

COPY ml/src /opt/fraudstream/ml/src
COPY feature_store/feature_repo /opt/fraudstream/feature_store/feature_repo

# The Feast config reads these from the environment. Inside the cluster the
# lakehouse answers to its Docker network service names rather than localhost.
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=fraudstream
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV MINIO_ENDPOINT=http://minio:9000
ENV FRAUDSTREAM_WAREHOUSE_URI=s3a://fraudstream/warehouse
ENV FEAST_SPARK_MASTER=local[4]

# Usernames and passwords are deliberately absent: they arrive as environment
# variables at run time so they are never baked into the image.

WORKDIR /opt/fraudstream
