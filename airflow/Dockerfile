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
    "apache-airflow-providers-hashicorp==4.8.3" \
    "pyspark==4.1.2" \
    "feast[spark,postgres,redis]==0.66.0" \
    "prometheus-client==0.24.1" \
    "onnxruntime==1.31.0" \
    "tokenizers==0.23.3"

# DataHub's SDK pins its own dependencies, so it gets a virtualenv of its own.
RUN python -m venv /home/airflow/datahub \
    && /home/airflow/datahub/bin/pip install --no-cache-dir "acryl-datahub==1.6.0" "pyyaml==6.0.3"

# The embedding model ships inside the image, fetched and checked by the module that pins it.
# Only that module is copied, so other code changes keep this layer cached.
ENV FRAUDSTREAM_EMBEDDING_DIR=/opt/airflow/models/bge-base-en-v1.5
COPY rag/src/fraudstream_rag/embedding.py /opt/airflow/embedding.py
RUN python /opt/airflow/embedding.py "$FRAUDSTREAM_EMBEDDING_DIR" --check \
    && rm /opt/airflow/embedding.py
