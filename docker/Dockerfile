FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements-bigdata.txt requirements-ml.txt ./
RUN pip install --no-cache-dir -r requirements-bigdata.txt -r requirements-ml.txt
COPY . .
RUN pip install --no-cache-dir --no-deps .
ENV PYTHONPATH=/app PYSPARK_PYTHON=/usr/local/bin/python AWS_REGION=us-east-1
ENV SPARK_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,org.apache.iceberg:iceberg-aws-bundle:1.9.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7
# Download connectors at build time rather than discovering them during a demo.
RUN spark-submit --packages "$SPARK_PACKAGES" scripts/warmup.py
CMD ["python", "run.py", "--help"]
