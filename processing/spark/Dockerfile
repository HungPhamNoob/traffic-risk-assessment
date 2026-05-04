# processing/spark/Dockerfile - Placeholder
FROM apache/spark:3.5.1-scala2.12-java11

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Copy code (placeholder)
COPY . /opt/spark/work-dir

# Default command
CMD ["bin/spark-class", "org.apache.spark.deploy.worker.Worker"]