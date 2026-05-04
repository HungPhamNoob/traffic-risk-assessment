# processing/spark/Dockerfile - Placeholder (Hiếu sẽ update sau)
FROM bitnami/spark:3.5.1

# Install Sedona + dependencies (comment lại nếu chưa cần)
# RUN pip install apache-sedona==1.4.0 shapely==2.0.2 geopandas==0.14.0

# Copy code
COPY . /opt/spark/work-dir

# Default command (override by docker-compose)
CMD ["bin/spark-class", "org.apache.spark.deploy.worker.Worker"]