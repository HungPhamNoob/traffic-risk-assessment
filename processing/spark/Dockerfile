# processing/spark/Dockerfile - Real Spark Image
FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Install Sedona + geo deps (Hiếu uncomment khi ready)
# RUN pip install apache-sedona==1.4.0 shapely geopandas pyarrow

COPY . /opt/spark/work-dir
WORKDIR /opt/spark/work-dir

CMD ["bin/spark-class", "org.apache.spark.deploy.worker.Worker"]