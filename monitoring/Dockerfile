FROM python:3.11-slim

WORKDIR /app

COPY monitoring /app/monitoring

RUN pip install --no-cache-dir \
    google-cloud-storage>=2.14.0 \
    prometheus-client>=0.19.0 \
    psycopg2-binary>=2.9.9 \
    requests>=2.31.0

ENV PYTHONPATH=/app

CMD ["python", "-m", "monitoring.pipeline_exporter"]
