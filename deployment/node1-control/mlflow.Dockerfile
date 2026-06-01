FROM ghcr.io/mlflow/mlflow:v2.12.1

RUN python -m pip install --no-cache-dir --disable-pip-version-check \
    google-auth \
    google-cloud-storage
