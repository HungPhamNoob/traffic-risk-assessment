# Placeholder - Huy sẽ update sau
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo "No requirements.txt"
COPY . .
EXPOSE 8000
CMD ["echo", "FastAPI placeholder - update by Huy"]