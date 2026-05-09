# Road Accident Risk Analysis Platform

Monorepo cho hệ thống phân tích và dự báo rủi ro tai nạn giao thông theo mô hình batch + streaming + ML + serving.

## Monorepo Overview

```text
road-accident-risk-platform/
├── docs/
├── deployment/
├── config/
├── schemas/
├── data/
├── ingestion/
├── processing/
├── ml/
├── simulation/
├── serving/
├── dashboard/
├── orchestration/
├── scripts/
├── tests/
└── .github/workflows/
```

## Quick Start

1. Tạo môi trường từ file mẫu:

   ```bash
   cp .env.example .env
   ```

2. Chạy local stack:

   ```bash
   make local-dev
   ```

3. Chạy test:

   ```bash
   make test
   ```

## Team Folder Ownership

- `deployment/`, `orchestration/`, `docs/`, `config/`: infra, Airflow, MLflow, Docker
- `ingestion/`, `processing/flink/`: realtime pipeline + TomTom
- `processing/spark/`: batch ETL + spatial analytics
- `ml/`, `simulation/`: model training + what-if analysis
- `serving/`, `dashboard/`: FastAPI + Deck.gl + PostGIS

## Data Lake

Chi tiết lớp dữ liệu xem tại `docs/data-lake-layers.md`.
