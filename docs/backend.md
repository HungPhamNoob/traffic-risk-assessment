# Backend Guide

## 1. Tai lieu nay dung de lam gi?

File nay giai thich backend theo goc nhin cua mot nguoi chua biet gi ve project nhung can:

- hieu backend dang doc du lieu tu dau,
- hieu moi API lay du lieu tu file/service nao,
- biet can mo file nao neu muon sua logic,
- hieu luong tu pipeline du lieu sang dashboard backend.

Neu ban phai code dashboard sau nay, hay doc theo thu tu:

1. `docs/backend.md`
2. `docs/database.md`
3. `docs/api.md`
4. code trong `dashboard/backend/app`

## 2. Backend nay co vai tro gi?

Backend nam trong `dashboard/backend` va la mot FastAPI service. No **khong** tu train model, **khong** tu stream Kafka, **khong** tu tao feature goc. Backend lam 4 viec chinh:

1. doc bang prediction trong PostgreSQL/PostGIS,
2. expose JSON API cho dashboard,
3. goi MLflow serving cho chuc nang what-if scenario,
4. expose health va Prometheus metrics.

Noi ngan gon:

- pipeline streaming/batch tao du lieu,
- PostgreSQL luu prediction,
- MLflow serving phuc vu mo hinh,
- backend doc hai nguon do va tra JSON cho FE.

## 3. Toan canh luong he thong ma backend dung

### Stage 1 - Raw replay vao Kafka

File tham gia:

- `ingestion/kafka/us_producer.py`
- `.env` hoac `.env.cloud`

Nhiem vu:

- doc file CSV replay sau nam 2020,
- gui moi dong CSV raw len Kafka topic `traffic.us.raw`,
- khong lam feature engineering o day.

Output cua stage nay:

- Kafka topic `traffic.us.raw`

Backend **khong** doc Kafka truc tiep.

### Stage 2 - Streaming feature engineering + inference

File tham gia:

- `processing/flink_streaming.py`
- `processing/feature_engineering.py`

Nhiem vu:

1. Flink doc message raw tu Kafka.
2. Goi `build_features()` trong `processing/feature_engineering.py` de bien raw row thanh feature vector chuan.
3. Ghi feature ra Silver storage (`SILVER_FEATURES_PATH`).
4. Goi `MLFLOW_SERVING_ENDPOINT` de lay `predicted_severity` va `risk_score`.
5. Insert ket qua vao bang PostgreSQL `traffic_risk_predictions`.

Output cua stage nay:

- Silver JSONL feature files
- bang PostgreSQL/PostGIS chua prediction

Day la nguon du lieu quan trong nhat cua backend dashboard.

### Stage 3 - Batch clean/retrain

File tham gia:

- `processing/spark_batch.py`
- `ml/training/h2o_after_2020.py`
- `ml/training/h2o_before_2020.py`
- `ml/dataset/dataset_offline.py`
- `orchestration/dags/dag_ml_pipeline.py`

Nhiem vu:

- Spark doc Silver, clean, fill default, dedupe, ghi Gold parquet/csv.
- H2O AutoML doc Gold de retrain model.
- MLflow tracking + registry luu run va model version.

Output cua stage nay:

- Gold retrain dataset
- model moi trong MLflow

Backend khong doc Gold truc tiep de tra API. Backend chi:

- doc `GOLD_RETRAIN_PATH` de hien metadata trong `/api/v1/system/status`,
- doc config model va goi MLflow serving trong scenario API.

### Stage 4 - API phuc vu dashboard

File tham gia:

- `dashboard/backend/app/app.py`
- `dashboard/backend/app/routes/*.py`
- `dashboard/backend/app/services/*.py`
- `dashboard/backend/app/core/*.py`

Nhiem vu:

- nhan HTTP request,
- query PostgreSQL,
- goi MLflow neu can,
- format JSON response cho dashboard.

## 4. Cau truc `dashboard/backend`

### `dashboard/backend/app/app.py`

Day la entrypoint cua backend.

Nhiem vu:

- tao `FastAPI(...)`,
- bat CORS cho `http://localhost:3000` va `http://localhost:8000`,
- them middleware Prometheus metrics,
- expose:
  - `GET /health`
  - `GET /metrics`
- register toan bo router `/api/v1/...`

Neu backend khong len duoc, day la file dau tien can check.

### `dashboard/backend/app/core/config.py`

Nhiem vu:

- doc env tu `.env.cloud`, `.env`, hoac process environment,
- map thanh `Settings`,
- cache lai bang `@lru_cache`.

Tat ca service deu lay config qua `get_settings()`.

### `dashboard/backend/app/core/database.py`

Nhiem vu:

- mo ket noi `psycopg2`,
- expose `fetch_one()` va `fetch_all()`,
- tra ket qua dang `dict`.

Luu y quan trong:

- backend hien tai khong dung ORM,
- moi request SQL se mo mot ket noi PostgreSQL moi,
- neu bang chua ton tai (`UndefinedTable`) thi helper tra:
  - `None` cho `fetch_one`
  - `[]` cho `fetch_all`

Dieu nay giai thich vi sao mot so endpoint co the tra rong thay vi loi 500 khi data chua duoc seed.

### `dashboard/backend/app/routes/`

Moi file route chi giu vai tro HTTP layer:

- khai bao endpoint,
- validate query/body o muc FastAPI/Pydantic,
- goi service function.

Route khong chua business logic phuc tap.

### `dashboard/backend/app/services/`

Day moi la noi chua logic backend that su:

- `prediction_service.py`
  - overview
  - map
  - latest
  - detail
  - risk level mapping
- `hotspot_service.py`
  - top hotspots
  - nearby events
  - hotspot detail
- `analytics_service.py`
  - timeseries
  - severity distribution
  - risk by hour
  - risk by weather
- `mlflow_service.py`
  - scenario prediction
  - MLflow serving request
  - heuristic fallback neu MLflow khong san sang

Neu muon thay doi output JSON, SQL query, cong thuc risk label, hoac logic scenario, day la thu muc can sua.

### `dashboard/backend/app/schemas/scenario.py`

Nhiem vu:

- dinh nghia body cho:
  - `POST /api/v1/scenarios/predict`
  - `POST /api/v1/scenarios/compare`

Neu FE gui sai field hoac field vuot range, FastAPI se tra `422`.

### `dashboard/backend/Dockerfile`

Nhiem vu:

- build image backend,
- install requirement,
- chay `uvicorn app.app:app --host 0.0.0.0 --port 8000`

## 5. Request lifecycle thuc te

### Vi du 1 - `GET /api/v1/predictions/map`

Luong file:

1. request vao `dashboard/backend/app/app.py`
2. router match toi `dashboard/backend/app/routes/predictions.py`
3. route goi `map_points(...)` trong `dashboard/backend/app/services/prediction_service.py`
4. service dung `table_identifier()` + `fetch_all()` trong:
   - `dashboard/backend/app/services/prediction_service.py`
   - `dashboard/backend/app/core/database.py`
5. PostgreSQL tra rows
6. service convert:
   - `event_time` -> ISO string
   - `risk_score` -> `risk_level`
   - `model_status` null -> `"unknown"`
7. backend tra JSON:
   - `{ "points": [...] }`

Nguon goc cua du lieu trong bang:

- duoc ghi boi `processing/flink_streaming.py`
- hoac duoc seed boi `scripts/local/run_pipeline.sh` trong local smoke mode

### Vi du 2 - `POST /api/v1/scenarios/compare`

Luong file:

1. request vao `app.py`
2. router match toi `dashboard/backend/app/routes/scenarios.py`
3. body duoc validate boi `dashboard/backend/app/schemas/scenario.py`
4. route goi `predict_scenario()` hai lan trong `dashboard/backend/app/services/mlflow_service.py`
5. service:
   - sap xep feature theo `MODEL_FEATURE_COLUMNS`
   - goi `MLFLOW_SERVING_ENDPOINT`
   - normalize output
   - neu fail thi dung `heuristic_prediction()`
6. route tu tinh `delta`
7. backend tra JSON so sanh baseline va scenario

Endpoint nay **khong doc database**.

## 6. Map route -> service -> data source

| Endpoint group | Route file | Service/file xu ly chinh | Data source chinh |
| --- | --- | --- | --- |
| `GET /health`, `GET /metrics` | `app/app.py` | `app/app.py` | process metrics trong memory |
| `GET /api/v1/overview/summary` | `app/routes/overview.py` | `app/services/prediction_service.py::overview_summary` | PostgreSQL prediction table |
| `GET /api/v1/predictions/*` | `app/routes/predictions.py` | `app/services/prediction_service.py` | PostgreSQL prediction table |
| `GET /api/v1/hotspots*` | `app/routes/hotspots.py` | `app/services/hotspot_service.py` | PostgreSQL prediction table |
| `GET /api/v1/analytics/*` | `app/routes/analytics.py` | `app/services/analytics_service.py` | PostgreSQL prediction table |
| `POST /api/v1/scenarios/*` | `app/routes/scenarios.py` | `app/services/mlflow_service.py` | MLflow serving endpoint |
| `GET /api/v1/system/status` | `app/routes/system.py` | `app/routes/system.py` + `overview_summary()` | env config + PostgreSQL |
| `GET /api/v1/system/model/info` | `app/routes/system.py` | `app/routes/system.py` | env config |
| `GET /api/v1/model/info` | `app/routes/model.py` | `app/routes/model.py` | env config |
| alias APIs | `app/routes/docs_aliases.py` | route alias sang service co san | giong endpoint goc |

## 7. Bien moi truong backend can quan tam

Doc tai `dashboard/backend/app/core/config.py`.

### Database

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PREDICTION_TABLE`

Dung cho tat ca endpoint doc du lieu prediction.

### Pipeline metadata

- `KAFKA_TOPIC_RAW`
- `FLINK_CHECKPOINT_DIR`
- `FLINK_CHECKPOINT_INTERVAL`
- `GOLD_RETRAIN_PATH`

Chu yeu dung de hien thong tin trong `/api/v1/system/status`.

### Model/MLflow

- `MLFLOW_TRACKING_URI`
- `MLFLOW_SERVING_ENDPOINT`
- `ML_MODEL_NAME`
- `ML_MODEL_VERSION`

Dung cho:

- scenario simulation,
- model info APIs,
- overview/system metadata.

## 8. Moi truong local va production khac nhau the nao?

### Local

File lien quan:

- `docker-compose.yaml`
- `scripts/local/run_pipeline.sh`

Local mode thuong:

- chay Postgres, Kafka, MLflow, FastAPI bang Docker,
- seed bang prediction tu script local,
- thu scenario qua JSON sample.

Trong local smoke, script `scripts/local/run_pipeline.sh` co the tao bang voi **nhieu cot hon** bang production de phuc vu test va inspect.
Ngoai ra, local smoke seed prediction bang cach lay `predicted_severity = true_severity` va suy ra `risk_score`, nghia la local demo data khong phai luc nao cung la output model serving that.

### Production/GCP

File lien quan:

- `.env.cloud`
- `deployment/node1-control/docker-compose.yaml`
- `deployment/node2-streaming/docker-compose.yaml`
- `deployment/node3-batch/docker-compose.yaml`
- `scripts/gcp/*.sh`

Production mode thuong:

- producer/Flink chay tren node streaming,
- Postgres + MLflow + Airflow nam tren node control,
- Spark + H2O nam tren node batch.

Backend van la FastAPI service, nhung no doc env cloud va query den Postgres/MLflow qua IP noi bo.

## 9. Nhung diem rat quan trong neu ban se code FE/BE

### 9.1 Backend doc prediction table, khong doc raw CSV

Dashboard FE khong can quan tam Kafka hay raw CSV. FE chi can:

- goi API,
- hieu y nghia field response,
- biet response do duoc tong hop tu bang prediction.

### 9.2 Scenario API la online inference rieng

Scenario API:

- khong lay row co san tu DB,
- khong can event co that,
- nhan mot feature vector tu user,
- goi model serving ngay luc do.

Vi vay FE phai xem day la mot form input model, khong phai search event.

### 9.3 Backend hien tai query spatial bang `lat/lon`, chua tan dung PostGIS day du

Mac du bang co cot `geom`, service hien tai:

- bbox filter dung `lat BETWEEN ...` va `lon BETWEEN ...`
- nearby query dung cong thuc xap xi khoang cach tren lat/lon

Neu sau nay can toi uu map/heavy geo query, can sua service SQL.

### 9.4 Backend khong co auth, khong co write API

Tat ca endpoint hien tai la:

- read-only cho dashboard,
- hoac inference-only cho scenario.

Chua co:

- login,
- RBAC,
- create/update/delete event.

### 9.5 Co duplicate model info endpoint

Hai endpoint nay tra gan nhu cung mot noi dung:

- `GET /api/v1/system/model/info`
- `GET /api/v1/model/info`

FE chi can chon mot endpoint va dung nhat quan.

## 10. Thu tu doc code de onboard nhanh

Neu ban moi vao project va can hieu backend nhanh nhat, doc theo thu tu nay:

1. `dashboard/backend/app/app.py`
2. `dashboard/backend/app/routes/predictions.py`
3. `dashboard/backend/app/services/prediction_service.py`
4. `dashboard/backend/app/core/database.py`
5. `dashboard/backend/app/routes/scenarios.py`
6. `dashboard/backend/app/services/mlflow_service.py`
7. `processing/flink_streaming.py`
8. `processing/feature_engineering.py`
9. `processing/spark_batch.py`
10. `ml/training/h2o_after_2020.py`

Neu doc theo thu tu nay, ban se thay ro:

- du lieu vao backend den tu dau,
- du lieu nao la persisted,
- du lieu nao la realtime inference,
- file nao can sua khi dashboard doi yeu cau.
