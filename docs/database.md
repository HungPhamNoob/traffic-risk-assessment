# Database Guide

## 1. Muc tieu tai lieu

File nay giai thich:

- backend dang doc du lieu tu dau,
- bang PostgreSQL nao la quan trong nhat,
- cot nao duoc tao boi file nao,
- cot nao duoc endpoint nao su dung,
- khac nhau giua schema production va local smoke.

Neu ban can code backend hoac dashboard, day la file giup ban hieu du lieu truoc khi nhin UI.

## 2. Database trong project nay la gi?

Project su dung PostgreSQL + PostGIS.

Vai tro:

- luu prediction da duoc Flink suy dien,
- lam nguon doc cho dashboard backend,
- luu toa do dia ly qua cot `geom`.

Backend khong dung Redis, Kafka hay Gold parquet de tra JSON truc tiep. Gan nhu moi API dashboard deu doc tu **mot bang chinh**:

- `traffic_risk_predictions`

Ten bang nay duoc lay tu env:

- `POSTGRES_PREDICTION_TABLE`

## 3. File nao tao va file nao doc database?

## 3.1 File ghi vao bang prediction

### Production streaming writer

File:

- `processing/flink_streaming.py`

Nhiem vu:

1. doc raw event tu Kafka,
2. feature engineering,
3. goi MLflow serving,
4. tao bang neu chua co,
5. `INSERT ... ON CONFLICT (event_id) DO UPDATE`

Day la writer chinh cua production.

### Local smoke writer

File:

- `scripts/local/run_pipeline.sh`

Nhiem vu:

- tao du lieu simulation local,
- seed prediction table bang script Python embedded,
- tao bang voi schema mo rong hon de de inspect.

Day la writer chinh khi ban test local bang smoke pipeline.

## 3.2 File doc tu bang prediction

### Backend read layer

File:

- `dashboard/backend/app/core/database.py`

Nhiem vu:

- tao ket noi `psycopg2`,
- execute query,
- tra ket qua dang `dict`

### Backend services doc du lieu

File:

- `dashboard/backend/app/services/prediction_service.py`
- `dashboard/backend/app/services/hotspot_service.py`
- `dashboard/backend/app/services/analytics_service.py`
- mot phan cua `dashboard/backend/app/routes/system.py`

## 4. Data lineage: du lieu vao bang prediction di the nao?

## Step 1 - Raw CSV

Nguon:

- file replay sau 2020
- vi du trong cloud: `US_PIPELINE_REPLAY_PATH=gs://big-data-group-4-bronze/process/us_pipeline_from_2020.csv`

File xu ly:

- `ingestion/kafka/us_producer.py`

Output:

- Kafka topic `traffic.us.raw`

## Step 2 - Shared feature engineering

File xu ly:

- `processing/feature_engineering.py`

Nhiem vu:

- parse `Start_Time`
- tao:
  - `event_year`
  - `hour`
  - `day_of_week`
  - `is_weekend`
  - `is_rush_hour`
  - `weather_code`
  - `road_type_code`
  - cac flag nhu `is_junction`, `is_crossing`, `is_night`, ...

Output:

- 1 feature dict chuan ma ca Flink, Spark va training deu dung

## Step 3 - Flink ghi Silver va PostgreSQL

File xu ly:

- `processing/flink_streaming.py`

Nhiem vu:

1. nhan raw JSON tu Kafka
2. goi `build_features()`
3. ghi feature ra Silver JSONL
4. goi MLflow serving
5. insert prediction vao PostgreSQL

Output quan trong cho backend:

- bang `traffic_risk_predictions`

## Step 4 - Backend doc PostgreSQL

File xu ly:

- `dashboard/backend/app/services/*.py`

Nhiem vu:

- query bang prediction
- aggregate/format thanh response JSON

## 5. Production schema thuc te do Flink tao

Schema nay nam trong `processing/flink_streaming.py` qua `CREATE_TABLE_SQL`.

```sql
CREATE TABLE IF NOT EXISTS traffic_risk_predictions (
    event_id VARCHAR PRIMARY KEY,
    event_year INT,
    event_time TIMESTAMP,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    true_severity INT,
    predicted_severity INT,
    risk_score DOUBLE PRECISION,
    weather_code INT,
    road_type_code INT,
    hour INT,
    is_weekend INT,
    is_rush_hour INT,
    model_status VARCHAR(20),
    inference_latency_ms DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);
```

## 5.1 Y nghia tung cot

| Cot | Kieu | Do file nao tao | Y nghia |
| --- | --- | --- | --- |
| `event_id` | `VARCHAR` | `processing/feature_engineering.py` | ID su kien, khoa chinh |
| `event_year` | `INT` | `processing/feature_engineering.py` | nam cua event |
| `event_time` | `TIMESTAMP` | `processing/feature_engineering.py` | thoi diem xay ra su kien |
| `lat` | `DOUBLE PRECISION` | `processing/feature_engineering.py` | vi do |
| `lon` | `DOUBLE PRECISION` | `processing/feature_engineering.py` | kinh do |
| `true_severity` | `INT` | raw data -> feature engineering | severity goc trong dataset |
| `predicted_severity` | `INT` | MLflow serving qua `processing/flink_streaming.py` | severity model du doan |
| `risk_score` | `DOUBLE PRECISION` | MLflow serving qua `processing/flink_streaming.py` | diem rui ro model |
| `weather_code` | `INT` | `processing/feature_engineering.py` | ma hoa thoi tiet |
| `road_type_code` | `INT` | `processing/feature_engineering.py` | ma hoa loai duong |
| `hour` | `INT` | `processing/feature_engineering.py` | gio xay ra event |
| `is_weekend` | `INT` | `processing/feature_engineering.py` | co phai cuoi tuan khong |
| `is_rush_hour` | `INT` | `processing/feature_engineering.py` | co phai gio cao diem khong |
| `model_status` | `VARCHAR(20)` | `processing/flink_streaming.py` | `ok` neu co prediction, `failed` neu khong |
| `inference_latency_ms` | `DOUBLE PRECISION` | `processing/flink_streaming.py` | do tre suy dien + insert path |
| `geom` | `GEOMETRY(Point, 4326)` | `processing/flink_streaming.py` | point tao tu `lon`, `lat` |
| `created_at` | `TIMESTAMP` | PostgreSQL default | thoi diem row duoc ghi/cap nhat |

## 5.2 Luu y schema production

Production schema **khong** luu day du toan bo feature vector.

Cac cot sau co trong `ScenarioInput` va trong Gold training data, nhung **khong co** trong production table do Flink tao:

- `day_of_week`
- `temperature_f`
- `humidity`
- `wind_speed_mph`
- `visibility_mi`
- `is_junction`
- `has_traffic_signal`
- `is_crossing`
- `is_roundabout`
- `is_stop`
- `is_station`
- `is_railway`
- `is_night`

Dieu nay rat quan trong cho nguoi lam dashboard:

- ban khong nen gia dinh prediction detail API luc nao cung co du cac feature nay trong production
- scenario API dung feature day du, nhung no goi MLflow truc tiep, khong lay tu bang nay

## 6. Local smoke schema mo rong

Trong `scripts/local/run_pipeline.sh`, local script tao bang voi nhieu cot hon:

- `temperature_f`
- `humidity`
- `wind_speed_mph`
- `visibility_mi`
- `day_of_week`
- `is_junction`
- `has_traffic_signal`
- `is_crossing`
- `is_roundabout`
- `is_stop`
- `is_station`
- `is_railway`
- `is_night`
- `ingestion_time`
- `processed_time`
- `end_to_end_latency_ms`

Y nghia:

- local smoke duoc thiet ke de de debug va review pipeline,
- production Flink writer hien tai ghi schema gon hon.
- local smoke khong nhat thiet dai dien cho prediction model that, vi script seed dang gan `predicted_severity` bang `true_severity` roi suy ra `risk_score`.

Vi vay `GET /api/v1/predictions/{event_id}`:

- o local co the tra nhieu field,
- o production co the tra it field hon.

## 7. Ai doc cot nao?

Bang sau giup ban thay endpoint nao dung cot nao trong DB.

| Endpoint/service | Cot duoc dung |
| --- | --- |
| `overview_summary()` | `risk_score`, `event_time` |
| `map_points()` | `event_id`, `lat`, `lon`, `risk_score`, `predicted_severity`, `true_severity`, `event_time`, `model_status` |
| `latest_predictions()` | `event_id`, `event_time`, `lat`, `lon`, `risk_score`, `predicted_severity`, `true_severity`, `model_status` |
| `prediction_detail()` | `SELECT *` tru `geom` |
| `top_hotspots()` | `lat`, `lon`, `risk_score`, `true_severity`, `predicted_severity`, `hour`, `event_time` |
| `nearby_events()` | `event_id`, `lat`, `lon`, `risk_score` |
| `timeseries()` | `event_time`, `risk_score` |
| `severity_distribution()` | `true_severity` |
| `risk_by_hour()` | `hour`, `risk_score` |
| `risk_by_weather()` | `weather_code`, `risk_score` |
| `system/status` | `event_time`, `risk_score`, row count metadata |

## 8. Luong ghi/update trong bang

Writer chinh la `processing/flink_streaming.py`.

No dung:

```sql
ON CONFLICT (event_id) DO UPDATE
```

Y nghia:

- neu cung `event_id` den lai, row se duoc update
- `created_at` cung bi set lai `NOW()` trong branch update

Tac dong:

- bang nay co the xem la latest state theo `event_id`
- no khong luu history version cua cung mot `event_id`

## 9. PostGIS dang duoc dung den muc nao?

Bang co cot:

- `geom GEOMETRY(Point, 4326)`

Nhung backend hien tai:

- khong query bang `geom`
- khong dung `ST_DWithin`, `ST_Intersects`, `ST_Within`
- bbox map filter bang `lat/lon`
- nearby events tinh khoang cach xap xi thu cong

Nghia la:

- PostGIS hien tai chu yeu dung de luu hinh hoc
- chua khai thac het kha nang spatial database

Neu can toi uu map spatial sau nay, service SQL la noi can nang cap.

## 10. Ket noi database cua backend

Backend doc config trong `dashboard/backend/app/core/config.py`:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PREDICTION_TABLE`

Ket noi thuc te duoc mo trong:

- `dashboard/backend/app/core/database.py::get_connection`

Moi query se:

1. mo mot connection
2. tao `RealDictCursor`
3. execute SQL
4. dong connection

Khi moi onboard backend, day la cho can doc neu co loi ket noi Postgres.

## 11. Nhung diem can canh bao cho nguoi moi

### 11.1 Backend khong tu tao extension PostGIS

Local smoke script co:

- `CREATE EXTENSION IF NOT EXISTS postgis;`

Nhung `processing/flink_streaming.py` chi tao bang co cot `GEOMETRY(...)`, khong tao extension.

Vi vay production database phai:

- da cai PostGIS san
- va extension da san sang

Neu khong, Flink insert se loi.

### 11.2 Khong co migration framework

Project hien tai khong thay:

- Alembic
- migration SQL versioned

Schema duoc tao boi code runtime:

- Flink job
- local smoke script

Nghia la khi doi schema, ban phai rat can than vi local va production co the bi lech.

### 11.3 Undefined table khong lam crash moi endpoint

`fetch_one()` / `fetch_all()` bat `psycopg2.errors.UndefinedTable`, nen:

- overview co the tra 0
- list API co the tra mang rong

Dieu nay tot cho demo, nhung khong co nghia pipeline da chay dung.

### 11.4 `event_time` la cot quan trong nhat cho dashboard

Rat nhieu endpoint dua vao `event_time`:

- latest
- overview
- timeseries
- map filter
- hotspots filter

Neu pipeline ghi `event_time` sai format, dashboard se hong nhieu man hinh cung luc.

## 12. Tom tat ngan gon

Neu chi nho 4 y chinh, hay nho:

1. Backend dashboard doc chu yeu tu bang `traffic_risk_predictions`.
2. Bang nay duoc production Flink ghi trong `processing/flink_streaming.py`.
3. Local smoke co schema rong hon production nen prediction detail co the khac field.
4. Scenario API khong doc bang nay; no goi MLflow serving truc tiep.
