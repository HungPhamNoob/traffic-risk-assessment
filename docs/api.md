# API Reference

## 1. Tong quan

Backend API duoc build bang FastAPI, entrypoint nam o `dashboard/backend/app/app.py`.

Base URL mac dinh:

- local: `http://localhost:8000`
- cloud: tuy theo service deployment, nhung route structure giu nguyen

Tien to chung cho dashboard API:

- `/api/v1/...`

Ngoai ra con co:

- `GET /health`
- `GET /metrics`

## 2. Quy uoc chung

### Dinh dang thoi gian

- cac field datetime duoc tra ve dang ISO string, vi du: `2024-01-01T08:30:00`
- query `start_time`, `end_time` duoc truyen len dang string va dua thang xuong SQL

Backend hien tai khong tu parse/chuan hoa sau, vi vay client nen gui datetime ro rang, uu tien ISO 8601.

### Risk level

Backend map `risk_score` thanh `risk_level` theo quy tac:

- `high`: `risk_score >= 0.7`
- `medium`: `0.4 <= risk_score < 0.7`
- `low`: `risk_score < 0.4`

Logic nay nam trong:

- `dashboard/backend/app/services/prediction_service.py`
- `dashboard/backend/app/services/mlflow_service.py`

### Loi thuong gap

- `400`: query parameter sai format, hien tai gap o `bbox`
- `404`: event khong ton tai, gap o `GET /api/v1/predictions/{event_id}`
- `422`: body khong hop le theo Pydantic schema

### Hanh vi khi database chua co bang

Do `fetch_one()` va `fetch_all()` trong `dashboard/backend/app/core/database.py` bat `UndefinedTable`, mot so endpoint se tra ket qua rong thay vi 500:

- overview -> so lieu 0
- list APIs -> mang rong

## 3. Health va Metrics

### `GET /health`

File xu ly:

- route/function: `dashboard/backend/app/app.py::health`

Muc dich:

- kiem tra process FastAPI co dang song hay khong

Input:

- khong co

Output:

```json
{
  "status": "ok"
}
```

Workflow:

1. request vao `app.py`
2. tra thang JSON tinh
3. khong doc DB, khong goi MLflow

### `GET /metrics`

File xu ly:

- route/function: `dashboard/backend/app/app.py::metrics`

Muc dich:

- expose Prometheus metrics

Input:

- khong co

Output:

- Prometheus text format, khong phai JSON

Workflow:

1. middleware trong `app.py` thu thap request count va latency
2. endpoint `/metrics` dump metrics bang `generate_latest()`

## 4. Overview API

### `GET /api/v1/overview/summary`

File xu ly:

- route: `dashboard/backend/app/routes/overview.py`
- service: `dashboard/backend/app/services/prediction_service.py::overview_summary`

Muc dich:

- lay cac KPI tong quan de hien dashboard overview card

Input:

- khong co query param

Output:

```json
{
  "total_events": 125000,
  "high_risk_events": 18400,
  "avg_risk_score": 0.4381,
  "latest_event_time": "2024-01-01T08:30:00",
  "latest_model_version": "latest"
}
```

Y nghia field:

- `total_events`: tong so row trong prediction table
- `high_risk_events`: so row co `risk_score >= 0.7`
- `avg_risk_score`: trung binh `risk_score`
- `latest_event_time`: event time lon nhat trong bang
- `latest_model_version`: doc tu env `ML_MODEL_VERSION`, khong lay tu DB

Workflow:

1. route goi `overview_summary()`
2. service query SQL aggregate tren bang prediction
3. service format `latest_event_time` thanh ISO string
4. service bo sung `latest_model_version` tu env config

Bang/cot duoc dung:

- `risk_score`
- `event_time`

## 5. Prediction APIs

## 5.1 `GET /api/v1/predictions/map`

File xu ly:

- route: `dashboard/backend/app/routes/predictions.py::get_prediction_map`
- service: `dashboard/backend/app/services/prediction_service.py::map_points`

Muc dich:

- tra danh sach diem prediction de ve map

Query input:

| Param | Kieu | Bat buoc | Mac dinh | Y nghia |
| --- | --- | --- | --- | --- |
| `bbox` | `string` | khong | `null` | `min_lon,min_lat,max_lon,max_lat` |
| `min_risk` | `float` | khong | `0.0` | chi lay diem co `risk_score >= min_risk` |
| `start_time` | `string` | khong | `null` | loc `event_time >= start_time` |
| `end_time` | `string` | khong | `null` | loc `event_time <= end_time` |
| `limit` | `int` | khong | `5000` | so diem toi da, min `1`, max `20000` |

Validation quan trong:

- `min_risk` phai trong `[0, 1]`
- `limit` trong `[1, 20000]`
- `bbox` phai co dung 4 gia tri, neu khong se tra `400`

Output:

```json
{
  "points": [
    {
      "event_id": "A-123",
      "lat": 39.8,
      "lon": -84.1,
      "risk_score": 0.82,
      "predicted_severity": 4,
      "true_severity": 3,
      "event_time": "2024-01-01T08:30:00",
      "model_status": "ok",
      "risk_level": "high"
    }
  ]
}
```

Workflow backend:

1. FastAPI validate `min_risk`, `limit`
2. service tao `where_clauses` dong
3. neu co `bbox`, service split string va them filter lat/lon
4. query PostgreSQL
5. sap xep `ORDER BY event_time DESC NULLS LAST`
6. them `risk_level`
7. normalize `model_status` null thanh `"unknown"`

Bang/cot duoc dung:

- `event_id`
- `lat`
- `lon`
- `risk_score`
- `predicted_severity`
- `true_severity`
- `event_time`
- `model_status`

Luu y:

- query spatial hien tai dung `lat/lon`, chua dung `geom`
- phu hop cho map render nhanh va filter co ban

## 5.2 `GET /api/v1/predictions/latest`

File xu ly:

- route: `dashboard/backend/app/routes/predictions.py::get_latest_predictions`
- service: `dashboard/backend/app/services/prediction_service.py::latest_predictions`

Muc dich:

- lay cac prediction moi nhat de hien bang recent events

Query input:

| Param | Kieu | Bat buoc | Mac dinh | Rang buoc |
| --- | --- | --- | --- | --- |
| `limit` | `int` | khong | `100` | min `1`, max `1000` |

Output:

```json
{
  "predictions": [
    {
      "event_id": "A-123",
      "event_time": "2024-01-01T08:30:00",
      "lat": 39.8,
      "lon": -84.1,
      "risk_score": 0.82,
      "predicted_severity": 4,
      "true_severity": 3,
      "model_status": "ok",
      "risk_level": "high"
    }
  ]
}
```

Workflow backend:

1. route doc `limit`
2. service query bang prediction
3. sap xep theo `event_time DESC`
4. them `risk_level`

## 5.3 `GET /api/v1/predictions/{event_id}`

File xu ly:

- route: `dashboard/backend/app/routes/predictions.py::get_prediction_detail`
- service: `dashboard/backend/app/services/prediction_service.py::prediction_detail`

Muc dich:

- lay chi tiet mot event cu the

Path input:

| Param | Kieu | Y nghia |
| --- | --- | --- |
| `event_id` | `string` | ID su kien trong prediction table |

Output:

- tra `SELECT *` cua row, nhung:
  - `geom` bi bo di truoc khi response
  - `event_time`, `created_at` duoc convert sang ISO string
  - duoc them `risk_level`
  - `model_status` null -> `"unknown"`

Vi day la `SELECT *`, so field co the khac nhau giua local va production:

- production table do `processing/flink_streaming.py` tao co bo cot gon hon
- local smoke table do `scripts/local/run_pipeline.sh` seed co them nhieu cot phu

Workflow backend:

1. query theo `event_id`
2. neu khong tim thay -> `404`
3. convert datetime
4. bo `geom`
5. tra full row con lai

## 6. Hotspot APIs

## 6.1 `GET /api/v1/hotspots`

File xu ly:

- route: `dashboard/backend/app/routes/hotspots.py::get_hotspots`
- service: `dashboard/backend/app/services/hotspot_service.py::top_hotspots`

Muc dich:

- tra cac diem nong rui ro cao dua tren nhom toa do gan nhau

Query input:

| Param | Kieu | Bat buoc | Mac dinh | Rang buoc |
| --- | --- | --- | --- | --- |
| `limit` | `int` | khong | `20` | min `1`, max `500` |
| `min_events` | `int` | khong | `5` | min `1` |
| `start_time` | `string` | khong | `null` | loc theo `event_time >= start_time` |
| `end_time` | `string` | khong | `null` | loc theo `event_time <= end_time` |

Output:

```json
{
  "hotspots": [
    {
      "rank": 1,
      "center_lat": 39.865,
      "center_lon": -84.059,
      "avg_risk_score": 0.8123,
      "accident_count": 42,
      "severe_count": 18,
      "peak_hour": 17
    }
  ]
}
```

Logic nhom hotspot:

- lam tron `lat`, `lon` toi 3 chu so thap phan
- group theo cap toa do da lam tron
- tinh:
  - `avg_risk_score`
  - `accident_count`
  - `severe_count`
  - `peak_hour`

`severe_count` duoc tinh khi:

- `true_severity >= 3`
- hoac `predicted_severity >= 3`

Workflow backend:

1. tao SQL CTE `grouped`
2. group by rounded lat/lon
3. loai nhom co count nho hon `min_events`
4. xep hang theo `avg_risk_score DESC, accident_count DESC`
5. them `rank` bang `ROW_NUMBER()`

## 6.2 `GET /api/v1/hotspots/nearby`

File xu ly:

- route: `dashboard/backend/app/routes/hotspots.py::get_nearby_events`
- service: `dashboard/backend/app/services/hotspot_service.py::nearby_events`

Muc dich:

- tra cac su kien gan mot diem do FE click/chon tren ban do

Query input:

| Param | Kieu | Bat buoc | Mac dinh | Rang buoc |
| --- | --- | --- | --- | --- |
| `lat` | `float` | co | - | - |
| `lon` | `float` | co | - | - |
| `radius_m` | `float` | khong | `5000` | `> 0` |
| `limit` | `int` | khong | `100` | min `1`, max `1000` |

Output:

```json
{
  "center": {
    "lat": 39.865,
    "lon": -84.059
  },
  "radius_m": 5000,
  "events": [
    {
      "event_id": "A-123",
      "lat": 39.86,
      "lon": -84.05,
      "risk_score": 0.72,
      "distance_m": 423.7
    }
  ]
}
```

Workflow backend:

1. service doi `radius_m` thanh bounding box xap xi tren lat/lon
2. query events nam trong box
3. tinh `distance_m` bang cong thuc xap xi
4. sap xep theo `distance_m ASC`

Luu y:

- day la truy van xap xi, chua dung `ST_DWithin`

## 6.3 `GET /api/v1/hotspots/{hotspot_id}`

File xu ly:

- route: `dashboard/backend/app/routes/hotspots.py::get_hotspot_detail`
- service: `dashboard/backend/app/services/hotspot_service.py::hotspot_detail`

Muc dich:

- lay mot hotspot theo thu hang 1-based

Path input:

| Param | Kieu | Y nghia |
| --- | --- | --- |
| `hotspot_id` | `int` | thu hang hotspot, bat dau tu 1 |

Output:

```json
{
  "hotspot": {
    "rank": 1,
    "center_lat": 39.865,
    "center_lon": -84.059,
    "avg_risk_score": 0.8123,
    "accident_count": 42,
    "severe_count": 18,
    "peak_hour": 17
  }
}
```

Neu rank vuot qua so hotspot hien co:

```json
{
  "hotspot": null
}
```

Luu y implementation:

- service goi lai `top_hotspots(limit=hotspot_id, min_events=5, ...)`
- nghia la detail dang phu thuoc ranking mac dinh, khong phai 1 row duoc luu san voi ID rieng

## 7. Scenario APIs

Scenario APIs dung cho what-if simulation. Day la nhom endpoint quan trong neu sau nay FE co form cho nguoi dung nhap dieu kien giao thong/thoi tiet.

Nguon logic:

- route: `dashboard/backend/app/routes/scenarios.py`
- schema: `dashboard/backend/app/schemas/scenario.py`
- service: `dashboard/backend/app/services/mlflow_service.py`

Scenario APIs **khong** doc prediction table.

## 7.1 Body schema `ScenarioInput`

Body duoc validate theo schema sau:

```json
{
  "lat": 0,
  "lon": 0,
  "hour": 0,
  "day_of_week": 1,
  "is_weekend": 0,
  "is_rush_hour": 0,
  "weather_code": 0,
  "temperature_f": 0,
  "humidity": 0,
  "wind_speed_mph": 0,
  "visibility_mi": 0,
  "road_type_code": 0,
  "is_junction": 0,
  "has_traffic_signal": 0,
  "is_crossing": 0,
  "is_roundabout": 0,
  "is_stop": 0,
  "is_station": 0,
  "is_railway": 0,
  "is_night": 0
}
```

Rang buoc validation tu schema:

- `hour`: `0..23`
- `day_of_week`: `1..7`
- cac field co dang flag:
  - `is_weekend`
  - `is_rush_hour`
  - `is_junction`
  - `has_traffic_signal`
  - `is_crossing`
  - `is_roundabout`
  - `is_stop`
  - `is_station`
  - `is_railway`
  - `is_night`
  deu phai la `0` hoac `1`

## 7.2 `POST /api/v1/scenarios/predict`

Muc dich:

- predict muc do rui ro cho mot scenario don

Input:

- JSON body theo `ScenarioInput`

Output:

```json
{
  "predicted_severity": 3,
  "risk_score": 0.6732,
  "risk_level": "medium",
  "model_name": "traffic-risk-model",
  "model_version": "latest",
  "model_status": "ok"
}
```

Y nghia field:

- `predicted_severity`: class severity model tra ve
- `risk_score`: diem rui ro sau normalize
- `risk_level`: low/medium/high
- `model_name`, `model_version`: lay tu env
- `model_status`:
  - `ok`: goi MLflow serving thanh cong
  - `heuristic_fallback`: MLflow serving loi, backend dung heuristic thay the

Workflow backend:

1. validate body bang Pydantic
2. service sap xep feature theo `MODEL_FEATURE_COLUMNS`
3. tao payload `dataframe_split`
4. `POST` toi `MLFLOW_SERVING_ENDPOINT`
5. normalize output prediction
6. neu `requests` fail -> dung `heuristic_prediction()`
7. tra response JSON

## 7.3 `POST /api/v1/scenarios/compare`

Muc dich:

- so sanh baseline va scenario da sua

Input:

```json
{
  "baseline": {
    "...": "ScenarioInput"
  },
  "scenario": {
    "...": "ScenarioInput"
  }
}
```

Output:

```json
{
  "baseline": {
    "predicted_severity": 2,
    "risk_score": 0.31,
    "risk_level": "low",
    "model_name": "traffic-risk-model",
    "model_version": "latest",
    "model_status": "ok"
  },
  "scenario": {
    "predicted_severity": 4,
    "risk_score": 0.81,
    "risk_level": "high",
    "model_name": "traffic-risk-model",
    "model_version": "latest",
    "model_status": "ok"
  },
  "delta": {
    "risk_score_change": 0.5,
    "risk_percent_change": 161.29,
    "severity_change": 2
  }
}
```

Workflow backend:

1. validate `baseline` va `scenario`
2. goi `predict_scenario()` cho `baseline`
3. goi `predict_scenario()` cho `scenario`
4. route tu tinh:
   - `risk_score_change`
   - `risk_percent_change`
   - `severity_change`
5. tra tong hop JSON

## 8. Analytics APIs

File xu ly:

- route: `dashboard/backend/app/routes/analytics.py`
- service: `dashboard/backend/app/services/analytics_service.py`

## 8.1 `GET /api/v1/analytics/timeseries`

Muc dich:

- tra chuoi thoi gian de ve line/bar chart

Query input:

| Param | Kieu | Bat buoc | Mac dinh | Y nghia |
| --- | --- | --- | --- | --- |
| `group_by` | `string` | khong | `day` | `day`, `month`, `year` |
| `metric` | `string` | khong | `avg_risk` | hien chi echo lai trong response |
| `start_time` | `string` | khong | `null` | loc `event_time >= start_time` |
| `end_time` | `string` | khong | `null` | loc `event_time <= end_time` |

Output:

```json
{
  "series": [
    {
      "time": "2024-01-01",
      "avg_risk_score": 0.421,
      "accident_count": 320
    }
  ],
  "metric": "avg_risk"
}
```

Luu y rat quan trong:

- tham so `metric` hien tai **khong doi SQL query**
- service luon tra ca:
  - `avg_risk_score`
  - `accident_count`
- `metric` chi duoc echo lai trong response

Neu FE muon nhieu metric thuc su, can sua them service.

## 8.2 `GET /api/v1/analytics/severity-distribution`

Muc dich:

- dem so event theo `true_severity`

Input:

- khong co

Output:

```json
{
  "distribution": [
    {
      "severity": 1,
      "count": 1200
    },
    {
      "severity": 2,
      "count": 5300
    }
  ]
}
```

Bang/cot duoc dung:

- `true_severity`

## 8.3 `GET /api/v1/analytics/risk-by-hour`

Muc dich:

- lay trung binh risk theo gio trong ngay

Input:

- khong co

Output:

```json
{
  "data": [
    {
      "hour": 0,
      "avg_risk_score": 0.29,
      "accident_count": 180
    }
  ]
}
```

Bang/cot duoc dung:

- `hour`
- `risk_score`

## 8.4 `GET /api/v1/analytics/risk-by-weather`

Muc dich:

- lay trung binh risk theo `weather_code`

Input:

- khong co

Output:

```json
{
  "data": [
    {
      "weather_code": 0,
      "avg_risk_score": 0.31,
      "accident_count": 2200
    }
  ]
}
```

Bang/cot duoc dung:

- `weather_code`
- `risk_score`

## 9. System va Model APIs

## 9.1 `GET /api/v1/system/status`

File xu ly:

- route: `dashboard/backend/app/routes/system.py::get_system_status`

Muc dich:

- tra mot ban tom tat config va metadata he thong cho dashboard admin/ops

Input:

- khong co

Output:

```json
{
  "environment": "local",
  "kafka": {
    "topic": "traffic.us.raw",
    "status": "configured"
  },
  "flink": {
    "job_name": "Flink Traffic Risk Prediction",
    "status": "configured",
    "checkpoint_dir": "gs://.../checkpoints/flink",
    "checkpoint_interval_ms": 30000
  },
  "spark": {
    "last_gold_update": "2024-01-01T08:30:00",
    "gold_path": "gs://.../gold/features/retrain"
  },
  "mlflow": {
    "model_name": "traffic-risk-model",
    "serving_endpoint": "http://localhost:5001/invocations",
    "latest_version": "latest"
  },
  "postgres": {
    "prediction_table": "traffic_risk_predictions",
    "row_count": 125000
  }
}
```

Workflow backend:

1. doc env qua `get_settings()`
2. goi `overview_summary()` de lay:
   - `latest_event_time`
   - `total_events`
3. ghep lai thanh object status

Luu y:

- day la endpoint metadata/config, khong phai health check deep
- cac `status: configured` la hardcoded, khong phai real probe
- `spark.last_gold_update` hien dang lay tu `overview_summary().latest_event_time`, nghia la no gan voi latest event trong bang prediction hon la timestamp cap nhat Gold dataset thuc su

## 9.2 `GET /api/v1/system/model/info`

## 9.3 `GET /api/v1/model/info`

Hai endpoint nay gan nhu tuong duong.

Muc dich:

- tra config model va MLflow backend dang duoc API su dung

Input:

- khong co

Output:

```json
{
  "model_name": "traffic-risk-model",
  "model_version": "latest",
  "tracking_uri": "http://localhost:5000",
  "serving_endpoint": "http://localhost:5001/invocations"
}
```

Khac nhau:

- `/api/v1/system/model/info` nam trong route `system.py`
- `/api/v1/model/info` nam trong route `model.py`

## 10. Alias APIs

File xu ly:

- `dashboard/backend/app/routes/docs_aliases.py`

Muc dich:

- giu tuong thich voi design doc cu

Bang map alias:

| Alias endpoint | Endpoint/noi dung that su |
| --- | --- |
| `GET /api/v1/risk/hotspots` | giong `GET /api/v1/hotspots` |
| `GET /api/v1/accidents` | giong `GET /api/v1/predictions/map` |
| `POST /api/v1/whatif/simulate` | giong `POST /api/v1/scenarios/compare` |
| `GET /api/v1/system/health` | giong `GET /api/v1/system/status` |

Neu FE moi, nen uu tien dung endpoint chinh thay vi alias.

## 11. Goi y dung API cho dashboard sau nay

Neu sau nay ban lam FE dashboard, cach map hop ly thuong la:

- overview cards:
  - `GET /api/v1/overview/summary`
- map points:
  - `GET /api/v1/predictions/map`
- recent table:
  - `GET /api/v1/predictions/latest`
- detail drawer/modal:
  - `GET /api/v1/predictions/{event_id}`
- hotspot panel:
  - `GET /api/v1/hotspots`
  - `GET /api/v1/hotspots/nearby`
- analytics charts:
  - `GET /api/v1/analytics/timeseries`
  - `GET /api/v1/analytics/severity-distribution`
  - `GET /api/v1/analytics/risk-by-hour`
  - `GET /api/v1/analytics/risk-by-weather`
- what-if simulator:
  - `POST /api/v1/scenarios/predict`
  - `POST /api/v1/scenarios/compare`
- admin/system tab:
  - `GET /api/v1/system/status`
  - `GET /api/v1/model/info`
