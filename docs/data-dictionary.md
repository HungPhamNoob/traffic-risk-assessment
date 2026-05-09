# Data Dictionary

## Nguon du lieu

- US accidents (historical)
- UK accidents (historical)
- TomTom incidents (realtime)
- Weather API (enrichment)

## Core fields

| Field | Layer | Mo ta |
| --- | --- | --- |
| event_id | bronze/silver/gold | Dinh danh su kien |
| source | bronze/silver/gold | us, uk, tomtom |
| event_time | bronze/silver/gold | Thoi diem su kien |
| lat | bronze/silver/gold | Vi do |
| lon | bronze/silver/gold | Kinh do |
| severity | silver/gold | Muc do nghiem trong |
| weather_code | silver/gold | Ma thoi tiet sau enrichment |
| road_type | silver/gold | Loai duong |
| risk_score | gold | Diem rui ro 0-1 |
| hotspot_id | gold | O hotspot (H3/grid) |

## TomTom mapping (goi y)

| TomTom field | Internal field |
| --- | --- |
| properties.iconCategory | incident_type |
| properties.magnitudeOfDelay | delay_magnitude |
| properties.delay | delay_seconds |
| properties.length | affected_length |
| geometry.coordinates | geometry |

