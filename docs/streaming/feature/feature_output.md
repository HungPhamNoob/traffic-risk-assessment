# Streaming Feature Output

The US replay stream and TomTom live stream both pass through the same feature builder where possible. US features are sent to MLflow/H2O for inference. TomTom features are used for live display and rule-based severity only.

| # | Feature | Type | Source column | Processing function | Description | Valid values |
| --- | --- | --- | --- | --- | --- | --- |
|  | **Metadata, not model input** |  |  |  |  |  |
| 1 | `event_id` | String | `ID` | `_safe_string` | Stable event identifier | `A-12345`, `tomtom-...` |
| 2 | `event_year` | Int | `Start_Time` | `parse_datetime(...).year` | Event year | 2016 onward |
| 3 | `event_time` | ISO string | `Start_Time` | `parse_datetime(...).isoformat()` | Event timestamp | ISO datetime string |
|  | **Label, not model input** |  |  |  |  |  |
| 4 | `true_severity` | Int | `Severity` | `_safe_int` | Ground-truth or rule-based severity | 1, 2, 3, 4 |
|  | **Geospatial** |  |  |  |  |  |
| 5 | `lat` | Float | `Start_Lat` | `_safe_float` | Event latitude | -90.0 to 90.0 |
| 6 | `lon` | Float | `Start_Lng` | `_safe_float` | Event longitude | -180.0 to 180.0 |
|  | **Time** |  |  |  |  |  |
| 7 | `hour` | Int | `Start_Time` | `parse_datetime(...).hour` | Hour of day | 0 to 23 |
| 8 | `day_of_week` | Int | `Start_Time` | `spark_day_of_week` | Spark-compatible day of week | 1=Sunday through 7=Saturday |
| 9 | `is_weekend` | Int | `day_of_week` | set membership | Weekend flag | 0 or 1 |
| 10 | `is_rush_hour` | Int | `hour` | `is_rush_hour` | Commute-hour flag | 0 or 1 |
|  | **Weather** |  |  |  |  |  |
| 11 | `weather_code` | Int | `Weather_Condition` | `encode_weather_condition` | Normalized weather category | 0=clear, 1=rain, 2=snow, 3=fog, 4=storm, 5=cloudy, 6=windy |
| 12 | `temperature_f` | Float | `Temperature(F)` | `_clip_float(-40, 130)` | Temperature in Fahrenheit | -40.0 to 130.0 |
| 13 | `humidity` | Float | `Humidity(%)` | `_clip_float(0, 100)` | Relative humidity percentage | 0.0 to 100.0 |
| 14 | `wind_speed_mph` | Float | `Wind_Speed(mph)` | `_clip_float(0, 100)` | Wind speed in miles per hour | 0.0 to 100.0 |
| 15 | `visibility_mi` | Float | `Visibility(mi)` | `_clip_float(0, 10)` | Visibility in miles | 0.0 to 10.0 |
|  | **Road and context** |  |  |  |  |  |
| 16 | `road_type_code` | Int | `Street` | `encode_road_type` | Normalized road category | 0=unknown, 1=interstate, 2=route, 3=street, 4=avenue, 5=boulevard, 6=drive, 7=road |
| 17 | `is_junction` | Int | `Junction` | `_safe_bool_as_int` | Junction flag | 0 or 1 |
| 18 | `has_traffic_signal` | Int | `Traffic_Signal` | `_safe_bool_as_int` | Traffic-signal flag | 0 or 1 |
| 19 | `is_crossing` | Int | `Crossing` | `_safe_bool_as_int` | Crossing flag | 0 or 1 |
| 20 | `is_roundabout` | Int | `Roundabout` | `_safe_bool_as_int` | Roundabout flag | 0 or 1 |
| 21 | `is_stop` | Int | `Stop` | `_safe_bool_as_int` | Stop-sign flag | 0 or 1 |
| 22 | `is_station` | Int | `Station` | `_safe_bool_as_int` | Station flag | 0 or 1 |
| 23 | `is_railway` | Int | `Railway` | `_safe_bool_as_int` | Railway flag | 0 or 1 |
|  | **Light** |  |  |  |  |  |
| 24 | `is_night` | Int | `Sunrise_Sunset` | string comparison | Nighttime flag | 0 or 1 |
