| # | Feature | Kiểu dữ liệu | Cột gốc trong CSV | Hàm xử lý | Mô tả | Giá trị hợp lệ |
| --- | --- | --- | --- | --- | --- | --- |
|  | **METADATA (không dùng làm input model)** |  |  |  |  |  |
| 1 | `event_id` | String | `ID` | `_safe_string` | Mã định danh tai nạn | A-12345, B-67890... |
| 2 | `event_year` | Int | `Start_Time` | `parse_datetime` → `.year` | Năm xảy ra tai nạn | 2020, 2021, 2022... |
| 3 | `event_time` | ISO String | `Start_Time` | `parse_datetime` → `.isoformat()` | Thời gian đầy đủ UTC | "2021-03-15T08:30:00" |
|  | **LABEL (không dùng làm input model)** |  |  |  |  |  |
| 4 | `true_severity` | Int | `Severity` | `_safe_int` | Mức độ nghiêm trọng thực tế | 1, 2, 3, 4 |
|  | **GEOSPATIAL** |  |  |  |  |  |
| 5 | `lat` | Float | `Start_Lat` | `_safe_float` | Vĩ độ điểm tai nạn | -90.0 đến 90.0 |
| 6 | `lon` | Float | `Start_Lng` | `_safe_float` | Kinh độ điểm tai nạn | -180.0 đến 180.0 |
|  | **TIME** |  |  |  |  |  |
| 7 | `hour` | Int | `Start_Time` | `parse_datetime` → `.hour` | Giờ trong ngày (0-23) | 0 đến 23 |
| 8 | `day_of_week` | Int | `Start_Time` | `spark_day_of_week` | Thứ trong tuần | 1=CN, 2=T2, 3=T3, 4=T4, 5=T5, 6=T6, 7=T7 |
| 9 | `is_weekend` | Int (0/1) | `Start_Time` | So sánh `day_of_week` | Cuối tuần? | 0=không, 1=có (CN hoặc T7) |
| 10 | `is_rush_hour` | Int (0/1) | `Start_Time` | `is_rush_hour(hour)` | Giờ cao điểm? | 0=không, 1=có (7-9h hoặc 16-18h) |
|  | **WEATHER** |  |  |  |  |  |
| 11 | `weather_code` | Int | `Weather_Condition` | `encode_weather_condition` | Mã hóa điều kiện thời tiết | 0=clear, 1=rain, 2=snow, 3=fog, 4=storm, 5=cloudy, 6=windy |
| 12 | `temperature_f` | Float | `Temperature(F)` | `_clip_float(-40, 130)` | Nhiệt độ Fahrenheit | -40.0 đến 130.0 (default=50.0) |
| 13 | `humidity` | Float | `Humidity(%)` | `_clip_float(0, 100)` | Độ ẩm phần trăm | 0.0 đến 100.0 (default=50.0) |
| 14 | `wind_speed_mph` | Float | `Wind_Speed(mph)` | `_clip_float(0, 100)` | Tốc độ gió (dặm/giờ) | 0.0 đến 100.0 (default=0.0) |
| 15 | `visibility_mi` | Float | `Visibility(mi)` | `_clip_float(0, 10)` | Tầm nhìn (dặm) | 0.0 đến 10.0 (default=10.0) |
|  | **ROAD / CONTEXT** |  |  |  |  |  |
| 16 | `road_type_code` | Int | `Street` | `encode_road_type` | Mã hóa loại đường | 0=unknown, 1=interstate, 2=route, 3=street, 4=avenue, 5=boulevard, 6=drive, 7=road |
| 17 | `is_junction` | Int (0/1) | `Junction` | `_safe_bool_as_int` | Gần giao lộ? | 0=không, 1=có |
| 18 | `has_traffic_signal` | Int (0/1) | `Traffic_Signal` | `_safe_bool_as_int` | Có đèn giao thông? | 0=không, 1=có |
| 19 | `is_crossing` | Int (0/1) | `Crossing` | `_safe_bool_as_int` | Gần lối qua đường? | 0=không, 1=có |
| 20 | `is_roundabout` | Int (0/1) | `Roundabout` | `_safe_bool_as_int` | Gần vòng xuyến? | 0=không, 1=có |
| 21 | `is_stop` | Int (0/1) | `Stop` | `_safe_bool_as_int` | Gần biển STOP? | 0=không, 1=có |
| 22 | `is_station` | Int (0/1) | `Station` | `_safe_bool_as_int` | Gần trạm dừng? | 0=không, 1=có |
| 23 | `is_railway` | Int (0/1) | `Railway` | `_safe_bool_as_int` | Gần đường sắt? | 0=không, 1=có |
|  | **LIGHT** |  |  |  |  |  |
| 24 | `is_night` | Int (0/1) | `Sunrise_Sunset` | So sánh "night" | Trời tối? | 0=ban ngày, 1=ban đêm |