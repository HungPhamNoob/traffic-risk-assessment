"""
schema_definitions.py
======================
PySpark StructType schemas cho từng nguồn dữ liệu.

Mục đích:
  - Đọc CSV với schema tường minh (không infer) để tránh lỗi type mismatch
  - Làm nguồn tham chiếu chung cho tất cả các job

Sources:
  - US Accidents: Kaggle dataset "US-Accidents" (Moosavi et al.)
  - UK Accidents: STATS19 dataset từ UK Dept for Transport
  - Internal Silver schema (sau khi clean)
"""
from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── US Accidents (Kaggle) ─────────────────────────────────────────────────
# Chỉ khai báo các cột sẽ dùng — PySpark sẽ bỏ qua cột ngoài schema
US_BRONZE_SCHEMA = StructType([
    StructField("ID",                  StringType(),    True),
    StructField("Severity",            IntegerType(),   True),
    StructField("Start_Time",          StringType(),    True),  # parse sau
    StructField("End_Time",            StringType(),    True),
    StructField("Start_Lat",           DoubleType(),    True),
    StructField("Start_Lng",           DoubleType(),    True),
    StructField("End_Lat",             DoubleType(),    True),
    StructField("End_Lng",             DoubleType(),    True),
    StructField("Distance(mi)",        DoubleType(),    True),
    StructField("Description",         StringType(),    True),
    StructField("Street",              StringType(),    True),
    StructField("City",                StringType(),    True),
    StructField("County",              StringType(),    True),
    StructField("State",               StringType(),    True),
    StructField("Zipcode",             StringType(),    True),
    StructField("Country",             StringType(),    True),
    StructField("Timezone",            StringType(),    True),
    StructField("Temperature(F)",      DoubleType(),    True),
    StructField("Wind_Chill(F)",       DoubleType(),    True),
    StructField("Humidity(%)",         DoubleType(),    True),
    StructField("Pressure(in)",        DoubleType(),    True),
    StructField("Visibility(mi)",      DoubleType(),    True),
    StructField("Wind_Direction",      StringType(),    True),
    StructField("Wind_Speed(mph)",     DoubleType(),    True),
    StructField("Precipitation(in)",   DoubleType(),    True),
    StructField("Weather_Condition",   StringType(),    True),
    StructField("Amenity",             StringType(),    True),
    StructField("Junction",            StringType(),    True),
    StructField("Traffic_Signal",      StringType(),    True),
    StructField("Sunrise_Sunset",      StringType(),    True),
    StructField("Civil_Twilight",      StringType(),    True),
])

# ─── UK Collisions (STATS19 - phiên bản mới, cột đổi accident_* → collision_*) ──
# Header thực tế từ file: Road Safety Data - Collisions
# Chỉ khai báo cột cần dùng — Spark đọc theo tên header, cột thừa tự bỏ qua
UK_BRONZE_SCHEMA = StructType([
    # ── Identifier ──────────────────────────────────────────────────────────
    StructField("collision_index",      StringType(),    True),  # thay accident_index
    StructField("collision_year",       IntegerType(),   True),  # thay accident_year
    StructField("collision_ref_no",     StringType(),    True),  # thay accident_reference

    # ── Tọa độ (dùng lon/lat, bỏ OSGR vì đã có lat/lon) ────────────────────
    StructField("longitude",            DoubleType(),    True),
    StructField("latitude",             DoubleType(),    True),

    # ── Thông tin cơ bản ─────────────────────────────────────────────────────
    StructField("police_force",                         IntegerType(),   True),
    StructField("collision_severity",                   IntegerType(),   True),  # thay accident_severity
    StructField("number_of_vehicles",                   IntegerType(),   True),
    StructField("number_of_casualties",                 IntegerType(),   True),

    # ── Thời gian ─────────────────────────────────────────────────────────────
    StructField("date",                                 StringType(),    True),
    StructField("day_of_week",                          IntegerType(),   True),
    StructField("time",                                 StringType(),    True),

    # ── Địa lý / hành chính ──────────────────────────────────────────────────
    StructField("local_authority_district",             IntegerType(),   True),
    StructField("local_authority_ons_district",         StringType(),    True),  # cột mới
    StructField("local_authority_highway",              StringType(),    True),  # cột mới
    StructField("urban_or_rural_area",                  IntegerType(),   True),
    StructField("lsoa_of_accident_location",            StringType(),    True),  # cột mới

    # ── Đường và điều kiện giao thông ────────────────────────────────────────
    StructField("first_road_class",                     IntegerType(),   True),
    StructField("first_road_number",                    IntegerType(),   True),  # cột mới
    StructField("road_type",                            IntegerType(),   True),
    StructField("speed_limit",                          IntegerType(),   True),
    # junction_detail_historic và junction_detail đều có → dùng junction_detail (mới hơn)
    StructField("junction_detail",                      IntegerType(),   True),
    StructField("junction_control",                     IntegerType(),   True),  # cột mới
    StructField("second_road_class",                    IntegerType(),   True),  # cột mới
    StructField("pedestrian_crossing",                  IntegerType(),   True),  # cột mới (thay thế 2 cột historic)
    StructField("light_conditions",                     IntegerType(),   True),
    StructField("weather_conditions",                   IntegerType(),   True),
    StructField("road_surface_conditions",              IntegerType(),   True),
    StructField("special_conditions_at_site",           IntegerType(),   True),  # cột mới
    StructField("carriageway_hazards",                  IntegerType(),   True),  # cột mới
    StructField("trunk_road_flag",                      IntegerType(),   True),  # cột mới

    # ── Phân loại nghiêm trọng bổ sung (phiên bản mới thêm) ─────────────────
    StructField("did_police_officer_attend_scene_of_accident", IntegerType(), True),
    StructField("enhanced_severity_collision",          IntegerType(),   True),  # cột mới
    StructField("collision_injury_based",               IntegerType(),   True),  # cột mới
])

# ─── Internal Silver Schema (unified sau clean) ────────────────────────────
# Đây là schema chuẩn cho mọi output ở silver layer
SILVER_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),  # NOT NULL
    StructField("source",           StringType(),    False),  # "us" | "uk" | "tomtom"
    StructField("event_time",       TimestampType(), True),
    StructField("lat",              DoubleType(),    True),
    StructField("lon",              DoubleType(),    True),
    StructField("severity",         IntegerType(),   True),   # 1-4 (1=lowest)
    StructField("weather_code",     IntegerType(),   True),   # mapped numeric
    StructField("road_type",        StringType(),    True),   # "motorway","urban","rural"
    StructField("state_or_region",  StringType(),    True),
    StructField("city",             StringType(),    True),
    StructField("description",      StringType(),    True),
    # Thêm ngày partition để dễ filter
    StructField("event_date",       StringType(),    True),   # "YYYY-MM-DD"
])

# ─── Gold Feature Schema ───────────────────────────────────────────────────
GOLD_FEATURE_SCHEMA = StructType([
    StructField("event_id",          StringType(),    False),
    StructField("source",            StringType(),    False),
    StructField("event_time",        TimestampType(), True),
    StructField("lat",               DoubleType(),    True),
    StructField("lon",               DoubleType(),    True),
    StructField("severity",          IntegerType(),   True),
    StructField("weather_code",      IntegerType(),   True),
    StructField("road_type",         StringType(),    True),
    # Temporal features
    StructField("hour_of_day",       IntegerType(),   True),
    StructField("day_of_week",       IntegerType(),   True),  # 1=Mon .. 7=Sun
    StructField("month",             IntegerType(),   True),
    StructField("is_weekend",        IntegerType(),   True),  # 0/1
    StructField("season",            StringType(),    True),  # "spring/summer/autumn/winter"
    StructField("is_rush_hour",      IntegerType(),   True),  # 0/1
    StructField("is_night",          IntegerType(),   True),  # 0/1
    # Spatial features
    StructField("h3_index_res8",     StringType(),    True),  # H3 resolution 8
    StructField("h3_index_res6",     StringType(),    True),  # H3 resolution 6 (coarser)
    StructField("event_date",        StringType(),    True),
])


def get_schema(name: str) -> StructType:
    """Lấy schema theo tên."""
    mapping = {
        "us_bronze":    US_BRONZE_SCHEMA,
        "uk_bronze":    UK_BRONZE_SCHEMA,
        "silver":       SILVER_SCHEMA,
        "gold_feature": GOLD_FEATURE_SCHEMA,
    }
    if name not in mapping:
        raise ValueError(f"Unknown schema: {name}. Available: {list(mapping)}")
    return mapping[name]
