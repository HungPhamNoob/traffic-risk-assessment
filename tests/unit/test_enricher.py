"""Unit tests for US-only feature engineering."""

from processing.feature_engineering import (
    encode_road_type,
    encode_weather_condition,
    parse_datetime,
)


def test_weather_encoder_groups_common_us_conditions():
    """Weather descriptions should map to stable model codes."""
    assert encode_weather_condition("Light Rain") == 1
    assert encode_weather_condition("Snow / Windy") == 2
    assert encode_weather_condition("Fog") == 3
    assert encode_weather_condition("Thunderstorm") == 4
    assert encode_weather_condition("Overcast") == 5


def test_road_encoder_detects_highway_and_local_road_types():
    """Street names should be converted into compact road type codes."""
    assert encode_road_type("I-70 E") == 1
    assert encode_road_type("US-101 N") == 2
    assert encode_road_type("Main Street") == 3
    assert encode_road_type("Broadway Avenue") == 4


def test_parse_datetime_accepts_us_dataset_formats():
    """Timestamp parsing should support the formats present in the US dataset."""
    assert parse_datetime("2016-02-08 05:46:00").year == 2016
    assert parse_datetime("2020-01-01T12:30:45").hour == 12
