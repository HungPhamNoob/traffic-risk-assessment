"""Scenario simulator request and response schemas."""

from pydantic import BaseModel, Field


class ScenarioInput(BaseModel):
    """Feature vector accepted by the what-if risk simulator."""

    lat: float
    lon: float
    hour: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=1, le=7)
    is_weekend: int = Field(ge=0, le=1)
    is_rush_hour: int = Field(ge=0, le=1)
    weather_code: int
    temperature_f: float
    humidity: float
    wind_speed_mph: float
    visibility_mi: float
    road_type_code: int
    is_junction: int = Field(ge=0, le=1)
    has_traffic_signal: int = Field(ge=0, le=1)
    is_crossing: int = Field(ge=0, le=1)
    is_roundabout: int = Field(ge=0, le=1)
    is_stop: int = Field(ge=0, le=1)
    is_station: int = Field(ge=0, le=1)
    is_railway: int = Field(ge=0, le=1)
    is_night: int = Field(ge=0, le=1)


class ScenarioCompareRequest(BaseModel):
    """Baseline and modified feature vectors for risk comparison."""

    baseline: ScenarioInput
    scenario: ScenarioInput
