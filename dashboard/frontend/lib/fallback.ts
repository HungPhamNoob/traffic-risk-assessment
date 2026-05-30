import type { Hotspot, OverviewSummary, PredictionPoint, ScenarioInput } from "./types";

export const fallbackSummary: OverviewSummary = {
  total_events: 18240,
  high_risk_events: 3140,
  avg_risk_score: 0.4685,
  latest_event_time: "2022-09-08T17:36:39",
  latest_model_version: "US H2O + TomTom rule-based"
};

export const fallbackPoints: PredictionPoint[] = [
  {
    event_id: "A-512429",
    lat: 35.345188,
    lon: -80.790482,
    risk_score: 0.4658298959,
    predicted_severity: 2,
    true_severity: 1,
    event_time: "2022-09-08T17:36:39",
    model_status: "ok",
    risk_level: "medium"
  },
  {
    event_id: "A-512426",
    lat: 35.144791,
    lon: -80.826439,
    risk_score: 0.4655608505,
    predicted_severity: 2,
    true_severity: 1,
    event_time: "2022-09-08T17:17:44",
    model_status: "ok",
    risk_level: "medium"
  },
  {
    event_id: "A-demo-high",
    lat: 34.0522,
    lon: -118.2437,
    risk_score: 0.82,
    predicted_severity: 4,
    true_severity: 3,
    event_time: "2022-09-08T18:02:00",
    model_status: "ok",
    risk_level: "high"
  },
  {
    event_id: "A-demo-low",
    lat: 39.7392,
    lon: -104.9903,
    risk_score: 0.18,
    predicted_severity: 1,
    true_severity: 1,
    event_time: "2022-09-08T13:25:00",
    model_status: "ok",
    risk_level: "low"
  },
  {
    event_id: "tomtom-demo-1",
    lat: 40.73061,
    lon: -73.935242,
    risk_score: 0.5,
    predicted_severity: 3,
    true_severity: 3,
    event_time: "2026-05-26T10:31:06",
    model_status: "rule_based",
    risk_level: "medium",
    data_source: "tomtom_live",
    marker_shape: "triangle"
  }
];

export const fallbackHotspots: Hotspot[] = [
  {
    rank: 1,
    center_lat: 35.345,
    center_lon: -80.79,
    avg_risk_score: 0.72,
    accident_count: 42,
    severe_count: 18,
    peak_hour: 17
  },
  {
    rank: 2,
    center_lat: 34.052,
    center_lon: -118.244,
    avg_risk_score: 0.66,
    accident_count: 31,
    severe_count: 12,
    peak_hour: 8
  }
];

export const scenarioPresets: Record<string, ScenarioInput> = {
  "Normal commute": {
    lat: 35.345188,
    lon: -80.790482,
    hour: 8,
    day_of_week: 2,
    is_weekend: 0,
    is_rush_hour: 1,
    weather_code: 0,
    temperature_f: 68,
    humidity: 55,
    wind_speed_mph: 5,
    visibility_mi: 10,
    road_type_code: 3,
    is_junction: 0,
    has_traffic_signal: 1,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Rainy rush hour": {
    lat: 35.345188,
    lon: -80.790482,
    hour: 17,
    day_of_week: 5,
    is_weekend: 0,
    is_rush_hour: 1,
    weather_code: 1,
    temperature_f: 52,
    humidity: 91,
    wind_speed_mph: 18,
    visibility_mi: 3,
    road_type_code: 1,
    is_junction: 1,
    has_traffic_signal: 1,
    is_crossing: 1,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Night junction": {
    lat: 39.7392,
    lon: -104.9903,
    hour: 23,
    day_of_week: 6,
    is_weekend: 0,
    is_rush_hour: 0,
    weather_code: 3,
    temperature_f: 42,
    humidity: 76,
    wind_speed_mph: 12,
    visibility_mi: 4,
    road_type_code: 4,
    is_junction: 1,
    has_traffic_signal: 1,
    is_crossing: 1,
    is_roundabout: 0,
    is_stop: 1,
    is_station: 0,
    is_railway: 0,
    is_night: 1
  }
};
