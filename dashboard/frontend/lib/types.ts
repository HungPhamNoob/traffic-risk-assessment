export type RiskLevel = "low" | "medium" | "high";

export type OverviewSummary = {
  total_events: number;
  high_risk_events: number;
  avg_risk_score: number;
  latest_event_time: string | null;
  latest_model_version: string;
};

export type PredictionPoint = {
  event_id: string;
  lat: number;
  lon: number;
  risk_score: number;
  predicted_severity: number | null;
  true_severity: number | null;
  event_time: string | null;
  model_status: string;
  risk_level: RiskLevel;
};

export type Hotspot = {
  rank: number;
  center_lat: number;
  center_lon: number;
  avg_risk_score: number;
  accident_count: number;
  severe_count: number;
  peak_hour: number | null;
};

export type ScenarioInput = {
  lat: number;
  lon: number;
  hour: number;
  day_of_week: number;
  is_weekend: number;
  is_rush_hour: number;
  weather_code: number;
  temperature_f: number;
  humidity: number;
  wind_speed_mph: number;
  visibility_mi: number;
  road_type_code: number;
  is_junction: number;
  has_traffic_signal: number;
  is_crossing: number;
  is_roundabout: number;
  is_stop: number;
  is_station: number;
  is_railway: number;
  is_night: number;
};

export type ScenarioResult = {
  predicted_severity: number;
  risk_score: number;
  risk_level: RiskLevel;
  model_name: string;
  model_version: string;
  model_status: string;
};
