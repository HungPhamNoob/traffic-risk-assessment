"use client";

import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { ArrowRight, FlaskConical } from "lucide-react";
import { api } from "@/lib/api";
import { scenarioPresets } from "@/lib/fallback";
import type { ScenarioInput, ScenarioResult } from "@/lib/types";
import { FallbackBanner, KpiCard } from "@/components/DataState";

const fieldLabels: Array<[keyof ScenarioInput, string, "number" | "flag"]> = [
  ["lat", "Latitude", "number"],
  ["lon", "Longitude", "number"],
  ["hour", "Hour", "number"],
  ["day_of_week", "Day of week", "number"],
  ["is_weekend", "Weekend", "flag"],
  ["is_rush_hour", "Rush hour", "flag"],
  ["weather_code", "Weather code", "number"],
  ["temperature_f", "Temp F", "number"],
  ["humidity", "Humidity", "number"],
  ["wind_speed_mph", "Wind mph", "number"],
  ["visibility_mi", "Visibility mi", "number"],
  ["road_type_code", "Road type", "number"],
  ["is_junction", "Junction", "flag"],
  ["has_traffic_signal", "Traffic signal", "flag"],
  ["is_crossing", "Crossing", "flag"],
  ["is_roundabout", "Roundabout", "flag"],
  ["is_stop", "Stop sign", "flag"],
  ["is_station", "Station", "flag"],
  ["is_railway", "Railway", "flag"],
  ["is_night", "Night", "flag"]
];

const extraPresets: Record<string, ScenarioInput> = {
  "Dry highway day": {
    lat: 40.8,
    lon: -73.9,
    hour: 14,
    day_of_week: 3,
    is_weekend: 0,
    is_rush_hour: 0,
    weather_code: 0,
    temperature_f: 75,
    humidity: 50,
    wind_speed_mph: 8,
    visibility_mi: 10,
    road_type_code: 1,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Snowy night highway": {
    lat: 41.5,
    lon: -75.0,
    hour: 1,
    day_of_week: 5,
    is_weekend: 0,
    is_rush_hour: 0,
    weather_code: 2,
    temperature_f: 22,
    humidity: 85,
    wind_speed_mph: 20,
    visibility_mi: 0.5,
    road_type_code: 1,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 1
  },
  "Foggy junction": {
    lat: 38.9,
    lon: -77.0,
    hour: 7,
    day_of_week: 2,
    is_weekend: 0,
    is_rush_hour: 1,
    weather_code: 3,
    temperature_f: 50,
    humidity: 95,
    wind_speed_mph: 5,
    visibility_mi: 0.1,
    road_type_code: 3,
    is_junction: 1,
    has_traffic_signal: 1,
    is_crossing: 1,
    is_roundabout: 0,
    is_stop: 1,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Rural dark crossing": {
    lat: 39.3,
    lon: -82.5,
    hour: 3,
    day_of_week: 1,
    is_weekend: 1,
    is_rush_hour: 0,
    weather_code: 0,
    temperature_f: 55,
    humidity: 70,
    wind_speed_mph: 10,
    visibility_mi: 3,
    road_type_code: 4,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 1,
    is_roundabout: 0,
    is_stop: 1,
    is_station: 0,
    is_railway: 1,
    is_night: 1
  },
  "Stormy interstate": {
    lat: 37.5,
    lon: -79.0,
    hour: 17,
    day_of_week: 4,
    is_weekend: 0,
    is_rush_hour: 1,
    weather_code: 4,
    temperature_f: 65,
    humidity: 90,
    wind_speed_mph: 30,
    visibility_mi: 0.2,
    road_type_code: 1,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 0
  },
  "Clear midnight residential": {
    lat: 40.2,
    lon: -74.5,
    hour: 0,
    day_of_week: 7,
    is_weekend: 1,
    is_rush_hour: 0,
    weather_code: 0,
    temperature_f: 45,
    humidity: 55,
    wind_speed_mph: 6,
    visibility_mi: 10,
    road_type_code: 5,
    is_junction: 0,
    has_traffic_signal: 0,
    is_crossing: 0,
    is_roundabout: 0,
    is_stop: 0,
    is_station: 0,
    is_railway: 0,
    is_night: 1
  }
};

const allPresets = { ...scenarioPresets, ...extraPresets };

export default function ScenarioPage() {
  const [scenario, setScenario] = useState<ScenarioInput>(
    allPresets["Normal commute"]
  );

  const predictMutation = useMutation({
    mutationFn: () => api.scenarioPredict(scenario) as Promise<ScenarioResult>
  });

  const updateScenario = (key: keyof ScenarioInput, value: number) => {
    setScenario((current) => ({ ...current, [key]: value }));
  };

  const renderFields = (data: ScenarioInput) => (
    <div className="field-grid">
      {fieldLabels.map(([key, label, type]) => (
        <div className="field" key={key}>
          <label>{label}</label>
          {type === "flag" ? (
            <select
              value={data[key]}
              onChange={(event) =>
                updateScenario(key, Number(event.target.value))
              }
            >
              <option value={0}>0</option>
              <option value={1}>1</option>
            </select>
          ) : (
            <input
              step="any"
              type="number"
              value={data[key]}
              onChange={(event) =>
                updateScenario(key, Number(event.target.value))
              }
            />
          )}
        </div>
      ))}
    </div>
  );

  const result = predictMutation.data;

  return (
    <div className="page-stack">
      <div className="page-title">
        <div>
          <h1>Scenario Simulator</h1>
          <p>Modify road and weather conditions to test risk predictions.</p>
        </div>
        <span className="status-pill">
          <FlaskConical size={14} />
          What-if inference
        </span>
      </div>

      <FallbackBanner
        active={result?.model_status === "heuristic_fallback"}
      />

      <section className="card">
        <div className="card-header">
          <h2 className="card-title">Presets</h2>
          <div className="toolbar" style={{ flexWrap: "wrap" }}>
            {Object.entries(allPresets).map(([name, preset]) => (
              <button
                className="ghost-button"
                key={name}
                onClick={() => setScenario(preset)}
                type="button"
              >
                {name}
              </button>
            ))}
          </div>
        </div>
      </section>

      <section className="card">
        <h2 className="card-title">Modified scenario</h2>
        {renderFields(scenario)}
      </section>

      <div className="control-row">
        <button
          className="primary-button"
          disabled={predictMutation.isPending}
          onClick={() => predictMutation.mutate()}
          type="button"
        >
          <ArrowRight size={16} /> Predict scenario
        </button>
      </div>

      {result ? (
        <section className="grid result-band">
          <KpiCard
            label="Risk score"
            value={result.risk_score.toFixed(4)}
            tone={result.risk_level}
          />
          <KpiCard label="Severity" value={result.predicted_severity} />
          <div className="card">
            <h2 className="card-title">Status</h2>
            <span className="status-pill" style={{ fontSize: "1.1rem" }}>
              {result.model_status === "heuristic_fallback"
                ? "failed"
                : "success"}
            </span>
          </div>
        </section>
      ) : null}
    </div>
  );
}