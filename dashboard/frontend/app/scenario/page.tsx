"use client";

import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { ArrowRight, FlaskConical, GitCompare } from "lucide-react";
import { api } from "@/lib/api";
import { scenarioPresets } from "@/lib/fallback";
import type { ScenarioInput, ScenarioResult } from "@/lib/types";
import { FallbackBanner, KpiCard, RiskBadge } from "@/components/DataState";

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

export default function ScenarioPage() {
  const [baseline, setBaseline] = useState<ScenarioInput>(
    scenarioPresets["Normal commute"]
  );
  const [scenario, setScenario] = useState<ScenarioInput>(
    scenarioPresets["Rainy rush hour"]
  );

  const predictMutation = useMutation({
    mutationFn: () => api.scenarioPredict(scenario) as Promise<ScenarioResult>
  });
  const compareMutation = useMutation({
    mutationFn: () =>
      api.scenarioCompare(baseline, scenario) as Promise<{
        baseline: ScenarioResult;
        scenario: ScenarioResult;
        delta: {
          risk_score_change: number;
          risk_percent_change: number;
          severity_change: number;
        };
      }>
  });

  const updateScenario = (
    target: "baseline" | "scenario",
    key: keyof ScenarioInput,
    value: number
  ) => {
    const setter = target === "baseline" ? setBaseline : setScenario;
    setter((current) => ({ ...current, [key]: value }));
  };

  const renderFields = (target: "baseline" | "scenario", data: ScenarioInput) => (
    <div className="field-grid">
      {fieldLabels.map(([key, label, type]) => (
        <div className="field" key={`${target}-${key}`}>
          <label>{label}</label>
          {type === "flag" ? (
            <select
              value={data[key]}
              onChange={(event) =>
                updateScenario(target, key, Number(event.target.value))
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
                updateScenario(target, key, Number(event.target.value))
              }
            />
          )}
        </div>
      ))}
    </div>
  );

  const result = predictMutation.data;
  const compare = compareMutation.data;
  const fallbackActive =
    result?.model_status === "heuristic_fallback" ||
    compare?.baseline.model_status === "heuristic_fallback" ||
    compare?.scenario.model_status === "heuristic_fallback";

  return (
    <div className="page-stack">
      <div className="page-title">
        <div>
          <h1>Scenario Simulator</h1>
          <p>Compare baseline and modified road conditions against the model.</p>
        </div>
        <span className="status-pill">
          <FlaskConical size={14} />
          What-if inference
        </span>
      </div>

      <FallbackBanner active={fallbackActive} />

      <section className="card">
        <div className="card-header">
          <h2 className="card-title">Presets</h2>
          <div className="toolbar">
            {Object.entries(scenarioPresets).map(([name, preset]) => (
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

      <section className="grid scenario-grid">
        <div className="card">
          <h2 className="card-title">Baseline feature vector</h2>
          {renderFields("baseline", baseline)}
        </div>
        <div className="card">
          <h2 className="card-title">Modified scenario</h2>
          {renderFields("scenario", scenario)}
        </div>
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
        <button
          className="ghost-button"
          disabled={compareMutation.isPending}
          onClick={() => compareMutation.mutate()}
          type="button"
        >
          <GitCompare size={16} /> Compare baseline
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
            <h2 className="card-title">Model status</h2>
            <RiskBadge level={result.risk_level} />
            <p className="muted">{result.model_status}</p>
          </div>
        </section>
      ) : null}

      {compare ? (
        <section className="grid result-band">
          <KpiCard
            label="Baseline risk"
            value={compare.baseline.risk_score.toFixed(4)}
            tone={compare.baseline.risk_level}
          />
          <KpiCard
            label="Scenario risk"
            value={compare.scenario.risk_score.toFixed(4)}
            tone={compare.scenario.risk_level}
          />
          <KpiCard
            label="Risk delta"
            value={`${compare.delta.risk_score_change >= 0 ? "+" : ""}${compare.delta.risk_score_change}`}
            detail={`${compare.delta.risk_percent_change}% risk, severity ${compare.delta.severity_change >= 0 ? "+" : ""}${compare.delta.severity_change}`}
          />
        </section>
      ) : null}
    </div>
  );
}
