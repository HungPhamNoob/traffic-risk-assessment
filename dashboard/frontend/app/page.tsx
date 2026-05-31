"use client";

import dynamic from "next/dynamic";
import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { Layers, RefreshCw, Search } from "lucide-react";
import { api } from "@/lib/api";
import { fallbackHotspots, fallbackPoints, fallbackSummary } from "@/lib/fallback";
import type { Hotspot, MapMode, OverviewSummary, PredictionPoint } from "@/lib/types";
import { FallbackBanner, KpiCard } from "@/components/DataState";

const RiskMap = dynamic(
  () => import("@/components/RiskMap").then((module) => module.RiskMap),
  { ssr: false }
);

export default function DashboardPage() {
  const [minRisk, setMinRisk] = useState(0);
  const [showHeatmap, setShowHeatmap] = useState(true);
  const [selected, setSelected] = useState<PredictionPoint | null>(null);
  const [mode, setMode] = useState<MapMode>("full");

  const summaryQuery = useQuery({
    queryKey: ["overview", mode],
    queryFn: () => api.overview(mode),
    refetchInterval: 10_000
  });
  const pointsQuery = useQuery({
    queryKey: ["points", minRisk, mode],
    queryFn: () => api.mapPoints({ limit: 5000, min_risk: minRisk, mode }),
    refetchInterval: 10_000
  });
  const latestQuery = useQuery({
    queryKey: ["latest", mode],
    queryFn: () => api.latest(10, mode),
    refetchInterval: 10_000
  });
  const hotspotsQuery = useQuery({
    queryKey: ["hotspots"],
    queryFn: () => api.hotspots({ limit: 10, min_events: 1 }),
    refetchInterval: 10_000
  });
  const riskByHourQuery = useQuery({
    queryKey: ["risk-by-hour", mode],
    queryFn: () => api.riskByHour(mode),
    refetchInterval: 300_000
  });
  const severityQuery = useQuery({
    queryKey: ["severity", mode],
    queryFn: () => api.severityDistribution(mode),
    refetchInterval: 300_000
  });
  const weatherQuery = useQuery({
    queryKey: ["weather-histogram", mode],
    queryFn: () => api.weatherHistogram(mode),
    refetchInterval: 300_000
  });

  const summary =
    (summaryQuery.data as OverviewSummary | undefined) || fallbackSummary;
  const points =
    ((pointsQuery.data?.points as PredictionPoint[] | undefined) || []).length > 0
      ? (pointsQuery.data?.points as PredictionPoint[])
      : fallbackPoints;
  const latest =
    ((latestQuery.data?.predictions as PredictionPoint[] | undefined) || []).length > 0
      ? (latestQuery.data?.predictions as PredictionPoint[])
      : points.slice(0, 8);
  const hotspots =
    ((hotspotsQuery.data?.hotspots as Hotspot[] | undefined) || []).length > 0
      ? (hotspotsQuery.data?.hotspots as Hotspot[])
      : fallbackHotspots;
  const fallbackActive =
    summaryQuery.isError ||
    pointsQuery.isError ||
    (pointsQuery.data?.points as unknown[] | undefined)?.length === 0;

  const highRiskPct = useMemo(() => {
    if (!summary.total_events) return "0%";
    return `${Math.round((summary.high_risk_events / summary.total_events) * 100)}%`;
  }, [summary.high_risk_events, summary.total_events]);

  const riskByHour =
    (riskByHourQuery.data?.data as Array<Record<string, number>> | undefined) || [];
  const severity =
    (severityQuery.data?.distribution as Array<Record<string, number>> | undefined) ||
    [];
  const weatherHistogram =
    (weatherQuery.data?.histogram as Record<string, Array<{ bin: string; count: number }>> | undefined) || {};

  const statusText = (point: PredictionPoint) =>
    point.model_status === "success" || point.model_status === "failed"
      ? point.model_status
      : point.model_status
        ? "success"
        : "failed";

  return (
    <div className="page-stack">
      <div className="page-title">
        <div>
          <h1>Realtime Risk Dashboard</h1>
          <p>Live accident risk map, hotspot ranking, and model output overview.</p>
        </div>
        <span className="status-pill">
          <RefreshCw size={14} />
          Auto-refresh 30s
        </span>
      </div>

      <FallbackBanner active={fallbackActive} />

      <section className="grid kpi-grid">
        <KpiCard label="Total events" value={summary.total_events.toLocaleString()} />
        <KpiCard
          label="High risk"
          value={summary.high_risk_events.toLocaleString()}
          tone="high"
          detail={highRiskPct}
        />
        <KpiCard
          label="Average risk"
          value={Number(summary.avg_risk_score || 0).toFixed(4)}
          tone={summary.avg_risk_score >= 0.7 ? "high" : "medium"}
        />
        <KpiCard
          label="Latest event"
          value={summary.latest_event_time ? "Online" : "No data"}
          detail={summary.latest_event_time || "Waiting for replay"}
        />
        <KpiCard
          label="Model"
          value={summary.latest_model_version || "latest"}
        />
      </section>

      <section className="grid dashboard-grid">
        <div className="map-frame">
          <div className="map-canvas">
            <RiskMap
              points={points}
              hotspots={hotspots}
              selectedId={selected?.event_id}
              showHeatmap={showHeatmap}
              onSelect={setSelected}
            />
          </div>
          <div className="map-overlay">
            <div className="toolbar">
              <Search size={16} />
              <label className="muted">Min risk</label>
              <input
                max={1}
                min={0}
                step={0.05}
                type="range"
                value={minRisk}
                onChange={(event) => setMinRisk(Number(event.target.value))}
              />
              <span className="mono">{minRisk.toFixed(2)}</span>
            </div>
            <div className="toolbar mode-switcher" aria-label="Map mode">
              {(["replay", "live", "full"] as MapMode[]).map((item) => (
                <button
                  className={mode === item ? "ghost-button active" : "ghost-button"}
                  key={item}
                  onClick={() => {
                    setMode(item);
                    setSelected(null);
                  }}
                  type="button"
                >
                  {item === "replay" ? "Replay ●" : item === "live" ? "Live ▲" : "Full ●▲"}
                </button>
              ))}
            </div>
            <button
              className="ghost-button"
              onClick={() => setShowHeatmap((value) => !value)}
              type="button"
            >
              <Layers size={16} /> {showHeatmap ? "Heatmap on" : "Points only"}
            </button>
          </div>
        </div>

        <aside className="grid">
          <section className="card">
            <h2 className="card-title">Active hotspots</h2>
            <div className="side-list">
              {hotspots.map((hotspot) => (
                <button
                  className="row-item"
                  key={`${hotspot.rank}-${hotspot.center_lat}`}
                  onClick={() =>
                    setSelected({
                      event_id: `hotspot-${hotspot.rank}`,
                      lat: hotspot.center_lat,
                      lon: hotspot.center_lon,
                      risk_score: hotspot.avg_risk_score,
                      predicted_severity: null,
                      true_severity: null,
                      event_time: null,
                      model_status: "hotspot",
                      data_source: "us_replay",
                      marker_shape: "circle",
                      risk_level:
                        hotspot.avg_risk_score >= 0.7
                          ? "high"
                          : hotspot.avg_risk_score >= 0.4
                            ? "medium"
                            : "low"
                    })
                  }
                  type="button"
                >
                  <div className="row-top">
                    <strong>#{hotspot.rank}</strong>
                    <span>{Number(hotspot.avg_risk_score).toFixed(3)}</span>
                  </div>
                  <span className="muted">
                    {hotspot.accident_count} events, peak {hotspot.peak_hour ?? "-"}h
                  </span>
                </button>
              ))}
            </div>
          </section>
        </aside>
      </section>

      <section className="grid analytics-grid">
        <div className="card">
          <h2 className="card-title">Risk by hour ({mode})</h2>
          <div className="chart-box">
            <ResponsiveContainer>
              <LineChart data={riskByHour}>
                <CartesianGrid stroke="rgba(148,163,184,0.14)" />
                <XAxis dataKey="hour" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip />
                <Line dataKey="avg_risk_score" stroke="#38bdf8" strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="card">
          <h2 className="card-title">Severity distribution ({mode})</h2>
          <div className="chart-box">
            <ResponsiveContainer>
              <BarChart data={severity}>
                <CartesianGrid stroke="rgba(148,163,184,0.14)" />
                <XAxis dataKey="severity" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip />
                <Bar dataKey="count" fill="#f59e0b" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </section>

      <section className="grid analytics-grid" style={{ gridTemplateColumns: "1fr 1fr 1fr" }}>
        {["temperature", "humidity", "wind_speed"].map((metric) => {
          const data = weatherHistogram[metric] || [];
          return (
            <div className="card" key={metric}>
              <h2 className="card-title">
                {metric.replace("_", " ").replace(/\b\w/g, (c) => c.toUpperCase())} ({mode})
              </h2>
              <div className="chart-box">
                <ResponsiveContainer>
                  <BarChart data={data}>
                    <CartesianGrid stroke="rgba(148,163,184,0.14)" />
                    <XAxis dataKey="bin" stroke="#94a3b8" fontSize={10} />
                    <YAxis stroke="#94a3b8" fontSize={10} />
                    <Tooltip />
                    <Bar dataKey="count" fill="#22c55e" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          );
        })}
      </section>

      <section className="card">
        <h2 className="card-title">Latest predictions</h2>
        <table className="table">
          <thead>
            <tr>
              <th>Event</th>
              <th>Risk</th>
              <th>Severity</th>
              <th>Time</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {latest.map((point) => (
              <tr key={point.event_id}>
                <td className="mono">{point.event_id}</td>
                <td>{Number(point.risk_score).toFixed(4)}</td>
                <td>{point.predicted_severity ?? "-"}</td>
                <td>{point.event_time || "-"}</td>
                <td>{statusText(point)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
