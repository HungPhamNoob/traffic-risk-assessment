"use client";

import DeckGL from "@deck.gl/react";
import { HeatmapLayer } from "@deck.gl/aggregation-layers";
import { PolygonLayer, ScatterplotLayer } from "@deck.gl/layers";
import Map from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import type { Hotspot, PredictionPoint } from "@/lib/types";

const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

function colorForRisk(score: number): [number, number, number, number] {
  if (score >= 0.7) return [239, 68, 68, 220];
  if (score >= 0.4) return [245, 158, 11, 210];
  return [34, 197, 94, 190];
}

function triangleForPoint(point: PredictionPoint): [number, number][] {
  const size = 0.006;
  return [
    [point.lon, point.lat + size],
    [point.lon - size * 0.75, point.lat - size],
    [point.lon + size * 0.75, point.lat - size]
  ];
}

export function RiskMap({
  points,
  hotspots,
  selectedId,
  onSelect,
  showHeatmap
}: {
  points: PredictionPoint[];
  hotspots?: Hotspot[];
  selectedId?: string;
  onSelect?: (point: PredictionPoint) => void;
  showHeatmap: boolean;
}) {
  const first = points[0];
  const replayPoints = points.filter((point) => point.data_source !== "tomtom_live");
  const livePoints = points.filter((point) => point.data_source === "tomtom_live");
  const viewState = {
    longitude: first?.lon ?? -96,
    latitude: first?.lat ?? 38,
    zoom: first ? 7 : 3.1,
    pitch: 35,
    bearing: 0
  };

  const layers = [
    showHeatmap &&
      new HeatmapLayer<PredictionPoint>({
        id: "risk-heatmap",
        data: points,
        getPosition: (d) => [d.lon, d.lat],
        getWeight: (d) => Math.max(0.05, d.risk_score),
        radiusPixels: 70,
        intensity: 1.2,
        threshold: 0.02
      }),
    new ScatterplotLayer<PredictionPoint>({
      id: "us-replay-risk-points",
      data: replayPoints,
      pickable: true,
      opacity: 0.9,
      stroked: true,
      filled: true,
      radiusMinPixels: 5,
      radiusMaxPixels: 18,
      getPosition: (d) => [d.lon, d.lat],
      getRadius: (d) => (d.event_id === selectedId ? 240 : 120),
      getFillColor: (d) => colorForRisk(d.risk_score),
      getLineColor: [255, 255, 255, 210],
      lineWidthMinPixels: 1,
      onClick: (info) => {
        if (info.object && onSelect) onSelect(info.object);
      }
    }),
    new PolygonLayer<PredictionPoint>({
      id: "tomtom-live-risk-triangles",
      data: livePoints,
      pickable: true,
      stroked: true,
      filled: true,
      getPolygon: triangleForPoint,
      getFillColor: (d) => colorForRisk(d.risk_score),
      getLineColor: [255, 255, 255, 230],
      lineWidthMinPixels: 1,
      onClick: (info) => {
        if (info.object && onSelect) onSelect(info.object);
      }
    }),
    hotspots?.length
      ? new ScatterplotLayer<Hotspot>({
          id: "hotspot-centers",
          data: hotspots,
          pickable: false,
          stroked: true,
          filled: false,
          radiusMinPixels: 16,
          radiusMaxPixels: 36,
          getPosition: (d) => [d.center_lon, d.center_lat],
          getRadius: (d) => 260 + d.accident_count * 4,
          getLineColor: [56, 189, 248, 220],
          lineWidthMinPixels: 2
        })
      : null
  ].filter(Boolean);

  return (
    <DeckGL
      controller
      initialViewState={viewState}
      layers={layers}
      getTooltip={({ object }) => {
        const point = object as PredictionPoint | undefined;
        if (!point?.event_id) return null;
        const riskValue = Number(point.risk_score);
        const riskText = Number.isFinite(riskValue)
          ? riskValue.toFixed(3)
          : "N/A";
        if (point.data_source === "tomtom_live") {
          const severity =
            point.predicted_severity ?? point.true_severity ?? "N/A";
          return `${point.event_id}\nSeverity ${severity}\nDisplay risk ${riskText}`;
        }
        return `${point.event_id}\nRisk ${riskText}`;
      }}
    >
      <Map reuseMaps mapStyle={MAP_STYLE} />
    </DeckGL>
  );
}