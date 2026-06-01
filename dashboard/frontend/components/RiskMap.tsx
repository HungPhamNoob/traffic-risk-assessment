"use client";

import DeckGL from "@deck.gl/react";
import { HeatmapLayer } from "@deck.gl/aggregation-layers";
import { GeoJsonLayer, PolygonLayer, ScatterplotLayer } from "@deck.gl/layers";
import type { StyleSpecification } from "maplibre-gl";
import { memo, useEffect, useMemo, useRef, useState } from "react";
import Map from "react-map-gl/maplibre";
import { feature } from "topojson-client";
import countriesTopology from "world-atlas/countries-110m.json";
import "maplibre-gl/dist/maplibre-gl.css";
import type { Hotspot, PredictionPoint } from "@/lib/types";

const COUNTRY_FEATURES = feature(
  countriesTopology as any,
  (countriesTopology as any).objects.countries
) as any;

const MAP_STYLE: StyleSpecification = {
  version: 8,
  name: "traffic-risk-local",
  sources: {},
  layers: [
    {
      id: "background",
      type: "background",
      paint: {
        "background-color": "#020617"
      }
    }
  ]
};

type RiskMapViewState = {
  longitude: number;
  latitude: number;
  zoom: number;
  pitch: number;
  bearing: number;
};

const DEFAULT_VIEW_STATE: RiskMapViewState = {
  longitude: -96,
  latitude: 38,
  zoom: 3.1,
  pitch: 35,
  bearing: 0
};

function colorForRisk(score: number): [number, number, number, number] {
  if (score >= 0.7) return [239, 68, 68, 220];
  if (score >= 0.4) return [245, 158, 11, 210];
  return [34, 197, 94, 190];
}

function colorForSeverity(point: PredictionPoint): [number, number, number, number] {
  const severity = point.predicted_severity ?? point.true_severity ?? null;
  if (severity === 4) return [239, 68, 68, 225];
  if (severity === 3) return [249, 115, 22, 220];
  if (severity === 2) return [234, 179, 8, 210];
  if (severity === 1) return [34, 197, 94, 200];
  return colorForRisk(point.risk_score);
}

function triangleForPoint(point: PredictionPoint): [number, number][] {
  const size = 0.006;
  return [
    [point.lon, point.lat + size],
    [point.lon - size * 0.75, point.lat - size],
    [point.lon + size * 0.75, point.lat - size]
  ];
}

const RiskMapInner = memo(function RiskMapInner({
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

  /* Split points once using useMemo so the arrays are referentially
     stable across renders unless the actual point list changes. */
  const { replayPoints, livePoints } = useMemo(() => {
    const replay: PredictionPoint[] = [];
    const live: PredictionPoint[] = [];
    for (const p of points) {
      if (p.data_source === "tomtom_live") live.push(p);
      else replay.push(p);
    }
    return { replayPoints: replay, livePoints: live };
  }, [points]);

  const [initialViewState, setInitialViewState] =
    useState<RiskMapViewState>(DEFAULT_VIEW_STATE);
  const hasUserAdjustedView = useRef(false);
  const initialPositionDone = useRef(false);

  useEffect(() => {
    if (!first || initialPositionDone.current || hasUserAdjustedView.current) {
      return;
    }
    initialPositionDone.current = true;
    setInitialViewState((current) => ({
      ...current,
      longitude: first.lon,
      latitude: first.lat,
      zoom: 7
    }));
  }, [first]);

  /* Memoize DeckGL layers so they are only recreated when their data
     or configuration actually changes – not on every parent re-render. */
  const layers = useMemo(() => [
    new GeoJsonLayer({
      id: "country-basemap",
      data: COUNTRY_FEATURES,
      pickable: false,
      stroked: true,
      filled: true,
      opacity: 0.92,
      getFillColor: [9, 20, 36, 230],
      getLineColor: [71, 85, 105, 235],
      lineWidthMinPixels: 1
    }),
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
      getFillColor: (d) => colorForSeverity(d),
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
      getFillColor: (d) => colorForSeverity(d),
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
  ].filter(Boolean), [points, replayPoints, livePoints, hotspots, selectedId, showHeatmap, onSelect]);

  return (
    <DeckGL
      controller
      layers={layers}
      initialViewState={initialViewState}
      onViewStateChange={({ interactionState }) => {
        const userIsInteracting = Boolean(
          interactionState?.isDragging ||
            interactionState?.isPanning ||
            interactionState?.isZooming ||
            interactionState?.isRotating
        );
        if (userIsInteracting) {
          hasUserAdjustedView.current = true;
        }
      }}
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
        const predictedSeverity = point.predicted_severity ?? "N/A";
        const trueSeverity = point.true_severity ?? "N/A";
        return `${point.event_id}\nPredicted severity ${predictedSeverity}\nTrue severity ${trueSeverity}\nRisk ${riskText}`;
      }}
    >
      <Map reuseMaps mapStyle={MAP_STYLE} attributionControl={false} />
    </DeckGL>
  );
});

export function RiskMap(props: {
  points: PredictionPoint[];
  hotspots?: Hotspot[];
  selectedId?: string;
  onSelect?: (point: PredictionPoint) => void;
  showHeatmap: boolean;
}) {
  return <RiskMapInner {...props} />;
}
