"use client";

import maplibregl, { Map as MapLibreMap, Popup } from "maplibre-gl";

const SOURCE = "audit-areas";
const FILL_LAYER = "audit-fill";
const LINE_LAYER = "audit-line";
const HOVER_FILL = "audit-fill-hover";
const HOVER_LINE = "audit-line-hover";

export interface FeatureStyle {
  fillColor: string;
  fillOpacity: number;
  strokeColor: string;
  strokeWidth: number;
  strokeOpacity: number;
}

export interface RenderOptions {
  isProvinceView: boolean;
  fitBounds: boolean;
  onAreaClick: (areaKey: string) => void;
  getFeatureStyle: (feature: GeoJSON.Feature) => FeatureStyle;
  getPopupHtml: (areaKey: string) => string | null;
  styleUrl?: string;
  hoverStrokeColor?: string;
}

export interface AuditMapController {
  render(
    container: HTMLElement,
    geo: GeoJSON.FeatureCollection,
    options: RenderOptions,
    onReady?: () => void
  ): void;
  refresh(
    geo: GeoJSON.FeatureCollection,
    getFeatureStyle: (feature: GeoJSON.Feature) => FeatureStyle
  ): void;
  /** Panggil saat ukuran kontainer berubah (flex/layout, tab, resize window). */
  resize(): void;
  zoomIn(): void;
  zoomOut(): void;
  fitAll(geo: GeoJSON.FeatureCollection): void;
  closePopup(): void;
  destroy(): void;
}

export function createAuditMap(): AuditMapController {
  let map: MapLibreMap | null = null;
  let popup: Popup | null = null;
  let hoveredId: number | string | null = null;
  let isProvinceView = false;
  let onAreaClick: ((areaKey: string) => void) | null = null;
  let getPopupHtml: ((areaKey: string) => string | null) | null = null;
  let currentStyleUrl: string | null = null;
  let hoverStrokeColor = "#111827";
  let eventsAttached = false;

  function getFeatureAreaKey(
    props: GeoJSON.GeoJsonProperties | null | undefined
  ): string {
    if (!props) return "";
    return isProvinceView
      ? String(props.provinceKey ?? "")
      : String(props.regionKey ?? "");
  }

  function buildStyledGeo(
    geo: GeoJSON.FeatureCollection,
    getFeatureStyle: (feature: GeoJSON.Feature) => FeatureStyle
  ): GeoJSON.FeatureCollection {
    return {
      type: "FeatureCollection",
      features: geo.features.map((feature) => ({
        type: "Feature",
        geometry: feature.geometry,
        properties: {
          ...feature.properties,
          ...getFeatureStyle(feature),
        },
      })),
    };
  }

  function walkCoords(
    geometry: GeoJSON.Geometry,
    fn: (lng: number, lat: number) => void
  ) {
    if (geometry.type === "GeometryCollection") {
      geometry.geometries.forEach((geom) => walkCoords(geom, fn));
      return;
    }
    const coords = geometry.coordinates as unknown;
    if (geometry.type === "Point") {
      const p = coords as number[];
      fn(p[0], p[1]);
    } else if (
      geometry.type === "LineString" ||
      geometry.type === "MultiPoint"
    ) {
      (coords as number[][]).forEach((p) => fn(p[0], p[1]));
    } else if (
      geometry.type === "Polygon" ||
      geometry.type === "MultiLineString"
    ) {
      (coords as number[][][]).forEach((ring) =>
        ring.forEach((p) => fn(p[0], p[1]))
      );
    } else if (geometry.type === "MultiPolygon") {
      (coords as number[][][][]).forEach((poly) =>
        poly.forEach((ring) => ring.forEach((p) => fn(p[0], p[1])))
      );
    }
  }

  function computeBounds(
    geo: GeoJSON.FeatureCollection
  ): [[number, number], [number, number]] | null {
    let minLng = Infinity;
    let minLat = Infinity;
    let maxLng = -Infinity;
    let maxLat = -Infinity;
    let hasCoords = false;
    geo.features.forEach((feature) => {
      if (!feature.geometry) return;
      walkCoords(feature.geometry, (lng, lat) => {
        hasCoords = true;
        if (lng < minLng) minLng = lng;
        if (lat < minLat) minLat = lat;
        if (lng > maxLng) maxLng = lng;
        if (lat > maxLat) maxLat = lat;
      });
    });
    return hasCoords
      ? [
          [minLng, minLat],
          [maxLng, maxLat],
        ]
      : null;
  }

  function ensureMap(container: HTMLElement, styleUrl: string) {
    if (map) return;
    map = new maplibregl.Map({
      container,
      center: [118, -2.5],
      zoom: 5,
      minZoom: 4,
      maxZoom: 12,
      style: styleUrl,
      attributionControl: false,
    });
    currentStyleUrl = styleUrl;
  }

  function closePopup() {
    if (popup) {
      popup.remove();
      popup = null;
    }
  }

  function clearHover() {
    if (!map) {
      closePopup();
      return;
    }
    if (hoveredId !== null) {
      try {
        map.setFeatureState(
          { source: SOURCE, id: hoveredId },
          { hover: false }
        );
      } catch (error) {
        console.warn("Failed to clear hover state:", error);
      }
      hoveredId = null;
    }
    closePopup();
  }

  function addLayers() {
    if (!map) return;
    if (map.getSource(SOURCE)) return;
    map.addSource(SOURCE, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
      generateId: true,
    });

    map.addLayer({
      id: FILL_LAYER,
      type: "fill",
      source: SOURCE,
      paint: {
        "fill-color": ["coalesce", ["get", "fillColor"], "#cbd5e1"],
        "fill-opacity": ["coalesce", ["get", "fillOpacity"], 0.08],
      },
    });

    map.addLayer({
      id: LINE_LAYER,
      type: "line",
      source: SOURCE,
      paint: {
        "line-color": ["coalesce", ["get", "strokeColor"], "#b5a882"],
        "line-width": ["coalesce", ["get", "strokeWidth"], 0.8],
        "line-opacity": ["coalesce", ["get", "strokeOpacity"], 0.17],
      },
    });

    map.addLayer({
      id: HOVER_FILL,
      type: "fill",
      source: SOURCE,
      paint: {
        "fill-color": ["coalesce", ["get", "fillColor"], "#cbd5e1"],
        "fill-opacity": [
          "case",
          ["boolean", ["feature-state", "hover"], false],
          [
            "min",
            ["+", ["coalesce", ["get", "fillOpacity"], 0.08], 0.16],
            0.85,
          ],
          0,
        ],
      },
    });

    map.addLayer({
      id: HOVER_LINE,
      type: "line",
      source: SOURCE,
      paint: {
        "line-color": hoverStrokeColor,
        "line-width": 1.8,
        "line-opacity": [
          "case",
          ["boolean", ["feature-state", "hover"], false],
          1,
          0,
        ],
      },
    });

    if (eventsAttached) return;
    eventsAttached = true;

    map.on("mousemove", FILL_LAYER, (event) => {
      if (!map || !event.features?.length) return;

      map.getCanvas().style.cursor = "pointer";
      const feature = event.features[0];
      const id = feature.id as number | string | undefined;

      if (id === undefined) return;

      if (hoveredId !== null && hoveredId !== id) {
        map.setFeatureState(
          { source: SOURCE, id: hoveredId },
          { hover: false }
        );
      }
      hoveredId = id;
      map.setFeatureState({ source: SOURCE, id }, { hover: true });

      if (getPopupHtml && feature.properties) {
        const areaKey = getFeatureAreaKey(feature.properties);
        const html = getPopupHtml(areaKey);
        if (html) {
          if (!popup) {
            popup = new maplibregl.Popup({
              closeButton: false,
              closeOnClick: false,
              maxWidth: "320px",
              className: "audit-popup",
              offset: 12,
            });
          }
          popup.setLngLat(event.lngLat).setHTML(html).addTo(map);
        }
      }
    });

    map.on("mouseleave", FILL_LAYER, () => {
      if (!map) return;
      map.getCanvas().style.cursor = "";
      clearHover();
    });

    map.on("click", FILL_LAYER, (event) => {
      if (!event.features?.length) return;
      const areaKey = getFeatureAreaKey(event.features[0].properties);
      onAreaClick?.(areaKey);
    });
  }

  function render(
    container: HTMLElement,
    geo: GeoJSON.FeatureCollection,
    options: RenderOptions,
    onReady?: () => void
  ) {
    isProvinceView = options.isProvinceView;
    onAreaClick = options.onAreaClick;
    getPopupHtml = options.getPopupHtml;
    if (options.hoverStrokeColor) hoverStrokeColor = options.hoverStrokeColor;

    const styleUrl =
      options.styleUrl ??
      "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

    ensureMap(container, styleUrl);

    if (map && currentStyleUrl !== styleUrl) {
      currentStyleUrl = styleUrl;
      const pendingFeatures = buildStyledGeo(geo, options.getFeatureStyle);
      map.setStyle(styleUrl);
      map.once("styledata", () => {
        if (!map) return;
        if (!map.getSource(SOURCE)) {
          addLayers();
        }
        const source = map.getSource(SOURCE) as
          | maplibregl.GeoJSONSource
          | undefined;
        source?.setData(pendingFeatures);
        map.resize();
      });
    }

    const apply = () => {
      if (!map) return;
      if (!map.getSource(SOURCE)) {
        addLayers();
      } else if (map.getLayer(HOVER_LINE)) {
        map.setPaintProperty(HOVER_LINE, "line-color", hoverStrokeColor);
      }
      clearHover();

      const styledGeo = buildStyledGeo(geo, options.getFeatureStyle);
      const source = map.getSource(SOURCE) as
        | maplibregl.GeoJSONSource
        | undefined;
      source?.setData(styledGeo);

      if (options.fitBounds) {
        const bounds = computeBounds(geo);
        if (bounds) {
          map.fitBounds(bounds, {
            padding: options.isProvinceView ? 80 : 50,
            duration: 300,
          });
        }
      }

      // Kontainer sering 0×0 saat pertama mount (flex); resize agar canvas tergambar.
      map.resize();

      onReady?.();
    };

    if (map && map.isStyleLoaded()) {
      apply();
    } else {
      map?.once("load", apply);
    }
  }

  function refresh(
    geo: GeoJSON.FeatureCollection,
    getFeatureStyle: (feature: GeoJSON.Feature) => FeatureStyle
  ) {
    if (!map || !map.getSource(SOURCE)) return;
    clearHover();
    const source = map.getSource(SOURCE) as maplibregl.GeoJSONSource;
    source.setData(buildStyledGeo(geo, getFeatureStyle));
  }

  function destroy() {
    closePopup();
    map?.remove();
    map = null;
  }

  function zoomIn() {
    map?.zoomIn({ duration: 200 });
  }

  function zoomOut() {
    map?.zoomOut({ duration: 200 });
  }

  function resize() {
    map?.resize();
  }

  function fitAll(geo: GeoJSON.FeatureCollection) {
    if (!map) return;
    const bounds = computeBounds(geo);
    if (!bounds) return;
    map.fitBounds(bounds, {
      padding: isProvinceView ? 80 : 50,
      duration: 400,
    });
  }

  return {
    render,
    refresh,
    resize,
    zoomIn,
    zoomOut,
    fitAll,
    closePopup: clearHover,
    destroy,
  };
}
