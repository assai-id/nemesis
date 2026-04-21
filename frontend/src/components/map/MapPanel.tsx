"use client";

import { useCallback, useEffect, useMemo, useRef } from "react";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import Stack from "@mui/material/Stack";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import CircularProgress from "@mui/material/CircularProgress";
import AddIcon from "@mui/icons-material/Add";
import RemoveIcon from "@mui/icons-material/Remove";
import MyLocationIcon from "@mui/icons-material/MyLocation";
import { pantau, getMapChromeColors } from "@/theme/theme";
import { useColorMode } from "@/theme/ColorModeContext";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import MapFilters from "./MapFilters";
import LegendPanel from "./LegendPanel";
import {
  createAuditMap,
  type AuditMapController,
  type FeatureStyle,
} from "./auditMap";
import {
  currentAreaType,
  getActiveAreas,
  getActiveGeo,
  getActiveLegend,
  getLegendColor,
  isProvinceView,
  areaMatchesCurrentView,
  ownerTypeCount,
} from "@/lib/dashboard-logic";
import type { ProvinceArea, RegionArea, AreaBase } from "@/types/dashboard";
import { formatCompactCurrency, formatNumber } from "@/lib/format";

const LIGHT_STYLE =
  "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";
const DARK_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function popupHtml(
  area: AreaBase | null | undefined,
  provinceView: boolean,
  legendColor: string,
  moneyColor: string
): string {
  if (!area) return `<div class="pt">Belum ada data</div>`;

  const progressPct = Math.min(
    100,
    area.totalPriorityPackages > 0
      ? Math.round(
          (area.totalPriorityPackages / Math.max(area.totalPackages, 1)) * 100
        )
      : 0
  );

  if (provinceView) {
    const p = area as ProvinceArea;
    return (
      `<div class="pt">${escapeHtml(p.displayName)}</div>` +
      `<div class="popup-sub">Paket Pemprov</div>` +
      `<div class="pr"><span class="l">Potensi pemborosan</span><span class="v" style="color:${moneyColor}">Rp ${escapeHtml(formatCompactCurrency(p.totalPotentialWaste))}</span></div>` +
      `<div class="pr"><span class="l">Paket prioritas</span><span class="v">${escapeHtml(formatNumber(p.totalPriorityPackages))}</span></div>` +
      `<div class="pr"><span class="l">Total paket</span><span class="v">${escapeHtml(formatNumber(p.totalPackages))}</span></div>` +
      `<div class="pr"><span class="l">Total pagu</span><span class="v">Rp ${escapeHtml(formatCompactCurrency(p.totalBudget))}</span></div>` +
      `<div class="pr"><span class="l">Severity high</span><span class="v">${escapeHtml(formatNumber(p.severityCounts.high))}</span></div>` +
      `<div class="ppb"><div class="ppbf" style="width:${progressPct}%;background:${legendColor}"></div></div>`
    );
  }

  return (
    `<div class="pt">${escapeHtml(area.displayName)}</div>` +
    `<div class="popup-sub">${escapeHtml(area.provinceName)}</div>` +
    `<div class="pr"><span class="l">Potensi pemborosan</span><span class="v" style="color:${moneyColor}">Rp ${escapeHtml(formatCompactCurrency(area.totalPotentialWaste))}</span></div>` +
    `<div class="pr"><span class="l">Paket prioritas</span><span class="v">${escapeHtml(formatNumber(area.totalPriorityPackages))}</span></div>` +
    `<div class="pr"><span class="l">Total paket</span><span class="v">${escapeHtml(formatNumber(area.totalPackages))}</span></div>` +
    `<div class="pr"><span class="l">Kementerian/Lembaga</span><span class="v">${escapeHtml(formatNumber(ownerTypeCount(area, "central")))}</span></div>` +
    `<div class="pr"><span class="l">Pemprov</span><span class="v">${escapeHtml(formatNumber(ownerTypeCount(area, "provinsi")))}</span></div>` +
    `<div class="pr"><span class="l">Pemkot</span><span class="v">${escapeHtml(formatNumber(ownerTypeCount(area, "kabkota")))}</span></div>` +
    `<div class="pr"><span class="l">Others</span><span class="v">${escapeHtml(formatNumber(ownerTypeCount(area, "other")))}</span></div>` +
    `<div class="ppb"><div class="ppbf" style="width:${progressPct}%;background:${legendColor}"></div></div>`
  );
}

export default function MapPanel() {
  const { mode } = useColorMode();
  const mapChrome = useMemo(() => getMapChromeColors(mode), [mode]);
  const styleUrl = mode === "light" ? LIGHT_STYLE : DARK_STYLE;

  const {
    data,
    loading,
    error,
    mapFilter,
    tab,
    selectedAreaKey,
    openAreaDetail,
  } = useDashboard();

  const containerRef = useRef<HTMLDivElement | null>(null);
  const controllerRef = useRef<AuditMapController | null>(null);
  const handlersRef = useRef({ openAreaDetail });

  useEffect(() => {
    handlersRef.current.openAreaDetail = openAreaDetail;
  }, [openAreaDetail]);

  const areasByKey = useMemo(() => {
    if (!data) return new Map<string, RegionArea | ProvinceArea>();
    const map = new Map<string, RegionArea | ProvinceArea>();
    data.regions.forEach((region) => map.set(region.regionKey, region));
    data.provinceView.provinces.forEach((province) =>
      map.set(province.provinceKey, province)
    );
    return map;
  }, [data]);

  const provinceView = isProvinceView(mapFilter);
  const areaType = currentAreaType(mapFilter);

  const activeAreasByKey = useMemo(() => {
    const map = new Map<string, RegionArea | ProvinceArea>();
    if (!data) return map;
    const areas = getActiveAreas(data, mapFilter);
    areas.forEach((area) => {
      const key =
        areaType === "province"
          ? (area as ProvinceArea).provinceKey
          : (area as RegionArea).regionKey;
      map.set(key, area);
    });
    return map;
  }, [data, mapFilter, areaType]);

  const featureStyle = useCallback(
    (feature: GeoJSON.Feature): FeatureStyle => {
      if (!data) {
        return {
          fillColor: mapChrome.areaNeutral,
          fillOpacity: 0.35,
          strokeColor: mapChrome.areaBorder,
          strokeWidth: 0.6,
          strokeOpacity: 0.45,
        };
      }
      const legend = getActiveLegend(data, mapFilter);
      const props = feature.properties as Record<string, unknown> | null;
      const key = provinceView
        ? String(props?.provinceKey ?? "")
        : String(props?.regionKey ?? "");
      const area = activeAreasByKey.get(key);
      const visible = areaMatchesCurrentView(area, mapFilter, tab);
      const selected = selectedAreaKey === key;

      return {
        fillColor: area
          ? getLegendColor(legend, area.totalPotentialWaste)
          : mapChrome.areaNeutral,
        fillOpacity: selected ? 0.85 : visible ? 0.7 : 0.35,
        strokeColor: selected ? mapChrome.selectedStroke : mapChrome.areaBorder,
        strokeWidth: selected ? 2.4 : 0.6,
        strokeOpacity: selected ? 1 : 0.55,
      };
    },
    [data, mapFilter, tab, selectedAreaKey, provinceView, activeAreasByKey, mapChrome]
  );

  useEffect(() => {
    if (!data) return;
    if (!containerRef.current) return;
    if (!controllerRef.current) {
      controllerRef.current = createAuditMap();
    }

    const geo = getActiveGeo(data, mapFilter);
    const legend = getActiveLegend(data, mapFilter);

    controllerRef.current.render(containerRef.current, geo, {
      isProvinceView: provinceView,
      fitBounds: true,
      styleUrl,
      hoverStrokeColor: mapChrome.hoverStroke,
      onAreaClick: (areaKey) => handlersRef.current.openAreaDetail(areaKey),
      getFeatureStyle: featureStyle,
      getPopupHtml: (areaKey) => {
        const area = areasByKey.get(areaKey);
        if (!area) return null;
        return popupHtml(
          area,
          provinceView,
          getLegendColor(legend, area.totalPotentialWaste),
          mapChrome.popupMoney
        );
      },
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, mapFilter, provinceView, mapChrome, styleUrl]);

  useEffect(() => {
    if (!data || !controllerRef.current) return;
    const geo = getActiveGeo(data, mapFilter);
    controllerRef.current.refresh(geo, featureStyle);
  }, [data, mapFilter, featureStyle]);

  useEffect(() => {
    return () => {
      controllerRef.current?.destroy();
      controllerRef.current = null;
    };
  }, []);

  // MapLibre mengukur canvas dari ukuran kontainer; setelah layout flex/tab, kontainer
  // sering baru dapat tinggi — resize + ResizeObserver mencegah canvas 0×0.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const ro = new ResizeObserver(() => {
      controllerRef.current?.resize();
    });
    ro.observe(el);

    const raf = requestAnimationFrame(() => {
      controllerRef.current?.resize();
    });

    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
  }, []);

  const doZoom = useCallback(
    (direction: "in" | "out" | "reset") => {
      const controller = controllerRef.current;
      if (!controller) return;
      if (direction === "in") controller.zoomIn();
      else if (direction === "out") controller.zoomOut();
      else if (data) controller.fitAll(getActiveGeo(data, mapFilter));
    },
    [data, mapFilter]
  );

  return (
    <Box
      sx={{
        flex: 1,
        minHeight: 0,
        width: "100%",
        position: "relative",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <Box
        ref={containerRef}
        sx={{
          flex: 1,
          minHeight: 0,
          width: "100%",
          bgcolor: pantau.bgMuted,
        }}
      />

      <MapFilters />
      <LegendPanel />

      {/* Zoom controls */}
      <Stack
        direction="column"
        spacing={0.5}
        sx={{
          position: "absolute",
          top: 12,
          right: 12,
          zIndex: 4,
          bgcolor: pantau.glass,
          border: `1px solid ${pantau.border}`,
          borderRadius: 2,
          boxShadow: pantau.shadowSm,
          p: 0.5,
          backdropFilter: "blur(8px)",
        }}
      >
        <Tooltip title="Perbesar" placement="left" arrow>
          <IconButton
            size="small"
            onClick={() => doZoom("in")}
            sx={{ color: pantau.text }}
            aria-label="Perbesar peta"
          >
            <AddIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Perkecil" placement="left" arrow>
          <IconButton
            size="small"
            onClick={() => doZoom("out")}
            sx={{ color: pantau.text }}
            aria-label="Perkecil peta"
          >
            <RemoveIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Tampilkan seluruh Indonesia" placement="left" arrow>
          <IconButton
            size="small"
            onClick={() => doZoom("reset")}
            sx={{ color: pantau.text }}
            aria-label="Reset tampilan peta"
          >
            <MyLocationIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Stack>

      {(loading || error) && (
        <Box
          sx={{
            position: "absolute",
            inset: "50% auto auto 50%",
            transform: "translate(-50%, -50%)",
            minWidth: 220,
            px: 2,
            py: 1.5,
            bgcolor: pantau.surface,
            border: `1px solid ${error ? pantau.danger : pantau.border}`,
            borderRadius: 2,
            color: error ? pantau.danger : pantau.textMuted,
            boxShadow: pantau.shadowMd,
            display: "flex",
            alignItems: "center",
            gap: 1,
          }}
        >
          {loading ? (
            <>
              <CircularProgress
                size={14}
                sx={{ color: pantau.primary }}
              />
              <Typography sx={{ fontSize: 12, color: pantau.textMuted }}>
                Memuat peta…
              </Typography>
            </>
          ) : (
            <Typography sx={{ fontSize: 12 }}>
              Gagal memuat peta: {error}
            </Typography>
          )}
        </Box>
      )}
    </Box>
  );
}
