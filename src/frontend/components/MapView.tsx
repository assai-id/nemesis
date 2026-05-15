import { useRef, useEffect } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { getLegendColor, formatCompactCurrency, formatNumber, escapeHtml } from '../lib/format';
import { getThemeColors } from '../lib/theme';
import type { Feature, FeatureCollection } from 'geojson';
import type { FeatureStyle } from '../types/audit-map';
import type { RegionRow, ProvinceRow } from '../types/api';

// side-effect import so globalThis.AuditMap is populated
import '../assets/js/map.js';

function computeVisibility(
  area: RegionRow | ProvinceRow,
  isProvince: boolean,
  tab: string,
  mapFilter: string,
): boolean {
  if (isProvince) return area.totalPackages > 0;
  const r = area as RegionRow;
  const tabOk =
    (tab === 'kabupaten' && r.regionType === 'Kabupaten') ||
    (tab === 'kota' && r.regionType === 'Kota') ||
    tab === 'all';
  const filterOk = (['central', 'provinsi', 'kabkota', 'other'] as const).includes(
    mapFilter as 'central' | 'provinsi' | 'kabkota' | 'other',
  )
    ? (r.ownerMix?.[mapFilter as keyof typeof r.ownerMix] ?? 0) > 0
    : true;
  return tabOk && filterOk;
}

function computeFeatureStyle(feature: Feature): FeatureStyle {
  const { mapFilter, selectedAreaKey, regionsByKey, provincesByKey, data, theme } =
    dashboardStore.getState();
  const isProvince = mapFilter === 'provinsi';
  const props = feature.properties as Record<string, unknown>;
  const areaKey = String(isProvince ? props.provinceKey : props.regionKey);
  const area = isProvince ? provincesByKey.get(areaKey) : regionsByKey.get(areaKey);

  let legend = null;
  if (data) {
    legend = isProvince ? data.provinceView.legend : data.legend;
  }

  const tab = dashboardStore.getState().tab;
  const visible = area ? computeVisibility(area, isProvince, tab, mapFilter) : false;

  const themeColors = getThemeColors(theme);
  const selected = selectedAreaKey === areaKey;
  const strokeOpacity = (selected ? 1 : 0.2) * (visible ? 0.85 : 0.2);
  const fillColor =
    area && legend ? getLegendColor(area.totalPotentialWaste, legend) : themeColors.zeroColor;

  let fillOpacity: number;
  if (selected) {
    fillOpacity = 0.72;
  } else {
    fillOpacity = visible ? 0.52 : 0.08;
  }

  return {
    fillColor,
    fillOpacity,
    strokeColor: selected ? themeColors.strokeSelected : themeColors.strokeDefault,
    strokeWidth: selected ? 2.1 : 0.8,
    strokeOpacity,
  };
}

function computePopupHtml(areaKey: string): string | null {
  const { mapFilter, regionsByKey, provincesByKey, data } = dashboardStore.getState();
  const isProvince = mapFilter === 'provinsi';
  const area = isProvince ? provincesByKey.get(areaKey) : regionsByKey.get(areaKey);

  let legend = null;
  if (data) {
    legend = isProvince ? data.provinceView.legend : data.legend;
  }

  if (!area || !legend) return `<div class="pt">Belum ada data</div>`;

  const progressPct = Math.min(
    100,
    area.totalPriorityPackages > 0
      ? Math.round((area.totalPriorityPackages / Math.max(area.totalPackages, 1)) * 100)
      : 0
  );
  const barColor = getLegendColor(area.totalPotentialWaste, legend);

  if (isProvince) {
    return (
      `<div class="pt">${escapeHtml(area.displayName)}</div>` +
      `<div class="popup-sub">Paket Pemprov</div>` +
      `<div class="pr"><span class="l">Potensi Pemborosan</span><span class="v" style="color:#b5a882">Rp ${formatCompactCurrency(area.totalPotentialWaste)}</span></div>` +
      `<div class="pr"><span class="l">Paket Prioritas</span><span class="v">${formatNumber(area.totalPriorityPackages)}</span></div>` +
      `<div class="pr"><span class="l">Total Paket</span><span class="v">${formatNumber(area.totalPackages)}</span></div>` +
      `<div class="pr"><span class="l">Total Pagu</span><span class="v">${formatCompactCurrency(area.totalBudget)}</span></div>` +
      `<div class="pr"><span class="l">Severity High</span><span class="v">${formatNumber(area.severityCounts.high)}</span></div>` +
      `<div class="ppb"><div class="ppbf" style="width:${progressPct}%;background:${barColor}"></div></div>`
    );
  }

  const r = area as RegionRow;
  return (
    `<div class="pt">${escapeHtml(area.displayName)}</div>` +
    `<div class="popup-sub">${escapeHtml(r.provinceName)}</div>` +
    `<div class="pr"><span class="l">Potensi Pemborosan</span><span class="v" style="color:#b5a882">Rp ${formatCompactCurrency(area.totalPotentialWaste)}</span></div>` +
    `<div class="pr"><span class="l">Paket Prioritas</span><span class="v">${formatNumber(area.totalPriorityPackages)}</span></div>` +
    `<div class="pr"><span class="l">Total Paket</span><span class="v">${formatNumber(area.totalPackages)}</span></div>` +
    `<div class="pr"><span class="l">Kementerian/Lembaga</span><span class="v">${formatNumber(r.ownerMix?.central ?? 0)}</span></div>` +
    `<div class="pr"><span class="l">Pemprov</span><span class="v">${formatNumber(r.ownerMix?.provinsi ?? 0)}</span></div>` +
    `<div class="pr"><span class="l">Pemkot</span><span class="v">${formatNumber(r.ownerMix?.kabkota ?? 0)}</span></div>` +
    `<div class="pr"><span class="l">Others</span><span class="v">${formatNumber(r.ownerMix?.other ?? 0)}</span></div>` +
    `<div class="ppb"><div class="ppbf" style="width:${progressPct}%;background:${barColor}"></div></div>`
  );
}

export function MapView() {
  const containerRef = useRef<HTMLDivElement>(null);
  const lastGeoRef = useRef<FeatureCollection | null>(null);
  const data = useDashboardStore((s) => s.data);
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const selectedAreaKey = useDashboardStore((s) => s.selectedAreaKey);
  const tab = useDashboardStore((s) => s.tab);
  const theme = useDashboardStore((s) => s.theme);
  const isProvince = mapFilter === 'provinsi';

  // Full render hanya saat geo reference benar-benar baru (data fetch atau
  // province/region switch). Toggle theme menghasilkan `data` reference baru
  // tapi `data.geo` tetap stabil — tanpa guard ini, fitBounds beranimasi
  // ulang dan setData 514 features dipanggil tanpa perlu.
  useEffect(() => {
    if (!containerRef.current || !data || !globalThis.AuditMap) return;
    const geo = isProvince ? data.provinceView.geo : data.geo;
    if (!geo.features.length) return;
    if (lastGeoRef.current === geo) return;
    lastGeoRef.current = geo;

    globalThis.AuditMap.render(
      containerRef.current,
      geo,
      {
        getFeatureStyle: computeFeatureStyle,
        getPopupHtml: computePopupHtml,
        onAreaClick: (areaKey) =>
          dashboardStore.getState().openAreaCaseFile(areaKey, isProvince ? 'province' : 'region'),
        fitBounds: true,
        isProvinceView: isProvince,
      },
      undefined
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, isProvince]);

  // Refresh styles when selection, filter, tab, or theme changes (without re-fitting bounds)
  useEffect(() => {
    if (!data || !globalThis.AuditMap) return;
    const geo = isProvince ? data.provinceView.geo : data.geo;
    globalThis.AuditMap.refresh(geo, computeFeatureStyle);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAreaKey, mapFilter, tab, theme]);

  return <div id="map" ref={containerRef} role="application" aria-label="Peta choropleth audit pengadaan" />;
}
