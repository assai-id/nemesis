import { useRef, useEffect } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { getLegendColor, formatCompactCurrency, formatNumber, escapeHtml } from '../lib/format';
import type { Feature } from 'geojson';
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
  const { mapFilter, selectedAreaKey, regionsByKey, provincesByKey, data } =
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

  const selected = selectedAreaKey === areaKey;
  const strokeOpacity = (selected ? 1 : 0.2) * (visible ? 0.85 : 0.2);
  const fillColor = area && legend ? getLegendColor(area.totalPotentialWaste, legend) : '#243155';

  let fillOpacity: number;
  if (selected) {
    fillOpacity = 0.72;
  } else {
    fillOpacity = visible ? 0.52 : 0.08;
  }

  return {
    fillColor,
    fillOpacity,
    strokeColor: selected ? '#f0d8a8' : '#b5a882',
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
  const data = useDashboardStore((s) => s.data);
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const selectedAreaKey = useDashboardStore((s) => s.selectedAreaKey);
  const isProvince = mapFilter === 'provinsi';

  // Full render when data arrives or province/region view switches
  useEffect(() => {
    if (!containerRef.current || !data || !globalThis.AuditMap) return;
    const geo = isProvince ? data.provinceView.geo : data.geo;
    if (!geo.features.length) return;

    globalThis.AuditMap.render(
      containerRef.current,
      geo,
      {
        getFeatureStyle: computeFeatureStyle,
        getPopupHtml: computePopupHtml,
        onAreaClick: (areaKey) =>
          dashboardStore.getState().openAreaModal(areaKey, isProvince ? 'province' : 'region'),
        fitBounds: true,
        isProvinceView: isProvince,
      },
      undefined
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, isProvince]);

  // Refresh styles when selection or filter changes (without re-fitting bounds)
  useEffect(() => {
    if (!data || !globalThis.AuditMap) return;
    const geo = isProvince ? data.provinceView.geo : data.geo;
    globalThis.AuditMap.refresh(geo, computeFeatureStyle);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAreaKey, mapFilter]);

  return <div id="map" ref={containerRef} />;
}
