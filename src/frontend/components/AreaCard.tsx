import { dashboardStore } from '../store/dashboard.store';
import { useDashboardStore } from '../hooks/useDashboardStore';
import {
  formatCompactCurrency,
  formatNumber,
  areaBadgeLabel,
  areaBadgeClass,
  getLegendColor,
} from '../lib/format';
import type { RegionRow, ProvinceRow, AreaMetrics, Legend } from '../types/api';
import type { MapFilter } from '../types/store';

interface Props {
  area: RegionRow | ProvinceRow;
  metrics: AreaMetrics;
  rank: number;
  maxWaste: number;
  legend: Legend;
  mapFilter: MapFilter;
}

function secondaryLine(area: RegionRow | ProvinceRow, isProvince: boolean): string {
  if (isProvince) return 'Hanya paket Pemprov';
  return (area as RegionRow).provinceName;
}

export function AreaCard({ area, metrics, rank, maxWaste, legend, mapFilter }: Readonly<Props>) {
  const selectedAreaKey = useDashboardStore((s) => s.selectedAreaKey);
  const isProvince = mapFilter === 'provinsi';

  const areaKey = isProvince
    ? (area as ProvinceRow).provinceKey
    : (area as RegionRow).regionKey;

  const isSelected = selectedAreaKey === areaKey;
  const barWidth = Math.max(4, Math.round((metrics.totalPotentialWaste / Math.max(maxWaste, 1)) * 100));
  const barColor = getLegendColor(metrics.totalPotentialWaste, legend);

  const open = () =>
    dashboardStore.getState().openAreaCaseFile(areaKey, isProvince ? 'province' : 'region');

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar') {
      e.preventDefault();
      open();
    }
  };

  return (
    <div
      class={`pi${isSelected ? ' a' : ''}`}
      role="button"
      tabIndex={0}
      aria-label={`Buka berkas ${area.displayName}`}
      onClick={open}
      onKeyDown={onKeyDown}
    >
      <span class="pi-num">#{formatNumber(rank)}</span>
      <div class="pn">{area.displayName}</div>
      <span class={`tbd ${areaBadgeClass(area)}`}>{areaBadgeLabel(area)}</span>
      <div class="pi-meta">
        {secondaryLine(area, isProvince)} &middot; {formatNumber(metrics.totalPackages)} paket &middot;{' '}
        {formatNumber(metrics.totalPriorityPackages)} prioritas
      </div>
      <div class="pi-waste">
        <span class="ppv">Rp {formatCompactCurrency(metrics.totalPotentialWaste)}</span>
        <span class="ppl">pemborosan</span>
      </div>
      <div class="bw">
        <div class="bf" style={{ width: `${barWidth}%`, background: barColor }}></div>
      </div>
      <div class="pi-budget">
        <span class="pi-budget-label">Pagu Teraudit</span>
        <span class="pi-budget-val">Rp {formatCompactCurrency(metrics.totalBudget)}</span>
      </div>
    </div>
  );
}
