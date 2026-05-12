import { dashboardStore } from '../store/dashboard.store';
import { useDashboardStore } from '../hooks/useDashboardStore';
import {
  formatCompactCurrency,
  formatNumber,
  areaBadgeLabel,
  areaBadgeClass,
  ownerTypeLabel,
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

export function AreaCard({ area, metrics, rank, maxWaste, legend, mapFilter }: Readonly<Props>) {
  const selectedAreaKey = useDashboardStore((s) => s.selectedAreaKey);
  const isProvince = mapFilter === 'provinsi';

  const areaKey = isProvince
    ? (area as ProvinceRow).provinceKey
    : (area as RegionRow).regionKey;

  const isSelected = selectedAreaKey === areaKey;
  const barWidth = Math.max(4, Math.round((metrics.totalPotentialWaste / Math.max(maxWaste, 1)) * 100));
  const barColor = getLegendColor(metrics.totalPotentialWaste, legend);
  const ownerKey = isProvince ? 'provinsi' : mapFilter;
  const ownerSummary = `${ownerTypeLabel(ownerKey)} saja`;
  const secondaryLine = isProvince ? 'Hanya paket Pemprov' : (area as RegionRow).provinceName;

  return (
    <button
      type="button"
      class={`pi${isSelected ? ' a' : ''}`}
      onClick={() => dashboardStore.getState().openAreaModal(areaKey, isProvince ? 'province' : 'region')}
    >
      <div class="pit">
        <div class="pn">
          <span style={{ color: 'var(--t3)', fontSize: '9px', marginRight: '5px' }}>#{rank}</span>
          {area.displayName}
        </div>
        <div class={`tbd ${areaBadgeClass(area)}`}>{areaBadgeLabel(area)}</div>
      </div>
      <div style={{ fontSize: '9.5px', color: 'var(--t3)', marginBottom: '4px' }}>{secondaryLine}</div>
      <div>
        <span class="ppv">Rp {formatCompactCurrency(metrics.totalPotentialWaste)}</span>
        <span class="ppl"> &middot; {formatNumber(metrics.totalPriorityPackages)} prioritas</span>
      </div>
      <div class="bw">
        <div class="bf" style={{ width: `${barWidth}%`, background: barColor }}></div>
      </div>
      <div class="ps">
        <div class="pst">Total Paket: <strong>{formatNumber(metrics.totalPackages)}</strong></div>
        <div class="pst">Pemilik: <strong>{ownerTypeLabel(ownerKey)}</strong></div>
      </div>
      <div class="owner-mix">{ownerSummary}</div>
      <div class="waste-row">
        <span class="waste-label">Pagu Teraudit</span>
        <span class="waste-val">Rp {formatCompactCurrency(metrics.totalBudget)}</span>
      </div>
    </button>
  );
}
