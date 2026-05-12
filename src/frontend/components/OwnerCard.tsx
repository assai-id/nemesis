import { dashboardStore } from '../store/dashboard.store';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { formatCompactCurrency, formatNumber, getLegendColor } from '../lib/format';
import type { OwnerRow, Legend } from '../types/api';

interface Props {
  owner: OwnerRow;
  rank: number;
  maxWaste: number;
  legend: Legend;
}

export function OwnerCard({ owner, rank, maxWaste, legend }: Readonly<Props>) {
  const selectedOwnerKey = useDashboardStore((s) => s.selectedOwnerKey);
  const ownerKey = `${owner.ownerType}::${owner.ownerName}`;
  const isSelected = selectedOwnerKey === ownerKey;
  const barWidth = Math.max(4, Math.round((owner.totalPotentialWaste / Math.max(maxWaste, 1)) * 100));
  const barColor = getLegendColor(owner.totalPotentialWaste, legend);

  return (
    <button
      type="button"
      class={`pi${isSelected ? ' a' : ''}`}
      onClick={() => dashboardStore.getState().openOwnerModal(owner.ownerName, owner.ownerType)}
    >
      <div class="pit">
        <div class="pn">
          <span style={{ color: 'var(--t3)', fontSize: '9px', marginRight: '5px' }}>#{rank}</span>
          {owner.ownerName}
        </div>
        <div class="tbd bc">K/L</div>
      </div>
      <div style={{ fontSize: '9.5px', color: 'var(--t3)', marginBottom: '4px' }}>Kementerian/Lembaga</div>
      <div>
        <span class="ppv">Rp {formatCompactCurrency(owner.totalPotentialWaste)}</span>
        <span class="ppl"> &middot; {formatNumber(owner.totalPriorityPackages)} prioritas</span>
      </div>
      <div class="bw">
        <div class="bf" style={{ width: `${barWidth}%`, background: barColor }}></div>
      </div>
      <div class="ps">
        <div class="pst">Total Paket: <strong>{formatNumber(owner.totalPackages)}</strong></div>
        <div class="pst">Severity High: <strong>{formatNumber(owner.severityCounts.high)}</strong></div>
      </div>
      <div class="owner-mix">Severity Absurd {formatNumber(owner.severityCounts.absurd)}</div>
      <div class="waste-row">
        <span class="waste-label">Pagu Teraudit</span>
        <span class="waste-val">Rp {formatCompactCurrency(owner.totalBudget)}</span>
      </div>
    </button>
  );
}
