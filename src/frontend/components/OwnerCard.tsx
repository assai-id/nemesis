import { dashboardStore } from '../store/dashboard.store';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { formatCompactCurrency, formatNumber, getLegendColor, ownerTypeLabel } from '../lib/format';
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
  const ownerTypeShort = owner.ownerType === 'central' ? 'K/L' : ownerTypeLabel(owner.ownerType);

  const open = () => dashboardStore.getState().openOwnerCaseFile(owner.ownerName, owner.ownerType);

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
      aria-label={`Buka berkas ${owner.ownerName}`}
      onClick={open}
      onKeyDown={onKeyDown}
    >
      <span class="pi-num">#{formatNumber(rank)}</span>
      <div class="pn">{owner.ownerName}</div>
      <span class="tbd bc">{ownerTypeShort}</span>
      <div class="pi-meta">
        {formatNumber(owner.totalPackages)} paket &middot; {formatNumber(owner.totalPriorityPackages)} prioritas
      </div>
      <div class="pi-waste">
        <span class="ppv">Rp {formatCompactCurrency(owner.totalPotentialWaste)}</span>
        <span class="ppl">pemborosan</span>
      </div>
      <div class="bw">
        <div class="bf" style={{ width: `${barWidth}%`, background: barColor }}></div>
      </div>
      <div class="pi-budget">
        <span class="pi-budget-label">Pagu Teraudit</span>
        <span class="pi-budget-val">Rp {formatCompactCurrency(owner.totalBudget)}</span>
      </div>
    </div>
  );
}
