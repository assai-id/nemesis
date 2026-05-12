import { dashboardStore } from '../store/dashboard.store';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { formatCompactCurrency, formatNumber, ownerTypeLabel, areaBadgeClass } from '../lib/format';
import type { RegionRow, ProvinceRow, OwnerRow } from '../types/api';

interface Props {
  region?: RegionRow;
  province?: ProvinceRow;
  owner?: OwnerRow;
}

function close() {
  dashboardStore.getState().closeModal();
}

export function ModalHeader({ region, province, owner }: Readonly<Props>) {
  const areaType = useDashboardStore((s) => s.modal.areaType);

  if (areaType === 'owner' && owner) {
    return (
      <div class="modal-top" id="modalTop">
        <div class="modal-top-row">
          <div>
            <h2>{owner.ownerName}</h2>
            <div class="msub">{ownerTypeLabel(owner.ownerType)} | Audit paket nasional TA 2026</div>
          </div>
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            <span class="tbd bc">K/L</span>
            <button class="modal-close" onClick={close}>&#10005; Tutup</button>
          </div>
        </div>
        <div class="modal-kpis">
          <div class="mkp">
            <div class="mkp-l">Potensi Pemborosan</div>
            <div class="mkp-v" style={{ color: 'var(--brick)' }}>Rp {formatCompactCurrency(owner.totalPotentialWaste)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Paket Prioritas</div>
            <div class="mkp-v">{formatNumber(owner.totalPriorityPackages)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Total Paket</div>
            <div class="mkp-v">{formatNumber(owner.totalPackages)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Total Pagu</div>
            <div class="mkp-v" style={{ color: 'var(--sage)' }}>Rp {formatCompactCurrency(owner.totalBudget)}</div>
          </div>
        </div>
      </div>
    );
  }

  if (areaType === 'province' && province) {
    return (
      <div class="modal-top" id="modalTop">
        <div class="modal-top-row">
          <div>
            <h2>{province.displayName}</h2>
            <div class="msub">Paket pemprov pada provinsi ini &middot; TA 2026</div>
          </div>
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            <span class={`tbd ${areaBadgeClass(province)}`}>Provinsi</span>
            <button class="modal-close" onClick={close}>&#10005; Tutup</button>
          </div>
        </div>
        <div class="modal-kpis">
          <div class="mkp">
            <div class="mkp-l">Potensi Pemborosan</div>
            <div class="mkp-v" style={{ color: 'var(--brick)' }}>Rp {formatCompactCurrency(province.totalPotentialWaste)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Paket Prioritas</div>
            <div class="mkp-v">{formatNumber(province.totalPriorityPackages)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Total Paket Pemprov</div>
            <div class="mkp-v">{formatNumber(province.totalPackages)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Total Pagu</div>
            <div class="mkp-v" style={{ color: 'var(--sage)' }}>Rp {formatCompactCurrency(province.totalBudget)}</div>
          </div>
        </div>
      </div>
    );
  }

  if (region) {
    return (
      <div class="modal-top" id="modalTop">
        <div class="modal-top-row">
          <div>
            <h2>{region.displayName}</h2>
            <div class="msub">{region.provinceName} | Audit paket pengadaan TA 2026</div>
          </div>
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            <span class={`tbd ${areaBadgeClass(region)}`}>{region.regionType}</span>
            <button class="modal-close" onClick={close}>&#10005; Tutup</button>
          </div>
        </div>
        <div class="modal-kpis">
          <div class="mkp">
            <div class="mkp-l">Potensi Pemborosan</div>
            <div class="mkp-v" style={{ color: 'var(--brick)' }}>Rp {formatCompactCurrency(region.totalPotentialWaste)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Paket Prioritas</div>
            <div class="mkp-v">{formatNumber(region.totalPriorityPackages)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Total Paket</div>
            <div class="mkp-v">{formatNumber(region.totalPackages)}</div>
          </div>
          <div class="mkp">
            <div class="mkp-l">Total Pagu</div>
            <div class="mkp-v" style={{ color: 'var(--sage)' }}>Rp {formatCompactCurrency(region.totalBudget)}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div class="modal-top" id="modalTop">
      <div class="modal-top-row">
        <div>
          <h2>Memuat area...</h2>
          <div class="msub">Audit paket pengadaan &middot; TA 2026</div>
        </div>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <button class="modal-close" onClick={close}>&#10005; Tutup</button>
        </div>
      </div>
    </div>
  );
}
