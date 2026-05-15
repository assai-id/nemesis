import { dashboardStore } from '../store/dashboard.store';
import { formatNumber } from '../lib/format';
import type { PaginationMeta } from '../types/api';

interface PaginationProps {
  pagination: PaginationMeta;
}

const PAGE_SIZES = [25, 50, 100, 250];

export function Pagination({ pagination }: Readonly<PaginationProps>) {
  const pageSize = pagination.pageSize || 25;

  function handleJump(e: Event) {
    const raw = parseInt((e.target as HTMLInputElement).value, 10);
    if (!Number.isFinite(raw)) return;
    const clamped = Math.min(Math.max(1, raw), Math.max(1, pagination.totalPages));
    dashboardStore.getState().setModalPage(clamped);
  }

  return (
    <div class="pager">
      <button
        type="button"
        class="pager-btn"
        disabled={pagination.page <= 1}
        onClick={() => dashboardStore.getState().setModalPage(pagination.page - 1)}
      >
        ← Sebelumnya
      </button>
      <div class="pager-info">
        <label class="pager-jump">
          <span>Halaman</span>
          <input
            type="number"
            min={1}
            max={pagination.totalPages}
            value={pagination.page}
            aria-label="Lompat ke halaman"
            onChange={handleJump}
          />
          <span>/ {formatNumber(pagination.totalPages)}</span>
        </label>
        <span class="pager-count">{formatNumber(pagination.totalItems)} paket</span>
        <label class="pager-size">
          <span>Per halaman</span>
          <select
            aria-label="Jumlah baris per halaman"
            value={pageSize}
            onChange={(e) =>
              dashboardStore
                .getState()
                .setModalPageSize(parseInt((e.target as HTMLSelectElement).value, 10))
            }
          >
            {PAGE_SIZES.map((n) => (
              <option key={n} value={n}>{n}</option>
            ))}
          </select>
        </label>
      </div>
      <button
        type="button"
        class="pager-btn"
        disabled={pagination.page >= pagination.totalPages}
        onClick={() => dashboardStore.getState().setModalPage(pagination.page + 1)}
      >
        Berikutnya →
      </button>
    </div>
  );
}
