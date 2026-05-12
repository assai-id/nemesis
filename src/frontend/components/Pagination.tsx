import { dashboardStore } from '../store/dashboard.store';
import { formatNumber } from '../lib/format';
import type { PaginationMeta } from '../types/api';

interface PaginationProps {
  pagination: PaginationMeta;
}

export function Pagination({ pagination }: Readonly<PaginationProps>) {
  return (
    <div class="pager">
      <button
        class="pager-btn"
        disabled={pagination.page <= 1}
        onClick={() => dashboardStore.getState().setModalPage(pagination.page - 1)}
      >
        Sebelumnya
      </button>
      <div class="pager-text">
        Halaman {formatNumber(pagination.page)} / {formatNumber(pagination.totalPages)} &middot;{' '}
        {formatNumber(pagination.totalItems)} paket
      </div>
      <button
        class="pager-btn"
        disabled={pagination.page >= pagination.totalPages}
        onClick={() => dashboardStore.getState().setModalPage(pagination.page + 1)}
      >
        Berikutnya
      </button>
    </div>
  );
}
