import { useState, useEffect, useRef } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { fetchRegionPackages, fetchProvincePackages, fetchOwnerPackages } from '../lib/api';
import { formatNumber } from '../lib/format';
import { PackageTable } from './PackageTable';
import { Pagination } from './Pagination';
import { ModalHeader } from './ModalHeader';
import type { PackagesResponse } from '../types/api';

interface FetchState {
  status: 'loading' | 'ready' | 'error';
  data: PackagesResponse | null;
  error: string | null;
}

const SEVERITY_OPTIONS = [
  { key: '', label: 'Semua Severity' },
  { key: 'low', label: 'Low' },
  { key: 'med', label: 'Medium' },
  { key: 'high', label: 'High' },
  { key: 'absurd', label: 'Absurd' },
];

export function ModalBody() {
  const modal = useDashboardStore((s) => s.modal);
  const [fetchState, setFetchState] = useState<FetchState>({
    status: 'loading',
    data: null,
    error: null,
  });
  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingSearchRef = useRef<string | null>(null);

  // Fetch packages whenever requestId changes (triggered by any modal state mutation)
  useEffect(() => {
    if (!modal.isOpen) return;
    if (modal.areaType === 'owner' && (!modal.ownerType || !modal.ownerName)) return;
    if (modal.areaType !== 'owner' && !modal.areaKey) return;

    let cancelled = false;
    setFetchState({ status: 'loading', data: null, error: null });

    const params = {
      page: modal.page,
      pageSize: modal.pageSize,
      search: modal.search || undefined,
      severity: modal.severity || undefined,
      priorityOnly: modal.priorityOnly || undefined,
    };

    let promise;
    if (modal.areaType === 'owner') {
      promise = fetchOwnerPackages(modal.ownerName, modal.ownerType, params);
    } else if (modal.areaType === 'province') {
      promise = fetchProvincePackages(modal.areaKey!, params);
    } else {
      promise = fetchRegionPackages(modal.areaKey!, { ...params, ownerType: modal.ownerType || undefined });
    }

    promise
      .then((data) => {
        if (!cancelled) setFetchState({ status: 'ready', data, error: null });
      })
      .catch((err: unknown) => {
        if (!cancelled)
          setFetchState({
            status: 'error',
            data: null,
            error: err instanceof Error ? err.message : String(err),
          });
      });

    return () => {
      cancelled = true;
    };
  }, [modal.requestId]); // eslint-disable-line react-hooks/exhaustive-deps

  function handleSearchInput(value: string) {
    pendingSearchRef.current = value;
    if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    searchTimeoutRef.current = setTimeout(() => {
      dashboardStore.getState().setModalSearch(pendingSearchRef.current ?? '');
    }, 800);
  }

  if (!modal.isOpen) return null;

  if (fetchState.status === 'loading') {
    return (
      <>
        <ModalHeader />
        <div class="modal-body" id="modalBody">
          <div class="modal-state">
            {modal.areaType === 'owner' ? 'Mengambil paket dari pemilik terpilih...' : 'Mengambil paket dari backend audit...'}
          </div>
        </div>
      </>
    );
  }

  if (fetchState.status === 'error') {
    return (
      <>
        <ModalHeader />
        <div class="modal-body" id="modalBody">
          <div class="modal-state error">Gagal memuat paket: {fetchState.error}</div>
        </div>
      </>
    );
  }

  const payload = fetchState.data!;

  let areaTypeLabel: string;
  if (modal.areaType === 'province') {
    areaTypeLabel = ' pemprov pada provinsi ini';
  } else {
    areaTypeLabel = modal.areaType === 'owner' ? ' pada pemilik ini' : ' pada area ini';
  }

  return (
    <>
      <ModalHeader
        region={payload.region}
        province={payload.province}
        owner={payload.owner}
      />
      <div class="modal-body" id="modalBody">
        {payload.region && (
          <div class="modal-summary-grid">
            <div class="mini-stat"><span>Kementerian/Lembaga</span><strong>{formatNumber(payload.region.ownerMix?.central ?? 0)}</strong></div>
            <div class="mini-stat"><span>Pemprov</span><strong>{formatNumber(payload.region.ownerMix?.provinsi ?? 0)}</strong></div>
            <div class="mini-stat"><span>Pemkot</span><strong>{formatNumber(payload.region.ownerMix?.kabkota ?? 0)}</strong></div>
            <div class="mini-stat"><span>Others</span><strong>{formatNumber(payload.region.ownerMix?.other ?? 0)}</strong></div>
            <div class="mini-stat"><span>Severity High</span><strong>{formatNumber(payload.region.severityCounts.high)}</strong></div>
            <div class="mini-stat"><span>Severity Absurd</span><strong>{formatNumber(payload.region.severityCounts.absurd)}</strong></div>
          </div>
        )}
        {payload.province && (
          <div class="modal-summary-grid">
            <div class="mini-stat"><span>Paket Flagged</span><strong>{formatNumber(payload.province.totalFlaggedPackages)}</strong></div>
            <div class="mini-stat"><span>Severity Medium</span><strong>{formatNumber(payload.province.severityCounts.med)}</strong></div>
            <div class="mini-stat"><span>Severity High</span><strong>{formatNumber(payload.province.severityCounts.high)}</strong></div>
            <div class="mini-stat"><span>Severity Absurd</span><strong>{formatNumber(payload.province.severityCounts.absurd)}</strong></div>
          </div>
        )}
        {payload.owner && (
          <div class="modal-summary-grid">
            <div class="mini-stat"><span>Paket Flagged</span><strong>{formatNumber(payload.owner.totalFlaggedPackages)}</strong></div>
            <div class="mini-stat"><span>Severity Medium</span><strong>{formatNumber(payload.owner.severityCounts.med)}</strong></div>
            <div class="mini-stat"><span>Severity High</span><strong>{formatNumber(payload.owner.severityCounts.high)}</strong></div>
            <div class="mini-stat"><span>Severity Absurd</span><strong>{formatNumber(payload.owner.severityCounts.absurd)}</strong></div>
          </div>
        )}

        <div class="modal-filters">
          <input
            id="modalSearch"
            type="text"
            placeholder={modal.areaType === 'owner' ? 'Cari paket atau satker...' : 'Cari paket, lembaga, atau satker...'}
            defaultValue={modal.search}
            onInput={(e) => handleSearchInput((e.target as HTMLInputElement).value)}
          />
          {modal.areaType === 'region' && (
            <select
              value={modal.ownerType}
              onChange={(e) => dashboardStore.getState().setModalOwnerType((e.target as HTMLSelectElement).value)}
              aria-label="Filter jenis pemilik"
            >
              <option value="">Semua Pemilik</option>
              <option value="central">Kementerian/Lembaga</option>
              <option value="provinsi">Pemprov</option>
              <option value="kabkota">Pemkot</option>
              <option value="other">Others</option>
            </select>
          )}
          <select
            value={modal.severity}
            onChange={(e) =>
              dashboardStore.getState().setModalSeverity(
                (e.target as HTMLSelectElement).value as '' | import('../types/api').SeverityLevel
              )
            }
            aria-label="Filter severity"
          >
            {SEVERITY_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>{o.label}</option>
            ))}
          </select>
          <label class="chk">
            <input
              type="checkbox"
              checked={modal.priorityOnly}
              onChange={(e) =>
                dashboardStore.getState().setModalPriorityOnly((e.target as HTMLInputElement).checked)
              }
            />{' '}
            Hanya prioritas
          </label>
        </div>

        <div class="modal-cnt">
          Menampilkan {formatNumber(payload.items.length)} dari{' '}
          {formatNumber(payload.pagination.totalItems)} paket
          {areaTypeLabel}
        </div>

        <PackageTable items={payload.items} />
        <Pagination pagination={payload.pagination} />
      </div>
    </>
  );
}
