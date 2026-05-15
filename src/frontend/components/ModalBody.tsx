import { useState, useEffect, useRef } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { fetchRegionPackages, fetchProvincePackages, fetchOwnerPackages } from '../lib/api';
import {
  formatCompactCurrency,
  formatDecimal,
  formatNumber,
  ownerTypeLabel,
} from '../lib/format';
import { PackageTable } from './PackageTable';
import { Pagination } from './Pagination';
import { CaseFileHero, pickFeaturedItem } from './CaseFileHero';
import { CaseFileSummary, type SummarySection, type SummaryItem } from './CaseFileSummary';
import type {
  PackagesResponse,
  RegionRow,
  ProvinceRow,
  OwnerRow,
  SeverityLevel,
} from '../types/api';

interface FetchState {
  status: 'loading' | 'ready' | 'error';
  data: PackagesResponse | null;
  error: string | null;
}

const SEVERITY_FILTER_OPTIONS: { value: string; label: string }[] = [
  { value: '', label: 'Semua paket' },
  { value: 'priority', label: 'Hanya prioritas (Medium ke atas)' },
  { value: 'low', label: 'Hanya Low' },
  { value: 'med', label: 'Hanya Medium' },
  { value: 'high', label: 'Hanya High' },
  { value: 'absurd', label: 'Hanya Absurd' },
];

function currentSeverityFilterValue(severity: string, priorityOnly: boolean): string {
  if (priorityOnly) return 'priority';
  return severity || '';
}

function ownerTypeCount(region: RegionRow, key: keyof RegionRow['ownerMix']): number {
  return Number(region.ownerMix?.[key] ?? 0);
}

interface CaseFileMeta {
  heroLabel: string;
  heroValue: string;
  heroSub: string;
  secondary: SummaryItem[];
  sections: SummarySection[];
  title: string;
  description: string;
  breadcrumb: string;
  caseTitle: string;
  cntLabel: string;
  searchPlaceholder: string;
}

function regionMeta(region: RegionRow, totalItems: number, shownItems: number): CaseFileMeta {
  const central = ownerTypeCount(region, 'central');
  const provinsi = ownerTypeCount(region, 'provinsi');
  const kabkota = ownerTypeCount(region, 'kabkota');
  const other = ownerTypeCount(region, 'other');
  return {
    heroLabel: 'Potensi Pemborosan',
    heroValue: `Rp ${formatCompactCurrency(region.totalPotentialWaste)}`,
    heroSub: `${formatNumber(region.totalPriorityPackages)} paket prioritas`,
    secondary: [
      { label: 'Total Paket', value: formatNumber(region.totalPackages) },
      { label: 'Pagu Teraudit', value: `Rp ${formatCompactCurrency(region.totalBudget)}` },
    ],
    sections: [
      {
        title: 'Pemilik',
        items: [
          { label: 'Kementerian/Lembaga', value: formatNumber(central), muted: central === 0 },
          { label: 'Pemprov', value: formatNumber(provinsi), muted: provinsi === 0 },
          { label: 'Pemkot', value: formatNumber(kabkota), muted: kabkota === 0 },
          { label: 'Others', value: formatNumber(other), muted: other === 0 },
        ],
      },
      {
        title: 'Severity',
        items: [
          { label: 'High', value: formatNumber(region.severityCounts.high), muted: region.severityCounts.high === 0 },
          { label: 'Absurd', value: formatNumber(region.severityCounts.absurd), muted: region.severityCounts.absurd === 0 },
        ],
      },
    ],
    title: `${region.displayName} · Audit Pengadaan TA 2026 · Nemesis`,
    description: `Berkas audit ${region.displayName}, ${region.provinceName}: ${formatNumber(region.totalPriorityPackages)} paket prioritas, potensi pemborosan Rp ${formatCompactCurrency(region.totalPotentialWaste)}.`,
    breadcrumb: `${region.provinceName} · ${region.displayName}`,
    caseTitle: region.displayName,
    cntLabel: `Menampilkan ${formatNumber(shownItems)} dari ${formatNumber(totalItems)} paket pada wilayah ini`,
    searchPlaceholder: 'Cari paket, lembaga, atau satker…',
  };
}

function provinceMeta(province: ProvinceRow, totalItems: number, shownItems: number): CaseFileMeta {
  return {
    heroLabel: 'Potensi Pemborosan',
    heroValue: `Rp ${formatCompactCurrency(province.totalPotentialWaste)}`,
    heroSub: `${formatNumber(province.totalPriorityPackages)} paket prioritas`,
    secondary: [
      { label: 'Paket Pemprov', value: formatNumber(province.totalPackages) },
      { label: 'Pagu Teraudit', value: `Rp ${formatCompactCurrency(province.totalBudget)}` },
    ],
    sections: [
      {
        title: 'Severity',
        items: [
          { label: 'Medium', value: formatNumber(province.severityCounts.med), muted: province.severityCounts.med === 0 },
          { label: 'High', value: formatNumber(province.severityCounts.high), muted: province.severityCounts.high === 0 },
          { label: 'Absurd', value: formatNumber(province.severityCounts.absurd), muted: province.severityCounts.absurd === 0 },
        ],
      },
      {
        title: 'Risk Score',
        items: [
          { label: 'Rata-rata', value: formatDecimal(province.avgRiskScore) },
          { label: 'Maksimum', value: formatNumber(province.maxRiskScore) },
          { label: 'Flagged', value: formatNumber(province.totalFlaggedPackages) },
        ],
      },
    ],
    title: `${province.displayName} · Audit Pengadaan Pemprov · Nemesis`,
    description: `Berkas audit pemprov ${province.displayName}: ${formatNumber(province.totalPriorityPackages)} paket prioritas, potensi pemborosan Rp ${formatCompactCurrency(province.totalPotentialWaste)}.`,
    breadcrumb: `Provinsi · ${province.displayName}`,
    caseTitle: province.displayName,
    cntLabel: `Menampilkan ${formatNumber(shownItems)} dari ${formatNumber(totalItems)} paket pemprov pada provinsi ini`,
    searchPlaceholder: 'Cari paket, lembaga, atau satker…',
  };
}

function ownerMeta(owner: OwnerRow, totalItems: number, shownItems: number): CaseFileMeta {
  return {
    heroLabel: 'Potensi Pemborosan',
    heroValue: `Rp ${formatCompactCurrency(owner.totalPotentialWaste)}`,
    heroSub: `${formatNumber(owner.totalPriorityPackages)} paket prioritas`,
    secondary: [
      { label: 'Total Paket', value: formatNumber(owner.totalPackages) },
      { label: 'Pagu Teraudit', value: `Rp ${formatCompactCurrency(owner.totalBudget)}` },
      { label: 'Paket Flagged', value: formatNumber(owner.totalFlaggedPackages) },
    ],
    sections: [
      {
        title: 'Severity',
        items: [
          { label: 'Medium', value: formatNumber(owner.severityCounts.med), muted: owner.severityCounts.med === 0 },
          { label: 'High', value: formatNumber(owner.severityCounts.high), muted: owner.severityCounts.high === 0 },
          { label: 'Absurd', value: formatNumber(owner.severityCounts.absurd), muted: owner.severityCounts.absurd === 0 },
        ],
      },
    ],
    title: `${owner.ownerName} · Audit Pengadaan TA 2026 · Nemesis`,
    description: `Berkas audit ${owner.ownerName}: ${formatNumber(owner.totalPriorityPackages)} paket prioritas, potensi pemborosan Rp ${formatCompactCurrency(owner.totalPotentialWaste)}.`,
    breadcrumb: `${ownerTypeLabel(owner.ownerType)} · ${owner.ownerName}`,
    caseTitle: owner.ownerName,
    cntLabel: `Menampilkan ${formatNumber(shownItems)} dari ${formatNumber(totalItems)} paket pada pemilik ini`,
    searchPlaceholder: 'Cari paket atau satker…',
  };
}

function setPageMeta(title: string, description: string) {
  if (typeof document === 'undefined') return;
  document.title = title;
  const meta = document.querySelector('meta[name="description"]');
  if (meta && description) meta.setAttribute('content', description);
}

const DEFAULT_PAGE_TITLE = 'Nemesis · Audit Pengadaan Nasional · TA 2026';
const DEFAULT_PAGE_DESCRIPTION =
  'Berkas perkara publik atas anomali pengadaan barang/jasa pemerintah Indonesia. Operasi Diponegoro · Abil Sudarman School of AI.';

export function ModalBody() {
  const modal = useDashboardStore((s) => s.modal);
  const viewMode = useDashboardStore((s) => s.viewMode);
  const [fetchState, setFetchState] = useState<FetchState>({
    status: 'loading',
    data: null,
    error: null,
  });
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingSearchRef = useRef<string | null>(null);
  const tableWrapRef = useRef<HTMLDivElement>(null);

  // Fetch packages whenever requestId changes
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
        if (cancelled) return;
        setFetchState({ status: 'ready', data, error: null });
        dashboardStore.getState().setModalTotalPages(data.pagination.totalPages);
        searchInputRef.current?.classList.remove('is-searching');
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setFetchState({
          status: 'error',
          data: null,
          error: err instanceof Error ? err.message : String(err),
        });
        searchInputRef.current?.classList.remove('is-searching');
      });

    return () => {
      cancelled = true;
    };
  }, [modal.requestId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Update breadcrumb + document.title once a payload is ready
  useEffect(() => {
    if (fetchState.status !== 'ready' || !fetchState.data) return;
    const meta = buildMetaFromPayload(modal.areaType, fetchState.data);
    if (!meta) return;
    dashboardStore.getState().setBreadcrumbLabel(meta.breadcrumb);
    setPageMeta(meta.title, meta.description);
  }, [fetchState.status, fetchState.data, modal.areaType]);

  // Reset document.title when leaving case file
  useEffect(() => {
    if (viewMode === 'dashboard') {
      setPageMeta(DEFAULT_PAGE_TITLE, DEFAULT_PAGE_DESCRIPTION);
    }
  }, [viewMode]);

  // Table overflow affordance — toggle .has-overflow when horizontal scroll exists
  useEffect(() => {
    const wrap = tableWrapRef.current;
    if (!wrap) return;
    const update = () => {
      const overflowing = wrap.scrollWidth > wrap.clientWidth + 1;
      const atEnd = wrap.scrollLeft + wrap.clientWidth >= wrap.scrollWidth - 1;
      wrap.classList.toggle('has-overflow', overflowing && !atEnd);
    };
    wrap.addEventListener('scroll', update, { passive: true });
    update();
    requestAnimationFrame(update);
    return () => wrap.removeEventListener('scroll', update);
  }, [fetchState.data]);

  function handleSearchInput(value: string) {
    pendingSearchRef.current = value;
    searchInputRef.current?.classList.add('is-searching');
    if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    searchTimeoutRef.current = setTimeout(() => {
      dashboardStore.getState().setModalSearch(pendingSearchRef.current ?? '');
    }, 300);
  }

  function handleSeverityChange(raw: string) {
    if (raw === 'priority') {
      dashboardStore.getState().setModalPriorityOnly(true);
      dashboardStore.getState().setModalSeverity('');
    } else {
      dashboardStore.getState().setModalPriorityOnly(false);
      dashboardStore.getState().setModalSeverity(raw as SeverityLevel | '');
    }
  }

  if (!modal.isOpen) return null;

  if (fetchState.status === 'loading') {
    return (
      <div class="modal-state">
        {modal.areaType === 'owner'
          ? 'Mengambil paket dari pemilik terpilih...'
          : 'Mengambil paket dari backend audit...'}
      </div>
    );
  }

  if (fetchState.status === 'error') {
    return <div class="modal-state error">Gagal memuat paket: {fetchState.error}</div>;
  }

  const payload = fetchState.data!;
  const meta = buildMetaFromPayload(modal.areaType, payload);
  if (!meta) {
    return <div class="modal-state">Data berkas tidak tersedia.</div>;
  }

  const featured = pickFeaturedItem(payload.items);
  const severityValue = currentSeverityFilterValue(modal.severity || '', modal.priorityOnly);

  return (
    <>
      <div class="case-grid">
        <CaseFileHero featured={featured} />
        <CaseFileSummary
          heroLabel={meta.heroLabel}
          heroValue={meta.heroValue}
          heroSub={meta.heroSub}
          secondary={meta.secondary}
          sections={meta.sections}
        />
      </div>

      <div class="modal-filters">
        <input
          ref={searchInputRef}
          id="modalSearch"
          type="search"
          placeholder={meta.searchPlaceholder}
          defaultValue={modal.search}
          aria-label={meta.searchPlaceholder}
          onInput={(e) => handleSearchInput((e.target as HTMLInputElement).value)}
        />
        <select
          aria-label="Filter berdasarkan severity"
          value={severityValue}
          onChange={(e) => handleSeverityChange((e.target as HTMLSelectElement).value)}
        >
          {SEVERITY_FILTER_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        {modal.areaType === 'region' && (
          <select
            aria-label="Filter jenis pemilik"
            value={modal.ownerType}
            onChange={(e) =>
              dashboardStore.getState().setModalOwnerType((e.target as HTMLSelectElement).value)
            }
          >
            <option value="">Semua Pemilik</option>
            <option value="central">Kementerian/Lembaga</option>
            <option value="provinsi">Pemprov</option>
            <option value="kabkota">Pemkot</option>
            <option value="other">Others</option>
          </select>
        )}
      </div>

      <div class="modal-cnt">{meta.cntLabel}</div>

      <div
        ref={tableWrapRef}
        class="table-wrap"
        tabIndex={0}
        role="region"
        aria-label="Tabel paket"
      >
        <PackageTable items={payload.items} />
      </div>

      <Pagination pagination={payload.pagination} />
    </>
  );
}

function buildMetaFromPayload(
  areaType: 'region' | 'province' | 'owner',
  payload: PackagesResponse,
): CaseFileMeta | null {
  const total = payload.pagination.totalItems;
  const shown = payload.items.length;
  if (areaType === 'owner' && payload.owner) return ownerMeta(payload.owner, total, shown);
  if (areaType === 'province' && payload.province) return provinceMeta(payload.province, total, shown);
  if (payload.region) return regionMeta(payload.region, total, shown);
  return null;
}
