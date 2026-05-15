import { useEffect, useRef, useState } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { formatNumber } from '../lib/format';
import { useFilteredSidebarAreas, useFilteredSidebarOwners } from '../hooks/useFilteredSidebarItems';
import type { TabKey, MapFilter, SortBy } from '../types/store';

const TABS: { key: TabKey; label: string }[] = [
  { key: 'all', label: 'Semua' },
  { key: 'kabupaten', label: 'Kabupaten' },
  { key: 'kota', label: 'Kota' },
];

const FILTERS: { key: MapFilter; label: string }[] = [
  { key: 'central', label: 'Kementerian/Lembaga' },
  { key: 'provinsi', label: 'Pemprov' },
  { key: 'kabkota', label: 'Pemkot' },
  { key: 'other', label: 'Others' },
];

export function SidebarHeader() {
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const tab = useDashboardStore((s) => s.tab);
  const search = useDashboardStore((s) => s.search);
  const sortBy = useDashboardStore((s) => s.sortBy);
  const bootstrapStatus = useDashboardStore((s) => s.bootstrapStatus);

  const isProvince = mapFilter === 'provinsi';
  const isCentral = mapFilter === 'central';
  const tabsDisabled = isProvince || isCentral;

  // Debounce search to avoid re-sorting the entire list on every keystroke.
  const [searchInput, setSearchInput] = useState(search);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => setSearchInput(search), [search]);
  useEffect(() => () => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
  }, []);
  function handleSearchInput(value: string) {
    setSearchInput(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      dashboardStore.getState().setSearch(value);
    }, 180);
  }

  const areaItems = useFilteredSidebarAreas();
  const ownerItems = useFilteredSidebarOwners();
  const count = isCentral ? ownerItems.length : areaItems.length;
  const showCount = bootstrapStatus === 'ready';

  let placeholder: string;
  if (isCentral) placeholder = 'Cari kementerian/lembaga…';
  else if (isProvince) placeholder = 'Cari provinsi…';
  else placeholder = 'Cari kabupaten/kota…';

  return (
    <div class="sbh">
      <div class="sbh-row sbh-toolbar">
        <h2 class="list-title">
          Wilayah
          <span class="list-title-count" id="listCount" aria-live="polite">
            {showCount ? ` · ${formatNumber(count)} entri` : ''}
          </span>
        </h2>
        <button
          type="button"
          class="map-toggle map-toggle-show"
          id="toggleMapBtnShow"
          aria-pressed="true"
          onClick={() => dashboardStore.getState().toggleMap()}
        >
          <span aria-hidden="true">▦</span> Tampilkan peta
        </button>
        <div class="sbt" id="tabs" role="tablist" aria-label="Filter jenis wilayah">
          {TABS.map((t) => {
            const active = tabsDisabled ? t.key === 'all' : t.key === tab;
            const disabled = tabsDisabled && t.key !== 'all';
            return (
              <button
                key={t.key}
                type="button"
                role="tab"
                aria-selected={active}
                aria-disabled={disabled}
                disabled={disabled}
                class={`stb${active ? ' a' : ''}`}
                onClick={() => dashboardStore.getState().setTab(disabled ? 'all' : t.key)}
              >
                {t.label}
              </button>
            );
          })}
        </div>
      </div>
      <div class="sbh-row sbh-sort">
        <div class="sort-bar">
          <label for="sidebarSort">Urutkan</label>
          <select
            id="sidebarSort"
            value={sortBy}
            aria-label="Urutkan wilayah"
            onChange={(e) =>
              dashboardStore
                .getState()
                .setSortBy((e.target as HTMLSelectElement).value as SortBy)
            }
          >
            <option value="waste">Potensi Pemborosan</option>
            <option value="priority">Paket Prioritas</option>
            <option value="packages">Total Paket</option>
            <option value="budget">Total Pagu</option>
          </select>
        </div>
      </div>
      <div class="sbh-row sbh-controls" id="sidebarControls">
        <div class="sw">
          <span class="si" aria-hidden="true">⌕</span>
          <input
            id="sidebarSearch"
            type="search"
            placeholder={placeholder}
            value={searchInput}
            aria-label={placeholder}
            onInput={(e) => handleSearchInput((e.target as HTMLInputElement).value)}
          />
        </div>
      </div>
      <div class="sbh-row sbh-mfsb">
        <div class="moc-sb" id="mfsb" role="tablist" aria-label="Filter pemilik">
          {FILTERS.map((f) => (
            <button
              key={f.key}
              type="button"
              role="tab"
              aria-selected={f.key === mapFilter}
              class={`fc${f.key === mapFilter ? ' a' : ''}`}
              onClick={() => dashboardStore.getState().setMapFilter(f.key)}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
