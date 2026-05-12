import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { useFilteredSidebarAreas, useFilteredSidebarOwners } from '../hooks/useFilteredSidebarItems';
import { AreaCard } from './AreaCard';
import { OwnerCard } from './OwnerCard';
import type { BootstrapResponse, Legend, OwnerRow, RegionRow, ProvinceRow, AreaMetrics } from '../types/api';
import type { MapFilter } from '../types/store';

function getActiveLegend(data: BootstrapResponse, mapFilter: MapFilter): Legend {
  return mapFilter === 'provinsi' ? data.provinceView.legend : data.legend;
}

function buildListContent(
  isCentralMode: boolean,
  isProvince: boolean,
  ownerItems: OwnerRow[],
  areaItems: Array<{ area: RegionRow | ProvinceRow; metrics: AreaMetrics }>,
  legend: Legend,
  mapFilter: MapFilter,
) {
  if (isCentralMode) {
    if (ownerItems.length === 0) {
      return <div class="panel-msg">Tidak ada kementerian/lembaga yang cocok dengan filter saat ini.</div>;
    }
    const maxWaste = Math.max(...ownerItems.map((o) => o.totalPotentialWaste), 1);
    return ownerItems.map((owner, i) => (
      <OwnerCard key={`${owner.ownerType}::${owner.ownerName}`} owner={owner} rank={i + 1} maxWaste={maxWaste} legend={legend} />
    ));
  }
  if (areaItems.length === 0) {
    return (
      <div class="panel-msg">
        Tidak ada {isProvince ? 'provinsi' : 'region'} yang cocok dengan filter saat ini.
      </div>
    );
  }
  const maxWaste = Math.max(...areaItems.map(({ metrics }) => metrics.totalPotentialWaste), 1);
  return areaItems.map(({ area, metrics }, i) => {
    const areaKey = isProvince
      ? (area as ProvinceRow).provinceKey
      : (area as RegionRow).regionKey;
    return (
      <AreaCard key={areaKey} area={area} metrics={metrics} rank={i + 1} maxWaste={maxWaste} legend={legend} mapFilter={mapFilter} />
    );
  });
}

export function SidebarList() {
  const data = useDashboardStore((s) => s.data);
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const search = useDashboardStore((s) => s.search);
  const sortBy = useDashboardStore((s) => s.sortBy);
  const bootstrapStatus = useDashboardStore((s) => s.bootstrapStatus);
  const isCentralMode = mapFilter === 'central';

  const areaItems = useFilteredSidebarAreas();
  const ownerItems = useFilteredSidebarOwners();

  if (bootstrapStatus === 'loading' || bootstrapStatus === 'idle') {
    return <div class="panel-msg">Memuat audit pengadaan per area...</div>;
  }

  if (bootstrapStatus === 'error' || !data) {
    return (
      <div class="panel-msg error">
        Gagal memuat dashboard audit.
      </div>
    );
  }

  const legend = getActiveLegend(data, mapFilter);
  const isProvince = mapFilter === 'provinsi';

  let searchPlaceholder: string;
  if (isCentralMode) {
    searchPlaceholder = 'Cari kementerian/lembaga...';
  } else {
    searchPlaceholder = isProvince ? 'Cari provinsi...' : 'Cari kabupaten/kota...';
  }

  const listContent = buildListContent(isCentralMode, isProvince, ownerItems, areaItems, legend, mapFilter);

  return (
    <>
      <div class="sw">
        <span class="si">&#128269;</span>
        <input
          id="sidebarSearch"
          type="text"
          placeholder={searchPlaceholder}
          value={search}
          onInput={(e) => dashboardStore.getState().setSearch((e.target as HTMLInputElement).value)}
        />
      </div>
      <div class="sort-bar">
        <label htmlFor="sortSelect">Urutkan</label>
        <select
          id="sortSelect"
          value={sortBy}
          onChange={(e) =>
            dashboardStore.getState().setSortBy(
              (e.target as HTMLSelectElement).value as import('../types/store').SortBy
            )
          }
        >
          <option value="waste">Potensi Pemborosan</option>
          <option value="priority">Paket Prioritas</option>
          <option value="packages">Total Paket</option>
          <option value="budget">Total Pagu</option>
        </select>
      </div>

      {listContent}
    </>
  );
}
