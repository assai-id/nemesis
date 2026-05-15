import { useMemo } from 'preact/hooks';
import { useDashboardStore } from './useDashboardStore';
import { ownerTypeLabel } from '../lib/format';
import type { RegionRow, ProvinceRow, OwnerRow, AreaMetrics } from '../types/api';
import type { MapFilter } from '../types/store';

const OWNER_FILTER_KEYS: ReadonlySet<MapFilter> = new Set(['central', 'provinsi', 'kabkota', 'other']);

function getAreaMetrics(
  area: RegionRow | ProvinceRow,
  ownerKey: string,
  isProvince: boolean
): AreaMetrics {
  const metrics = (area.ownerMetrics as Record<string, AreaMetrics>)[ownerKey];
  if (metrics) return metrics;
  if (isProvince && ownerKey === 'provinsi') {
    return {
      totalPackages: area.totalPackages,
      totalPriorityPackages: area.totalPriorityPackages,
      totalPotentialWaste: area.totalPotentialWaste,
      totalBudget: area.totalBudget,
    };
  }
  const mix = (area.ownerMix as unknown as Record<string, number>)[ownerKey] ?? 0;
  return { totalPackages: mix, totalPriorityPackages: 0, totalPotentialWaste: 0, totalBudget: 0 };
}

export function useFilteredSidebarAreas(): { area: RegionRow | ProvinceRow; metrics: AreaMetrics }[] {
  // Select only stable primitives/references — never return new objects from the selector
  const data = useDashboardStore((s) => s.data);
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const tab = useDashboardStore((s) => s.tab);
  const search = useDashboardStore((s) => s.search);
  const sortBy = useDashboardStore((s) => s.sortBy);

  return useMemo(() => {
    if (!data) return [];

    const isProvince = mapFilter === 'provinsi';
    const areas: (RegionRow | ProvinceRow)[] = isProvince
      ? data.provinceView.provinces
      : data.regions;
    const ownerKey = isProvince ? 'provinsi' : mapFilter;
    const activeOwnerLabel = ownerTypeLabel(ownerKey).toLowerCase();

    const filtered = areas.filter((area) => {
      if (isProvince) return area.totalPackages > 0;
      if (tab === 'kabupaten' && (area as RegionRow).regionType !== 'Kabupaten') return false;
      if (tab === 'kota' && (area as RegionRow).regionType !== 'Kota') return false;
      if (OWNER_FILTER_KEYS.has(mapFilter)) {
        return ((area as RegionRow).ownerMix?.[mapFilter as keyof RegionRow['ownerMix']] ?? 0) > 0;
      }
      return true;
    });

    const searched = search
      ? filtered.filter((area) => {
          const q = search.toLowerCase();
          const r = area as RegionRow;
          const matchesName =
            area.displayName.toLowerCase().includes(q) ||
            (r.provinceName?.toLowerCase() ?? '').includes(q);
          if (isProvince) return matchesName;
          return matchesName || activeOwnerLabel.includes(q);
        })
      : filtered;

    const withMetrics = searched.map((area) => ({
      area,
      metrics: getAreaMetrics(area, ownerKey, isProvince),
    }));

    const sorters: Record<string, (a: (typeof withMetrics)[0], b: (typeof withMetrics)[0]) => number> =
      {
        waste: (a, b) => b.metrics.totalPotentialWaste - a.metrics.totalPotentialWaste,
        priority: (a, b) => b.metrics.totalPriorityPackages - a.metrics.totalPriorityPackages,
        packages: (a, b) => b.metrics.totalPackages - a.metrics.totalPackages,
        budget: (a, b) => b.metrics.totalBudget - a.metrics.totalBudget,
      };

    return withMetrics.sort((a, b) => {
      const primary = (sorters[sortBy] ?? sorters.waste)(a, b);
      return primary === 0 ? a.area.displayName.localeCompare(b.area.displayName, 'id') : primary;
    });
  }, [data, mapFilter, tab, search, sortBy]);
}

export function useFilteredSidebarOwners(): OwnerRow[] {
  const data = useDashboardStore((s) => s.data);
  const search = useDashboardStore((s) => s.search);
  const sortBy = useDashboardStore((s) => s.sortBy);

  return useMemo(() => {
    if (!data) return [];

    const owners = search
      ? data.ownerLists.central.filter((o) =>
          o.ownerName.toLowerCase().includes(search.toLowerCase())
        )
      : data.ownerLists.central.slice();

    const sorters: Record<string, (a: OwnerRow, b: OwnerRow) => number> = {
      waste: (a, b) => b.totalPotentialWaste - a.totalPotentialWaste,
      priority: (a, b) => b.totalPriorityPackages - a.totalPriorityPackages,
      packages: (a, b) => b.totalPackages - a.totalPackages,
      budget: (a, b) => b.totalBudget - a.totalBudget,
    };

    return owners.sort((a, b) => {
      const primary = (sorters[sortBy] ?? sorters.waste)(a, b);
      return primary === 0 ? a.ownerName.localeCompare(b.ownerName, 'id') : primary;
    });
  }, [data, search, sortBy]);
}
