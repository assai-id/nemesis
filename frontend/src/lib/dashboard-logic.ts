import type {
  AreaBase,
  DashboardData,
  Legend,
  MapFilter,
  OwnerSummary,
  OwnerMetrics,
  OwnerType,
  ProvinceArea,
  RegionArea,
  SeverityCounts,
  SidebarTab,
  SortKey,
} from "@/types/dashboard";

export const FILTERS: { key: MapFilter; label: string }[] = [
  { key: "central", label: "Kementerian/Lembaga" },
  { key: "provinsi", label: "Pemprov" },
  { key: "kabkota", label: "Pemkot" },
  { key: "other", label: "Others" },
];

export const TABS: { key: SidebarTab; label: string }[] = [
  { key: "all", label: "Semua" },
  { key: "kabupaten", label: "Kabupaten" },
  { key: "kota", label: "Kota" },
];

export const SEVERITY_FILTERS = [
  { key: "", label: "Semua Severity" },
  { key: "low", label: "Low" },
  { key: "med", label: "Medium" },
  { key: "high", label: "High" },
  { key: "absurd", label: "Absurd" },
] as const;

export function isProvinceView(mapFilter: MapFilter): boolean {
  return mapFilter === "provinsi";
}

export function isCentralOwnerMode(mapFilter: MapFilter): boolean {
  return mapFilter === "central";
}

export function currentAreaType(
  mapFilter: MapFilter
): "province" | "region" {
  return isProvinceView(mapFilter) ? "province" : "region";
}

export function ownerTypeCount(
  area: AreaBase | null | undefined,
  ownerType: OwnerType
): number {
  if (!area?.ownerMix) return 0;
  return Number(area.ownerMix[ownerType]) || 0;
}

export function getOwnerCardKey(ownerType: string, ownerName: string): string {
  return `${ownerType}::${ownerName}`;
}

export function totalAreaMetrics(area: AreaBase | null | undefined): OwnerMetrics {
  return {
    totalPackages: Number(area?.totalPackages) || 0,
    totalPriorityPackages: Number(area?.totalPriorityPackages) || 0,
    totalPotentialWaste: Number(area?.totalPotentialWaste) || 0,
    totalBudget: Number(area?.totalBudget) || 0,
  };
}

export function getAreaMetricsForOwner(
  area: AreaBase | null | undefined,
  ownerKey: OwnerType,
  provinceView: boolean
): OwnerMetrics {
  if (!area) return totalAreaMetrics(null);

  const metrics = area.ownerMetrics?.[ownerKey];
  if (metrics) {
    return {
      totalPackages: Number(metrics.totalPackages) || 0,
      totalPriorityPackages: Number(metrics.totalPriorityPackages) || 0,
      totalPotentialWaste: Number(metrics.totalPotentialWaste) || 0,
      totalBudget: Number(metrics.totalBudget) || 0,
    };
  }

  if (provinceView && ownerKey === "provinsi") {
    return totalAreaMetrics(area);
  }

  return {
    totalPackages: ownerTypeCount(area, ownerKey),
    totalPriorityPackages: 0,
    totalPotentialWaste: 0,
    totalBudget: 0,
  };
}

export function getActiveSidebarOwnerKey(mapFilter: MapFilter): OwnerType {
  return isProvinceView(mapFilter) ? "provinsi" : (mapFilter as OwnerType);
}

export function getSidebarAreaMetrics(
  area: AreaBase,
  mapFilter: MapFilter
): OwnerMetrics {
  const ownerKey = getActiveSidebarOwnerKey(mapFilter);
  return getAreaMetricsForOwner(area, ownerKey, isProvinceView(mapFilter));
}

export function getLegendColor(
  legend: Legend | null | undefined,
  value: number
): string {
  if (!legend) return "#cbd5e1";
  if (!value || value <= 0) return legend.zeroColor || "#cbd5e1";
  const range = (legend.ranges || []).find(
    (item) => value >= item.min && value <= item.max
  );
  if (range) return range.color;
  const last = legend.ranges?.[legend.ranges.length - 1];
  return last?.color || "#a83c2e";
}

export function getActiveAreas(
  data: DashboardData,
  mapFilter: MapFilter
): (RegionArea | ProvinceArea)[] {
  return isProvinceView(mapFilter)
    ? data.provinceView.provinces
    : data.regions;
}

export function getActiveGeo(
  data: DashboardData,
  mapFilter: MapFilter
): GeoJSON.FeatureCollection {
  return isProvinceView(mapFilter) ? data.provinceView.geo : data.geo;
}

export function getActiveLegend(
  data: DashboardData,
  mapFilter: MapFilter
): Legend {
  return isProvinceView(mapFilter) ? data.provinceView.legend : data.legend;
}

export function getAreaKey(
  area: RegionArea | ProvinceArea,
  areaType: "region" | "province"
): string {
  return areaType === "province"
    ? (area as ProvinceArea).provinceKey
    : (area as RegionArea).regionKey;
}

export function areaMatchesCurrentView(
  area: RegionArea | ProvinceArea | null | undefined,
  mapFilter: MapFilter,
  tab: SidebarTab
): boolean {
  if (!area) return false;
  if (isProvinceView(mapFilter)) return area.totalPackages > 0;
  if (tab === "kabupaten" && area.regionType !== "Kabupaten") return false;
  if (tab === "kota" && area.regionType !== "Kota") return false;
  if (FILTERS.some((filter) => filter.key === mapFilter)) {
    return ownerTypeCount(area, mapFilter) > 0;
  }
  return true;
}

function compareSorters(
  sortBy: SortKey,
  metricsLeft: OwnerMetrics,
  metricsRight: OwnerMetrics
): number {
  switch (sortBy) {
    case "priority":
      return metricsRight.totalPriorityPackages - metricsLeft.totalPriorityPackages;
    case "packages":
      return metricsRight.totalPackages - metricsLeft.totalPackages;
    case "budget":
      return metricsRight.totalBudget - metricsLeft.totalBudget;
    case "waste":
    default:
      return (
        metricsRight.totalPotentialWaste - metricsLeft.totalPotentialWaste
      );
  }
}

export function getFilteredAreasForSidebar(
  data: DashboardData,
  mapFilter: MapFilter,
  tab: SidebarTab,
  search: string,
  sortBy: SortKey,
  activeSidebarOwnerLabel: string
): (RegionArea | ProvinceArea)[] {
  let areas = getActiveAreas(data, mapFilter).filter((area) =>
    areaMatchesCurrentView(area, mapFilter, tab)
  );

  if (search) {
    const query = search.toLowerCase();
    const ownerQuery = activeSidebarOwnerLabel.toLowerCase();
    const provView = isProvinceView(mapFilter);
    areas = areas.filter((area) => {
      const matchesName =
        area.displayName.toLowerCase().includes(query) ||
        area.provinceName.toLowerCase().includes(query);
      if (provView) return matchesName;
      return matchesName || ownerQuery.includes(query);
    });
  }

  const areaType = currentAreaType(mapFilter);
  const metricsByKey = new Map<string, OwnerMetrics>(
    areas.map((area) => [
      getAreaKey(area, areaType),
      getSidebarAreaMetrics(area, mapFilter),
    ])
  );

  return [...areas].sort((left, right) => {
    const leftMetrics = metricsByKey.get(getAreaKey(left, areaType))!;
    const rightMetrics = metricsByKey.get(getAreaKey(right, areaType))!;
    const primary = compareSorters(sortBy, leftMetrics, rightMetrics);
    return primary !== 0
      ? primary
      : left.displayName.localeCompare(right.displayName, "id");
  });
}

export function getFilteredOwnersForSidebar(
  data: DashboardData,
  search: string,
  sortBy: SortKey
): OwnerSummary[] {
  const central = data.ownerLists.central ?? [];
  let owners = central.slice();

  if (search) {
    const query = search.toLowerCase();
    owners = owners.filter((owner) =>
      owner.ownerName.toLowerCase().includes(query)
    );
  }

  return owners.sort((left, right) => {
    const primary = compareSorters(
      sortBy,
      {
        totalPackages: left.totalPackages,
        totalPriorityPackages: left.totalPriorityPackages,
        totalPotentialWaste: left.totalPotentialWaste,
        totalBudget: left.totalBudget,
      },
      {
        totalPackages: right.totalPackages,
        totalPriorityPackages: right.totalPriorityPackages,
        totalPotentialWaste: right.totalPotentialWaste,
        totalBudget: right.totalBudget,
      }
    );
    return primary !== 0
      ? primary
      : left.ownerName.localeCompare(right.ownerName, "id");
  });
}

export function areaBadgeLabel(area: AreaBase): string {
  if (area.regionType === "Provinsi") return "Prov.";
  if (area.regionType === "Kota") return "Kota";
  return "Kab.";
}

export function areaBadgeVariant(area: AreaBase): "region" | "city" {
  return area.regionType === "Kota" ? "city" : "region";
}

export function areaSecondaryLine(
  area: AreaBase,
  mapFilter: MapFilter
): string {
  return isProvinceView(mapFilter) ? "Hanya paket Pemprov" : area.provinceName;
}

/**
 * Ambil top-N wilayah berdasarkan potensi pemborosan. Default menggabungkan
 * seluruh regions (level kab/kota). Dipakai di OverviewTab.
 */
export function getTopAreas(
  data: DashboardData | null,
  limit: number = 5
): RegionArea[] {
  if (!data) return [];
  return [...data.regions]
    .filter((area) => area.totalPotentialWaste > 0)
    .sort((a, b) => b.totalPotentialWaste - a.totalPotentialWaste)
    .slice(0, limit);
}

/** Top-N Kementerian/Lembaga berdasarkan potensi pemborosan. */
export function getTopOwners(
  data: DashboardData | null,
  limit: number = 5
): OwnerSummary[] {
  if (!data) return [];
  return [...(data.ownerLists.central ?? [])]
    .filter((o) => o.totalPotentialWaste > 0)
    .sort((a, b) => b.totalPotentialWaste - a.totalPotentialWaste)
    .slice(0, limit);
}

/** Agregat severity counts dari seluruh regions. */
export function getSeverityDistribution(
  data: DashboardData | null
): SeverityCounts {
  const totals: SeverityCounts = { low: 0, med: 0, high: 0, absurd: 0 };
  if (!data) return totals;
  for (const area of data.regions) {
    totals.low += area.severityCounts?.low ?? 0;
    totals.med += area.severityCounts?.med ?? 0;
    totals.high += area.severityCounts?.high ?? 0;
    totals.absurd += area.severityCounts?.absurd ?? 0;
  }
  return totals;
}

export function ownerMixSummary(area: AreaBase): string {
  const fmt = (n: number) =>
    n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  return `K/L ${fmt(ownerTypeCount(area, "central"))} | Pemprov ${fmt(
    ownerTypeCount(area, "provinsi")
  )} | Pemkot ${fmt(ownerTypeCount(area, "kabkota"))} | Others ${fmt(
    ownerTypeCount(area, "other")
  )}`;
}
