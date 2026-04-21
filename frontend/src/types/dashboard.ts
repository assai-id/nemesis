export type OwnerType = "central" | "provinsi" | "kabkota" | "other";
export type Severity = "low" | "med" | "high" | "absurd";

export interface Summary {
  totalPackages: number;
  totalPriorityPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  unmappedPackages: number;
  multiLocationPackages: number;
}

export interface LegendRange {
  min: number;
  max: number;
  color: string;
}

export interface Legend {
  zeroColor: string;
  ranges: LegendRange[];
}

export interface OwnerMix {
  central: number;
  provinsi: number;
  kabkota: number;
  other: number;
}

export interface SeverityCounts {
  low: number;
  med: number;
  high: number;
  absurd: number;
}

export interface OwnerMetrics {
  totalPackages: number;
  totalPriorityPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
}

export type OwnerMetricsMap = Partial<Record<OwnerType, OwnerMetrics>>;

export interface AreaBase {
  displayName: string;
  provinceName: string;
  regionType: "Kabupaten" | "Kota" | "Provinsi";
  totalPackages: number;
  totalPriorityPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  ownerMix: OwnerMix;
  severityCounts: SeverityCounts;
  ownerMetrics?: OwnerMetricsMap;
}

export interface RegionArea extends AreaBase {
  regionKey: string;
  provinceKey?: string;
}

export interface ProvinceArea extends AreaBase {
  provinceKey: string;
  totalFlaggedPackages: number;
  avgRiskScore: number;
  maxRiskScore: number;
}

export interface OwnerSummary {
  ownerName: string;
  ownerType: OwnerType;
  totalPackages: number;
  totalPriorityPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  severityCounts: SeverityCounts;
  totalFlaggedPackages: number;
}

export interface DashboardData {
  summary: Summary;
  legend: Legend;
  geo: GeoJSON.FeatureCollection;
  regions: RegionArea[];
  provinceView: {
    legend: Legend;
    geo: GeoJSON.FeatureCollection;
    provinces: ProvinceArea[];
  };
  ownerLists: {
    central: OwnerSummary[];
  };
}

export interface PackageAudit {
  severity: Severity;
  reason: string;
}

export interface PackageItem {
  id: number | string;
  sourceId: number | string;
  packageName: string;
  ownerName: string;
  ownerType: OwnerType;
  satker: string | null;
  locationRaw: string | null;
  budget: number | null;
  audit: PackageAudit;
}

export interface PaginationInfo {
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}

export interface RegionPackagesPayload {
  region: RegionArea;
  items: PackageItem[];
  pagination: PaginationInfo;
}

export interface ProvincePackagesPayload {
  province: ProvinceArea & { totalFlaggedPackages: number };
  items: PackageItem[];
  pagination: PaginationInfo;
}

export interface OwnerPackagesPayload {
  owner: OwnerSummary;
  items: PackageItem[];
  pagination: PaginationInfo;
}

export type PackagesPayload =
  | RegionPackagesPayload
  | ProvincePackagesPayload
  | OwnerPackagesPayload;

export type MapFilter = "central" | "provinsi" | "kabkota" | "other";
export type SidebarTab = "all" | "kabupaten" | "kota";
export type SortKey = "waste" | "priority" | "packages" | "budget";

export interface DetailState {
  areaType: "region" | "province" | "owner";
  areaKey: string | null;
  ownerName: string;
  ownerType: OwnerType | "";
  page: number;
  pageSize: number;
  search: string;
  severity: Severity | "";
  priorityOnly: boolean;
}

/** @deprecated nama lama, gunakan DetailState */
export type ModalState = DetailState;
