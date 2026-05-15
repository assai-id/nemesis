import type { FeatureCollection } from 'geojson';

export type OwnerType = 'central' | 'provinsi' | 'kabkota' | 'other';
export type SeverityLevel = 'low' | 'med' | 'high' | 'absurd';

export interface SeverityCounts {
  med: number;
  high: number;
  absurd: number;
}

export interface OwnerMix {
  central: number;
  provinsi: number;
  kabkota: number;
  other: number;
}

export interface AreaMetrics {
  totalPackages: number;
  totalPriorityPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
}

export interface RegionRow {
  regionKey: string;
  code: string;
  provinceName: string;
  regionName: string;
  regionType: 'Kabupaten' | 'Kota' | 'Provinsi';
  displayName: string;
  totalPackages: number;
  totalPriorityPackages: number;
  totalFlaggedPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  avgRiskScore: number;
  maxRiskScore: number | null;
  ownerMix: OwnerMix;
  ownerMetrics: Record<OwnerType, AreaMetrics>;
  severityCounts: SeverityCounts;
  dominantOwnerType: OwnerType | null;
}

export interface ProvinceRow {
  provinceKey: string;
  code: string;
  provinceName: string;
  regionType: 'Provinsi';
  displayName: string;
  totalPackages: number;
  totalPriorityPackages: number;
  totalFlaggedPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  avgRiskScore: number;
  maxRiskScore: number | null;
  ownerMix: OwnerMix;
  ownerMetrics: Record<OwnerType, AreaMetrics>;
  severityCounts: SeverityCounts;
  dominantOwnerType: OwnerType | null;
}

export interface OwnerRow {
  ownerType: OwnerType;
  ownerName: string;
  totalPackages: number;
  totalPriorityPackages: number;
  totalFlaggedPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  severityCounts: SeverityCounts;
}

export interface LegendRange {
  key: string;
  color: string;
  min: number;
  max: number;
}

export interface Legend {
  zeroColor: string;
  ranges: LegendRange[];
}

export interface NationalSummary {
  totalPackages: number;
  totalPriorityPackages: number;
  totalPotentialWaste: number;
  totalBudget: number;
  unmappedPackages: number;
  multiLocationPackages: number;
}

export interface BootstrapResponse {
  summary: NationalSummary;
  legend: Legend;
  geo: FeatureCollection;
  regions: RegionRow[];
  provinceView: {
    legend: Legend;
    geo: FeatureCollection;
    provinces: ProvinceRow[];
  };
  ownerLists: {
    central: OwnerRow[];
  };
}

export interface PackageAudit {
  schemaVersion: number;
  severity: SeverityLevel;
  potensiPemborosan: number | null;
  reason: string | null;
  flags: { isMencurigakan: boolean | null; isPemborosan: boolean | null };
}

export interface PackageMeta {
  isPriority: boolean;
  isFlagged: boolean;
  riskScore: number;
  activeTagCount: number;
  mappedRegionCount: number;
}

export interface PackageRow {
  id: number;
  sourceId: string | null;
  packageName: string;
  ownerName: string;
  ownerType: OwnerType;
  satker: string | null;
  locationRaw: string | null;
  budget: number | null;
  fundingSource: string | null;
  procurementType: string | null;
  procurementMethod: string | null;
  selectionDate: string | null;
  audit: PackageAudit;
  meta: PackageMeta;
}

export interface PaginationMeta {
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}

export interface PackagesResponse {
  items: PackageRow[];
  pagination: PaginationMeta;
  region?: RegionRow;
  province?: ProvinceRow;
  owner?: OwnerRow;
}
