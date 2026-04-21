import type {
  DashboardData,
  OwnerPackagesPayload,
  ProvincePackagesPayload,
  RegionPackagesPayload,
} from "@/types/dashboard";

const API_BASE_URL = (
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:3000/api"
).replace(/\/$/, "");

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, { signal });
  const text = await response.text();
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      throw new Error(`Invalid JSON response from ${path}`);
    }
  }
  if (!response.ok) {
    const message =
      payload && typeof payload === "object" && "error" in payload
        ? String((payload as { error: unknown }).error)
        : `Request failed (${response.status})`;
    throw new Error(message);
  }
  return payload as T;
}

export function normalizeDashboardData(payload: unknown): DashboardData {
  if (!payload || typeof payload !== "object") {
    throw new Error("Bootstrap payload tidak valid.");
  }

  const raw = payload as Partial<DashboardData> & {
    provinceView?: Partial<DashboardData["provinceView"]>;
    ownerLists?: Partial<DashboardData["ownerLists"]>;
  };

  return {
    summary: raw.summary ?? {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
      unmappedPackages: 0,
      multiLocationPackages: 0,
    },
    legend: raw.legend ?? { zeroColor: "#cbd5e1", ranges: [] },
    geo: raw.geo ?? { type: "FeatureCollection", features: [] },
    regions: Array.isArray(raw.regions) ? raw.regions : [],
    provinceView: {
      legend: raw.provinceView?.legend ?? { zeroColor: "#cbd5e1", ranges: [] },
      geo:
        raw.provinceView?.geo ?? {
          type: "FeatureCollection",
          features: [],
        },
      provinces: Array.isArray(raw.provinceView?.provinces)
        ? raw.provinceView!.provinces!
        : [],
    },
    ownerLists: {
      central: Array.isArray(raw.ownerLists?.central)
        ? raw.ownerLists!.central!
        : [],
    },
  };
}

export async function fetchBootstrap(
  signal?: AbortSignal
): Promise<DashboardData> {
  const raw = await fetchJson<unknown>("/bootstrap", signal);
  return normalizeDashboardData(raw);
}

export interface PackageQuery {
  page: number;
  pageSize: number;
  search?: string;
  ownerType?: string;
  ownerName?: string;
  severity?: string;
  priorityOnly?: boolean;
}

function toParams(query: PackageQuery): URLSearchParams {
  const params = new URLSearchParams({
    page: String(query.page),
    pageSize: String(query.pageSize),
  });
  if (query.search) params.set("search", query.search);
  if (query.ownerType) params.set("ownerType", query.ownerType);
  if (query.ownerName) params.set("ownerName", query.ownerName);
  if (query.severity) params.set("severity", query.severity);
  if (query.priorityOnly) params.set("priorityOnly", "true");
  return params;
}

export function fetchRegionPackages(
  regionKey: string,
  query: PackageQuery,
  signal?: AbortSignal
) {
  const qs = toParams(query).toString();
  return fetchJson<RegionPackagesPayload>(
    `/regions/${encodeURIComponent(regionKey)}/packages?${qs}`,
    signal
  );
}

export function fetchProvincePackages(
  provinceKey: string,
  query: PackageQuery,
  signal?: AbortSignal
) {
  const qs = toParams(query).toString();
  return fetchJson<ProvincePackagesPayload>(
    `/provinces/${encodeURIComponent(provinceKey)}/packages?${qs}`,
    signal
  );
}

export function fetchOwnerPackages(query: PackageQuery, signal?: AbortSignal) {
  const qs = toParams(query).toString();
  return fetchJson<OwnerPackagesPayload>(`/owners/packages?${qs}`, signal);
}
