import type { BootstrapResponse, PackagesResponse } from '../types/api';

function getApiBase(): string {
  return (globalThis.DASHBOARD_API_BASE_URL ?? '/api').replace(/\/$/, '');
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${getApiBase()}${path}`);
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
    const err = payload as { error?: string } | null;
    throw new Error(err?.error ?? `Request failed (${response.status})`);
  }
  return payload as T;
}

export function normalizeDashboardData(payload: unknown): BootstrapResponse {
  const p = payload as Record<string, unknown>;
  if (!p || typeof p !== 'object') throw new Error('Bootstrap payload tidak valid.');

  const pv = (p.provinceView ?? {}) as Record<string, unknown>;
  const ol = (p.ownerLists ?? {}) as Record<string, unknown>;

  return {
    summary: (p.summary as BootstrapResponse['summary']) ?? {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
      unmappedPackages: 0,
      multiLocationPackages: 0,
    },
    legend: (p.legend as BootstrapResponse['legend']) ?? { zeroColor: '#243155', ranges: [] },
    geo: (p.geo as BootstrapResponse['geo']) ?? { type: 'FeatureCollection', features: [] },
    regions: Array.isArray(p.regions) ? (p.regions as BootstrapResponse['regions']) : [],
    provinceView: {
      legend: (pv.legend as BootstrapResponse['legend']) ?? { zeroColor: '#243155', ranges: [] },
      geo: (pv.geo as BootstrapResponse['geo']) ?? { type: 'FeatureCollection', features: [] },
      provinces: Array.isArray(pv.provinces)
        ? (pv.provinces as BootstrapResponse['provinceView']['provinces'])
        : [],
    },
    ownerLists: {
      central: Array.isArray(ol.central)
        ? (ol.central as BootstrapResponse['ownerLists']['central'])
        : [],
    },
  };
}

export function fetchBootstrap(): Promise<BootstrapResponse> {
  return fetchJson<BootstrapResponse>('/bootstrap');
}

export interface PackageQueryParams {
  page: number;
  pageSize: number;
  search?: string;
  ownerType?: string;
  severity?: string;
  priorityOnly?: boolean;
}

function buildQs(params: Record<string, string | number | boolean | undefined>): string {
  const p = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '' && value !== false) {
      p.set(key, String(value));
    }
  }
  return p.toString();
}

export function fetchRegionPackages(
  regionKey: string,
  params: PackageQueryParams
): Promise<PackagesResponse> {
  const qs = buildQs({ ...params });
  return fetchJson<PackagesResponse>(`/regions/${encodeURIComponent(regionKey)}/packages?${qs}`);
}

export function fetchProvincePackages(
  provinceKey: string,
  params: PackageQueryParams
): Promise<PackagesResponse> {
  const qs = buildQs({ ...params });
  return fetchJson<PackagesResponse>(`/provinces/${encodeURIComponent(provinceKey)}/packages?${qs}`);
}

export function fetchOwnerPackages(
  ownerName: string,
  ownerType: string,
  params: PackageQueryParams
): Promise<PackagesResponse> {
  const qs = buildQs({ ...params, ownerType, ownerName });
  return fetchJson<PackagesResponse>(`/owners/packages?${qs}`);
}
