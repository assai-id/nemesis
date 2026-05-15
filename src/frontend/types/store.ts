import type { OwnerType, SeverityLevel, RegionRow, ProvinceRow, BootstrapResponse } from './api';

export type MapFilter = 'central' | 'provinsi' | 'kabkota' | 'other';
export type TabKey = 'all' | 'kabupaten' | 'kota';
export type SortBy = 'waste' | 'priority' | 'packages' | 'budget';
export type AreaType = 'region' | 'province' | 'owner';
export type Theme = 'light' | 'dark';
export type ViewMode = 'dashboard' | 'casefile';

export interface ModalState {
  isOpen: boolean;
  areaType: AreaType;
  areaKey: string | null;
  ownerName: string;
  ownerType: string;
  page: number;
  pageSize: number;
  totalPages: number;
  search: string;
  severity: SeverityLevel | '';
  priorityOnly: boolean;
  requestId: number;
}

export interface DashboardState {
  bootstrapStatus: 'idle' | 'loading' | 'ready' | 'error';
  bootstrapError: string | null;
  data: BootstrapResponse | null;
  regionsByKey: Map<string, RegionRow>;
  provincesByKey: Map<string, ProvinceRow>;

  mapFilter: MapFilter;
  tab: TabKey;
  sortBy: SortBy;
  search: string;
  selectedAreaKey: string | null;
  selectedOwnerKey: string | null;
  isLegendHidden: boolean;
  isMapVisible: boolean;

  theme: Theme;
  viewMode: ViewMode;
  isMethodsOpen: boolean;
  breadcrumbLabel: string;

  modal: ModalState;
}

export interface DashboardActions {
  setBootstrapLoading: () => void;
  setBootstrapReady: (data: BootstrapResponse) => void;
  setBootstrapError: (error: string) => void;

  setMapFilter: (mapFilter: MapFilter) => void;
  setTab: (tab: TabKey) => void;
  setSearch: (search: string) => void;
  setSortBy: (sortBy: SortBy) => void;
  toggleLegend: () => void;
  toggleMap: () => void;

  setSelectedAreaKey: (key: string | null) => void;
  setSelectedOwnerKey: (key: string | null) => void;

  openAreaModal: (areaKey: string, areaType: AreaType) => void;
  openOwnerModal: (ownerName: string, ownerType: OwnerType) => void;
  closeModal: () => void;

  setModalSearch: (search: string) => void;
  setModalPage: (page: number) => void;
  setModalPageSize: (pageSize: number) => void;
  setModalTotalPages: (totalPages: number) => void;
  setModalOwnerType: (ownerType: string) => void;
  setModalSeverity: (severity: SeverityLevel | '') => void;
  setModalPriorityOnly: (priorityOnly: boolean) => void;

  toggleTheme: () => void;
  setTheme: (theme: Theme) => void;

  openMethods: () => void;
  closeMethods: () => void;

  setViewMode: (mode: ViewMode) => void;
  setBreadcrumbLabel: (label: string) => void;

  openAreaCaseFile: (areaKey: string, areaType: AreaType, opts?: { skipNav?: boolean }) => void;
  openOwnerCaseFile: (
    ownerName: string,
    ownerType: OwnerType,
    opts?: { skipNav?: boolean }
  ) => void;
  closeCaseFile: (opts?: { skipNav?: boolean }) => void;
  navigateHome: () => void;
  syncFromPath: () => void;
}

export type DashboardStore = DashboardState & DashboardActions;
