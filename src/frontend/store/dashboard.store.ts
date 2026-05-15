import { createStore } from 'zustand/vanilla';
import type { BootstrapResponse, SeverityLevel, OwnerType, RegionRow, ProvinceRow } from '../types/api';
import type {
  DashboardStore,
  MapFilter,
  TabKey,
  SortBy,
  AreaType,
  ModalState,
  Theme,
  ViewMode,
} from '../types/store';
import { applyThemeToBootstrap } from '../lib/api';

const INITIAL_MODAL: ModalState = {
  isOpen: false,
  areaType: 'region',
  areaKey: null,
  ownerName: '',
  ownerType: '',
  page: 1,
  pageSize: 25,
  totalPages: 1,
  search: '',
  severity: '',
  priorityOnly: false,
  requestId: 0,
};

const THEME_STORAGE_KEY = 'nemesis-theme';

function readInitialTheme(): Theme {
  if (typeof document === 'undefined') return 'light';
  const attr = document.documentElement.getAttribute('data-theme');
  return attr === 'dark' ? 'dark' : 'light';
}

function applyThemeToDocument(theme: Theme) {
  if (typeof document === 'undefined') return;
  document.documentElement.setAttribute('data-theme', theme);
  const meta = document.getElementById('metaThemeColor');
  if (meta) meta.setAttribute('content', theme === 'dark' ? '#111d35' : '#fafaf6');
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    /* ignore quota / privacy errors */
  }
}

function suppressPopstate(fn: () => void) {
  // Best-effort: pushState then run fn. Browsers don't fire popstate for pushState,
  // so no actual suppression is needed — kept as a helper for readability.
  fn();
}

function buildCaseFilePath(modal: ModalState): string {
  if (modal.areaType === 'owner' && modal.ownerType && modal.ownerName) {
    return `/owner/${encodeURIComponent(modal.ownerType)}/${encodeURIComponent(modal.ownerName)}`;
  }
  if (modal.areaType === 'province' && modal.areaKey) {
    return `/provinsi/${encodeURIComponent(modal.areaKey)}`;
  }
  if (modal.areaType === 'region' && modal.areaKey) {
    return `/wilayah/${encodeURIComponent(modal.areaKey)}`;
  }
  return '/';
}

function pushCaseFilePath(modal: ModalState) {
  if (typeof history === 'undefined' || typeof location === 'undefined') return;
  const path = buildCaseFilePath(modal);
  if (!path || path === location.pathname) return;
  suppressPopstate(() => history.pushState({ view: 'casefile' }, '', path));
}

function pushHomePath() {
  if (typeof history === 'undefined' || typeof location === 'undefined') return;
  if (location.pathname === '/' || location.pathname === '') return;
  suppressPopstate(() => history.pushState({ view: 'dashboard' }, '', '/'));
}

interface ParsedRoute {
  kind: AreaType;
  areaKey?: string;
  ownerType?: OwnerType;
  ownerName?: string;
}

function parseCurrentPath(): ParsedRoute | null {
  if (typeof location === 'undefined') return null;
  const path = location.pathname || '/';
  if (path === '/' || path === '') return null;
  const parts = path
    .split('/')
    .filter(Boolean)
    .map((p) => {
      try {
        return decodeURIComponent(p);
      } catch {
        return p;
      }
    });
  if (parts[0] === 'wilayah' && parts[1]) return { kind: 'region', areaKey: parts[1] };
  if (parts[0] === 'provinsi' && parts[1]) return { kind: 'province', areaKey: parts[1] };
  if (parts[0] === 'owner' && parts[1] && parts[2]) {
    return { kind: 'owner', ownerType: parts[1] as OwnerType, ownerName: parts[2] };
  }
  return null;
}

export const dashboardStore = createStore<DashboardStore>()((set, get) => ({
  bootstrapStatus: 'idle',
  bootstrapError: null,
  data: null,
  regionsByKey: new Map<string, RegionRow>(),
  provincesByKey: new Map<string, ProvinceRow>(),

  mapFilter: 'central',
  tab: 'all',
  sortBy: 'waste',
  search: '',
  selectedAreaKey: null,
  selectedOwnerKey: null,
  isLegendHidden: false,
  isMapVisible: true,

  theme: readInitialTheme(),
  viewMode: 'dashboard',
  isMethodsOpen: false,
  breadcrumbLabel: 'Berkas',

  modal: { ...INITIAL_MODAL },

  setBootstrapLoading: () => set({ bootstrapStatus: 'loading' }),

  setBootstrapReady: (data: BootstrapResponse) =>
    set({
      bootstrapStatus: 'ready',
      data,
      regionsByKey: new Map(data.regions.map((r) => [r.regionKey, r])),
      provincesByKey: new Map(data.provinceView.provinces.map((p) => [p.provinceKey, p])),
    }),

  setBootstrapError: (error: string) => set({ bootstrapStatus: 'error', bootstrapError: error }),

  setMapFilter: (mapFilter: MapFilter) =>
    set((state) => {
      const wasProvince = state.mapFilter === 'provinsi';
      const nowProvince = mapFilter === 'provinsi';
      const viewChanged = wasProvince !== nowProvince;
      const wasCentral = state.mapFilter === 'central';
      const nowCentral = mapFilter === 'central';
      const centralChanged = wasCentral !== nowCentral;

      const updates: Partial<DashboardStore> = { mapFilter };

      if (viewChanged || centralChanged) {
        updates.tab = 'all';
        updates.selectedAreaKey = null;
        updates.selectedOwnerKey = null;
      }

      if (viewChanged) {
        updates.modal = { ...INITIAL_MODAL, areaType: nowProvince ? 'province' : 'region' };
        updates.viewMode = 'dashboard';
      } else if (centralChanged && wasCentral && state.modal.areaType === 'owner') {
        updates.modal = { ...INITIAL_MODAL, requestId: state.modal.requestId + 1 };
        updates.viewMode = 'dashboard';
      }

      return updates;
    }),

  setTab: (tab: TabKey) => set({ tab }),

  setSearch: (search: string) => set({ search }),

  setSortBy: (sortBy: SortBy) => set({ sortBy }),

  toggleLegend: () => set((s) => ({ isLegendHidden: !s.isLegendHidden })),

  toggleMap: () => set((s) => ({ isMapVisible: !s.isMapVisible })),

  setSelectedAreaKey: (key: string | null) => set({ selectedAreaKey: key }),

  setSelectedOwnerKey: (key: string | null) => set({ selectedOwnerKey: key }),

  openAreaModal: (areaKey: string, areaType: AreaType) => {
    get().openAreaCaseFile(areaKey, areaType);
  },

  openOwnerModal: (ownerName: string, ownerType: OwnerType) => {
    get().openOwnerCaseFile(ownerName, ownerType);
  },

  closeModal: () => {
    get().closeCaseFile();
  },

  setModalSearch: (search: string) =>
    set((s) => ({
      modal: { ...s.modal, search, page: 1, requestId: s.modal.requestId + 1 },
    })),

  setModalPage: (page: number) =>
    set((s) => ({
      modal: { ...s.modal, page, requestId: s.modal.requestId + 1 },
    })),

  setModalPageSize: (pageSize: number) =>
    set((s) => {
      const valid = [25, 50, 100, 250];
      const next = valid.includes(pageSize) ? pageSize : 25;
      if (s.modal.pageSize === next) return s;
      return {
        modal: { ...s.modal, pageSize: next, page: 1, requestId: s.modal.requestId + 1 },
      };
    }),

  setModalTotalPages: (totalPages: number) =>
    set((s) => {
      if (s.modal.totalPages === totalPages) return s;
      return { modal: { ...s.modal, totalPages } };
    }),

  setModalOwnerType: (ownerType: string) =>
    set((s) => {
      if (s.modal.areaType === 'province' || s.modal.areaType === 'owner') return s;
      return { modal: { ...s.modal, ownerType, page: 1, requestId: s.modal.requestId + 1 } };
    }),

  setModalSeverity: (severity: SeverityLevel | '') =>
    set((s) => ({
      modal: { ...s.modal, severity, page: 1, requestId: s.modal.requestId + 1 },
    })),

  setModalPriorityOnly: (priorityOnly: boolean) =>
    set((s) => ({
      modal: { ...s.modal, priorityOnly, page: 1, requestId: s.modal.requestId + 1 },
    })),

  toggleTheme: () => {
    const next: Theme = get().theme === 'dark' ? 'light' : 'dark';
    applyThemeToDocument(next);
    const current = get().data;
    set({
      theme: next,
      data: current ? applyThemeToBootstrap(current, next) : current,
    });
    if (typeof globalThis !== 'undefined' && globalThis.AuditMap?.setTheme) {
      globalThis.AuditMap.setTheme();
    }
  },

  setTheme: (theme: Theme) => {
    applyThemeToDocument(theme);
    const current = get().data;
    set({
      theme,
      data: current ? applyThemeToBootstrap(current, theme) : current,
    });
    if (typeof globalThis !== 'undefined' && globalThis.AuditMap?.setTheme) {
      globalThis.AuditMap.setTheme();
    }
  },

  openMethods: () => set({ isMethodsOpen: true }),
  closeMethods: () => set({ isMethodsOpen: false }),

  setViewMode: (mode: ViewMode) => set({ viewMode: mode }),
  setBreadcrumbLabel: (label: string) => set({ breadcrumbLabel: label || 'Berkas' }),

  openAreaCaseFile: (areaKey, areaType, opts) => {
    if (typeof globalThis !== 'undefined' && globalThis.AuditMap?.closePopup) {
      globalThis.AuditMap.closePopup();
    }
    set((s) => ({
      viewMode: 'casefile',
      selectedAreaKey: areaKey,
      selectedOwnerKey: null,
      modal: {
        ...INITIAL_MODAL,
        isOpen: true,
        areaType,
        areaKey,
        requestId: s.modal.requestId + 1,
      },
    }));
    if (!opts?.skipNav) pushCaseFilePath(get().modal);
  },

  openOwnerCaseFile: (ownerName, ownerType, opts) => {
    if (typeof globalThis !== 'undefined' && globalThis.AuditMap?.closePopup) {
      globalThis.AuditMap.closePopup();
    }
    set((s) => ({
      viewMode: 'casefile',
      selectedAreaKey: null,
      selectedOwnerKey: `${ownerType}::${ownerName}`,
      modal: {
        ...INITIAL_MODAL,
        isOpen: true,
        areaType: 'owner',
        ownerName,
        ownerType,
        requestId: s.modal.requestId + 1,
      },
    }));
    if (!opts?.skipNav) pushCaseFilePath(get().modal);
  },

  closeCaseFile: (opts) => {
    set((s) => ({
      viewMode: 'dashboard',
      modal: {
        ...INITIAL_MODAL,
        areaType: s.mapFilter === 'provinsi' ? 'province' : 'region',
        requestId: s.modal.requestId + 1,
      },
      breadcrumbLabel: 'Berkas',
    }));
    if (!opts?.skipNav) pushHomePath();
  },

  navigateHome: () => {
    if (typeof location !== 'undefined' && (location.pathname === '/' || location.pathname === '')) {
      set({ viewMode: 'dashboard' });
      return;
    }
    pushHomePath();
    get().closeCaseFile({ skipNav: true });
  },

  syncFromPath: () => {
    const target = parseCurrentPath();
    const state = get();
    if (!target) {
      if (state.viewMode === 'casefile') state.closeCaseFile({ skipNav: true });
      return;
    }
    if (target.kind === 'owner' && target.ownerName && target.ownerType) {
      if (state.mapFilter !== 'central') {
        set({ mapFilter: 'central' });
      }
      state.openOwnerCaseFile(target.ownerName, target.ownerType, { skipNav: true });
    } else if (target.kind === 'province' && target.areaKey) {
      if (state.mapFilter !== 'provinsi') {
        set({ mapFilter: 'provinsi' });
      }
      state.openAreaCaseFile(target.areaKey, 'province', { skipNav: true });
    } else if (target.kind === 'region' && target.areaKey) {
      if (state.mapFilter === 'provinsi' || state.mapFilter === 'central') {
        set({ mapFilter: 'kabkota' });
      }
      state.openAreaCaseFile(target.areaKey, 'region', { skipNav: true });
    }
  },
}));
