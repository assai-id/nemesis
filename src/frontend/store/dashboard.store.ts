import { createStore } from 'zustand/vanilla';
import type { BootstrapResponse, SeverityLevel, OwnerType, RegionRow, ProvinceRow } from '../types/api';
import type { DashboardStore, MapFilter, TabKey, SortBy, AreaType, ModalState } from '../types/store';

const INITIAL_MODAL: ModalState = {
  isOpen: false,
  areaType: 'region',
  areaKey: null,
  ownerName: '',
  ownerType: '',
  page: 1,
  pageSize: 25,
  search: '',
  severity: '',
  priorityOnly: false,
  requestId: 0,
};

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
      } else if (centralChanged && wasCentral && state.modal.areaType === 'owner') {
        updates.modal = { ...INITIAL_MODAL, requestId: state.modal.requestId + 1 };
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

  openAreaModal: (areaKey: string, areaType: AreaType) =>
    set((s) => ({
      selectedAreaKey: areaKey,
      selectedOwnerKey: null,
      modal: {
        ...INITIAL_MODAL,
        isOpen: true,
        areaType,
        areaKey,
        requestId: s.modal.requestId + 1,
      },
    })),

  openOwnerModal: (ownerName: string, ownerType: OwnerType) =>
    set((s) => ({
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
    })),

  closeModal: () =>
    set((s) => ({
      modal: {
        ...INITIAL_MODAL,
        areaType: s.mapFilter === 'provinsi' ? 'province' : 'region',
        requestId: s.modal.requestId + 1,
      },
    })),

  setModalSearch: (search: string) =>
    set((s) => ({
      modal: { ...s.modal, search, page: 1, requestId: s.modal.requestId + 1 },
    })),

  setModalPage: (page: number) =>
    set((s) => ({
      modal: { ...s.modal, page, requestId: s.modal.requestId + 1 },
    })),

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
}));
