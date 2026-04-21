"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
  fetchBootstrap,
  fetchOwnerPackages,
  fetchProvincePackages,
  fetchRegionPackages,
} from "@/lib/api";
import type {
  DashboardData,
  MapFilter,
  DetailState,
  OwnerType,
  PackagesPayload,
  Severity,
  SidebarTab,
  SortKey,
} from "@/types/dashboard";

export type ActiveTab = "overview" | "map" | "list";

const VALID_TABS: ActiveTab[] = ["overview", "map", "list"];

const DEFAULT_DETAIL: DetailState = {
  areaType: "region",
  areaKey: null,
  ownerName: "",
  ownerType: "",
  page: 1,
  pageSize: 25,
  search: "",
  severity: "",
  priorityOnly: false,
};

interface DashboardContextValue {
  data: DashboardData | null;
  loading: boolean;
  error: string | null;

  activeTab: ActiveTab;
  setActiveTab: (value: ActiveTab) => void;

  mapFilter: MapFilter;
  tab: SidebarTab;
  sortBy: SortKey;
  search: string;
  selectedAreaKey: string | null;
  selectedOwnerKey: string | null;

  detailOpen: boolean;
  detail: DetailState;
  detailPayload: PackagesPayload | null;
  detailLoading: boolean;
  detailError: string | null;

  setMapFilter: (value: MapFilter) => void;
  setTab: (value: SidebarTab) => void;
  setSearch: (value: string) => void;
  setSortBy: (value: SortKey) => void;

  openAreaDetail: (areaKey: string) => void;
  openOwnerDetail: (ownerName: string, ownerType: OwnerType) => void;
  closeDetail: () => void;

  setDetailSearch: (value: string) => void;
  setDetailOwnerType: (value: OwnerType | "") => void;
  setDetailSeverity: (value: Severity | "") => void;
  setDetailPriorityOnly: (value: boolean) => void;
  changeDetailPage: (page: number) => void;
}

const DashboardContext = createContext<DashboardContextValue | null>(null);

export function useDashboard() {
  const ctx = useContext(DashboardContext);
  if (!ctx) {
    throw new Error("useDashboard must be used within DashboardProvider");
  }
  return ctx;
}

export function DashboardProvider({ children }: { children: ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [activeTab, setActiveTabState] = useState<ActiveTab>(() => {
    const t = searchParams?.get("tab");
    return (VALID_TABS as string[]).includes(t ?? "")
      ? (t as ActiveTab)
      : "overview";
  });

  const [mapFilter, setMapFilterState] = useState<MapFilter>("central");
  const [tab, setTabState] = useState<SidebarTab>("all");
  const [sortBy, setSortBy] = useState<SortKey>("waste");
  const [search, setSearch] = useState("");
  const [selectedAreaKey, setSelectedAreaKey] = useState<string | null>(null);
  const [selectedOwnerKey, setSelectedOwnerKey] = useState<string | null>(null);

  const [detailOpen, setDetailOpen] = useState(false);
  const [detail, setDetail] = useState<DetailState>(DEFAULT_DETAIL);
  const [detailPayload, setDetailPayload] = useState<PackagesPayload | null>(
    null
  );
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  const detailAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    fetchBootstrap(controller.signal)
      .then((payload) => {
        if (controller.signal.aborted) return;
        setData(payload);
      })
      .catch((err) => {
        if (controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, []);

  // URL sync: tab <-> ?tab=...
  const setActiveTab = useCallback(
    (value: ActiveTab) => {
      setActiveTabState(value);
      const params = new URLSearchParams(
        searchParams?.toString() ?? ""
      );
      if (value === "overview") {
        params.delete("tab");
      } else {
        params.set("tab", value);
      }
      const qs = params.toString();
      router.replace(`${pathname}${qs ? `?${qs}` : ""}`, { scroll: false });
    },
    [router, pathname, searchParams]
  );

  const loadDetailPackages = useCallback(
    async (targetDetail: DetailState) => {
      if (targetDetail.areaType === "owner") {
        if (!targetDetail.ownerType || !targetDetail.ownerName) return;
      } else if (!targetDetail.areaKey) {
        return;
      }

      detailAbortRef.current?.abort();
      const controller = new AbortController();
      detailAbortRef.current = controller;

      setDetailLoading(true);
      setDetailError(null);

      const query = {
        page: targetDetail.page,
        pageSize: targetDetail.pageSize,
        search: targetDetail.search || undefined,
        severity: targetDetail.severity || undefined,
        priorityOnly: targetDetail.priorityOnly || undefined,
        ownerType:
          targetDetail.areaType === "region" && targetDetail.ownerType
            ? targetDetail.ownerType
            : targetDetail.areaType === "owner"
              ? targetDetail.ownerType || undefined
              : undefined,
        ownerName:
          targetDetail.areaType === "owner"
            ? targetDetail.ownerName
            : undefined,
      };

      try {
        let payload: PackagesPayload;
        if (targetDetail.areaType === "owner") {
          payload = await fetchOwnerPackages(query, controller.signal);
        } else if (targetDetail.areaType === "province") {
          payload = await fetchProvincePackages(
            targetDetail.areaKey!,
            query,
            controller.signal
          );
        } else {
          payload = await fetchRegionPackages(
            targetDetail.areaKey!,
            query,
            controller.signal
          );
        }
        if (controller.signal.aborted) return;
        setDetailPayload(payload);
      } catch (err) {
        if (controller.signal.aborted) return;
        setDetailError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!controller.signal.aborted) setDetailLoading(false);
      }
    },
    []
  );

  const closeDetail = useCallback(() => {
    detailAbortRef.current?.abort();
    setDetailOpen(false);
    setDetailPayload(null);
    setDetailError(null);
    setDetail(DEFAULT_DETAIL);
    setSelectedAreaKey(null);
    setSelectedOwnerKey(null);
  }, []);

  const openAreaDetail = useCallback(
    (areaKey: string) => {
      const areaType: DetailState["areaType"] =
        mapFilter === "provinsi" ? "province" : "region";
      const next: DetailState = {
        ...DEFAULT_DETAIL,
        areaType,
        areaKey,
      };
      setSelectedAreaKey(areaKey);
      setSelectedOwnerKey(null);
      setDetail(next);
      setDetailPayload(null);
      setDetailOpen(true);
      void loadDetailPackages(next);
    },
    [mapFilter, loadDetailPackages]
  );

  const openOwnerDetail = useCallback(
    (ownerName: string, ownerType: OwnerType) => {
      const next: DetailState = {
        ...DEFAULT_DETAIL,
        areaType: "owner",
        ownerName,
        ownerType,
      };
      setSelectedAreaKey(null);
      setSelectedOwnerKey(`${ownerType}::${ownerName}`);
      setDetail(next);
      setDetailPayload(null);
      setDetailOpen(true);
      void loadDetailPackages(next);
    },
    [loadDetailPackages]
  );

  const setMapFilter = useCallback((value: MapFilter) => {
    setMapFilterState((prev) => {
      const wasProvinceView = prev === "provinsi";
      const nowProvinceView = value === "provinsi";
      const wasCentral = prev === "central";
      const nowCentral = value === "central";

      if (wasProvinceView !== nowProvinceView) {
        setTabState("all");
        setSelectedAreaKey(null);
        setSelectedOwnerKey(null);
        setDetailOpen(false);
        setDetail(DEFAULT_DETAIL);
        setDetailPayload(null);
      } else if (wasCentral !== nowCentral) {
        setTabState("all");
        setSelectedAreaKey(null);
        setSelectedOwnerKey(null);
        if (!nowCentral) {
          setDetailOpen(false);
          setDetail(DEFAULT_DETAIL);
          setDetailPayload(null);
        }
      }

      return value;
    });
  }, []);

  const setTab = useCallback((value: SidebarTab) => {
    setMapFilterState((current) => {
      if (current === "provinsi" || current === "central") {
        setTabState("all");
      } else {
        setTabState(value);
      }
      return current;
    });
  }, []);

  const setDetailSearch = useCallback(
    (value: string) => {
      setDetail((current) => {
        const next = { ...current, search: value, page: 1 };
        void loadDetailPackages(next);
        return next;
      });
    },
    [loadDetailPackages]
  );

  const setDetailOwnerType = useCallback(
    (value: OwnerType | "") => {
      setDetail((current) => {
        if (current.areaType === "province" || current.areaType === "owner") {
          return current;
        }
        const next = { ...current, ownerType: value, page: 1 };
        void loadDetailPackages(next);
        return next;
      });
    },
    [loadDetailPackages]
  );

  const setDetailSeverity = useCallback(
    (value: Severity | "") => {
      setDetail((current) => {
        const next = { ...current, severity: value, page: 1 };
        void loadDetailPackages(next);
        return next;
      });
    },
    [loadDetailPackages]
  );

  const setDetailPriorityOnly = useCallback(
    (value: boolean) => {
      setDetail((current) => {
        const next = { ...current, priorityOnly: value, page: 1 };
        void loadDetailPackages(next);
        return next;
      });
    },
    [loadDetailPackages]
  );

  const changeDetailPage = useCallback(
    (page: number) => {
      setDetail((current) => {
        const next = { ...current, page };
        void loadDetailPackages(next);
        return next;
      });
    },
    [loadDetailPackages]
  );

  useEffect(() => {
    if (!detailOpen) return;
    const handler = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeDetail();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [detailOpen, closeDetail]);

  const value = useMemo<DashboardContextValue>(
    () => ({
      data,
      loading,
      error,
      activeTab,
      setActiveTab,
      mapFilter,
      tab,
      sortBy,
      search,
      selectedAreaKey,
      selectedOwnerKey,
      detailOpen,
      detail,
      detailPayload,
      detailLoading,
      detailError,
      setMapFilter,
      setTab,
      setSearch,
      setSortBy,
      openAreaDetail,
      openOwnerDetail,
      closeDetail,
      setDetailSearch,
      setDetailOwnerType,
      setDetailSeverity,
      setDetailPriorityOnly,
      changeDetailPage,
    }),
    [
      data,
      loading,
      error,
      activeTab,
      setActiveTab,
      mapFilter,
      tab,
      sortBy,
      search,
      selectedAreaKey,
      selectedOwnerKey,
      detailOpen,
      detail,
      detailPayload,
      detailLoading,
      detailError,
      setMapFilter,
      setTab,
      openAreaDetail,
      openOwnerDetail,
      closeDetail,
      setDetailSearch,
      setDetailOwnerType,
      setDetailSeverity,
      setDetailPriorityOnly,
      changeDetailPage,
    ]
  );

  return (
    <DashboardContext.Provider value={value}>
      {children}
    </DashboardContext.Provider>
  );
}
