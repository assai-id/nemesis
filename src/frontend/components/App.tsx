import { useEffect } from 'preact/hooks';
import { dashboardStore } from '../store/dashboard.store';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { fetchBootstrap, normalizeDashboardData } from '../lib/api';
import { Header } from './Header';
import { MapContainer } from './MapContainer';
import { Sidebar } from './Sidebar';
import { Modal as CaseFile } from './Modal';
import { MethodsOverlay } from './MethodsOverlay';
import { AiDisclosureModal } from './AiDisclosureModal';

export function App() {
  const viewMode = useDashboardStore((s) => s.viewMode);
  const bootstrapStatus = useDashboardStore((s) => s.bootstrapStatus);
  const isMapVisible = useDashboardStore((s) => s.isMapVisible);

  useEffect(() => {
    const store = dashboardStore.getState();
    store.setBootstrapLoading();
    const initialTheme = store.theme;
    fetchBootstrap()
      .then((raw) => {
        store.setBootstrapReady(normalizeDashboardData(raw, initialTheme));
        // On phone-sized viewports, start with the map legend collapsed so it
        // doesn't cover the choropleth on first paint.
        if (typeof window !== 'undefined' && window.innerWidth <= 640) {
          const current = dashboardStore.getState();
          if (!current.isLegendHidden) current.toggleLegend();
        }
      })
      .catch((err: unknown) =>
        store.setBootstrapError(err instanceof Error ? err.message : String(err))
      );
  }, []);

  useEffect(() => {
    if (bootstrapStatus === 'ready') {
      dashboardStore.getState().syncFromPath();
    }
  }, [bootstrapStatus]);

  useEffect(() => {
    const onPop = () => dashboardStore.getState().syncFromPath();
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const state = dashboardStore.getState();
      if (e.key === 'Escape') {
        if (state.isMethodsOpen) return; // MethodsOverlay handles its own Escape
        if (state.viewMode === 'casefile') state.navigateHome();
        return;
      }
      // Pagination keyboard shortcuts when on case file and not typing
      if (state.viewMode !== 'casefile') return;
      const target = e.target;
      if (
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLSelectElement
      ) {
        return;
      }
      const pagination = state.modal;
      if (e.key === 'ArrowLeft' && pagination.page > 1) {
        e.preventDefault();
        state.setModalPage(pagination.page - 1);
      } else if (
        e.key === 'ArrowRight' &&
        pagination.totalPages > 0 &&
        pagination.page < pagination.totalPages
      ) {
        e.preventDefault();
        state.setModalPage(pagination.page + 1);
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, []);

  useEffect(() => {
    if (typeof document === 'undefined') return;
    document.body.classList.toggle('map-hidden', !isMapVisible);
    if (isMapVisible) {
      setTimeout(() => window.dispatchEvent(new Event('resize')), 50);
    }
  }, [isMapVisible]);

  useEffect(() => {
    if (typeof window !== 'undefined') window.scrollTo(0, 0);
  }, [viewMode]);

  return (
    <div id="preact-wrapper" data-view={viewMode}>
      <Header />
      <main class="ml view-dashboard" aria-label="Investigasi peta dan daftar wilayah">
        <MapContainer />
        <Sidebar />
      </main>
      <CaseFile />
      <MethodsOverlay />
      <AiDisclosureModal />
    </div>
  );
}
