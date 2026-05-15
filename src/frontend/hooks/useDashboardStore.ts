import { useSyncExternalStore } from 'preact/compat';
import { dashboardStore } from '../store/dashboard.store';
import type { DashboardStore } from '../types/store';

export function useDashboardStore<T>(selector: (s: DashboardStore) => T): T {
  return useSyncExternalStore(
    dashboardStore.subscribe,
    () => selector(dashboardStore.getState())
  );
}
