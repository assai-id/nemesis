import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import type { TabKey } from '../types/store';

const TABS: { key: TabKey; label: string }[] = [
  { key: 'all', label: 'Semua' },
  { key: 'kabupaten', label: 'Kabupaten' },
  { key: 'kota', label: 'Kota' },
];

export function SidebarHeader() {
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const tab = useDashboardStore((s) => s.tab);
  const isMapVisible = useDashboardStore((s) => s.isMapVisible);

  const isProvince = mapFilter === 'provinsi';
  const isCentral = mapFilter === 'central';
  const tabsDisabled = isProvince || isCentral;

  return (
    <div class="sbh" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
      <div class="sbt" id="tabs">
        {TABS.map((t) => {
          const active = tabsDisabled ? t.key === 'all' : t.key === tab;
          const disabled = tabsDisabled && t.key !== 'all';
          return (
            <button
              key={t.key}
              class={`stb${active ? ' a' : ''}`}
              disabled={disabled}
              onClick={() => dashboardStore.getState().setTab(disabled ? 'all' : t.key)}
            >
              {t.label}
            </button>
          );
        })}
      </div>
      <button
        class={`stb${isMapVisible ? '' : ' a'}`}
        id="toggleMapBtn"
        onClick={() => dashboardStore.getState().toggleMap()}
      >
        &#128506;{isMapVisible ? ' Sembunyikan Peta' : ' Tampilkan Peta'}
      </button>
    </div>
  );
}
