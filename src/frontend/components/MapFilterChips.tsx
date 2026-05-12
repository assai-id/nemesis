import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import type { MapFilter } from '../types/store';

const FILTERS: { key: MapFilter; label: string }[] = [
  { key: 'central', label: 'Kementerian/Lembaga' },
  { key: 'provinsi', label: 'Pemprov' },
  { key: 'kabkota', label: 'Pemkot' },
  { key: 'other', label: 'Others' },
];

export function MapFilterChips() {
  const mapFilter = useDashboardStore((s) => s.mapFilter);

  return (
    <div class="moc" id="mf">
      {FILTERS.map((f) => (
        <button
          type="button"
          key={f.key}
          class={`fc${f.key === mapFilter ? ' a' : ''}`}
          onClick={() => dashboardStore.getState().setMapFilter(f.key)}
        >
          {f.label}
        </button>
      ))}
    </div>
  );
}
