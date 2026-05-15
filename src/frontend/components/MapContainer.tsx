import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { MapView } from './MapView';
import { MapLegend } from './MapLegend';

export function MapContainer() {
  const isMapVisible = useDashboardStore((s) => s.isMapVisible);

  return (
    <div class="mc" style={isMapVisible ? undefined : { display: 'none' }}>
      <MapView />
      <MapLegend />
      <button
        type="button"
        class="map-toggle map-toggle-hide"
        id="toggleMapBtn"
        aria-pressed={!isMapVisible}
        title="Sembunyikan peta untuk fokus ke daftar wilayah"
        onClick={() => dashboardStore.getState().toggleMap()}
      >
        <span aria-hidden="true">⇲</span> Sembunyikan peta
      </button>
    </div>
  );
}
