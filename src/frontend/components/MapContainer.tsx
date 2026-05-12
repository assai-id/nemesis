import { useDashboardStore } from '../hooks/useDashboardStore';
import { MapView } from './MapView';
import { MapFilterChips } from './MapFilterChips';
import { MapLegend } from './MapLegend';

export function MapContainer() {
  const isMapVisible = useDashboardStore((s) => s.isMapVisible);

  return (
    <div class="mc" style={{ display: isMapVisible ? '' : 'none' }}>
      <MapView />
      <MapFilterChips />
      <MapLegend />
    </div>
  );
}
