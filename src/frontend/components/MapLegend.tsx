import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { formatCompactCurrency } from '../lib/format';

// Composite a fill swatch dengan opacity 0.52 di atas warna basemap yang
// disimulasikan, supaya swatch tampak persis seperti area peta yang
// di-render dengan `fill-opacity: 0.52` di MapLibre.
//
// Basemap reference colors (dominant land tint setelah anti-alias):
//   - dark-matter-nolabels  → #0d1424 (deep navy/black)
//   - positron-nolabels     → #fafaf6 (near-white cream)
function swatchBg(color: string, theme: 'light' | 'dark'): string {
  const base = theme === 'dark' ? '#0d1424' : '#fafaf6';
  return `color-mix(in srgb, ${color} 52%, ${base})`;
}

export function MapLegend() {
  const isLegendHidden = useDashboardStore((s) => s.isLegendHidden);
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const data = useDashboardStore((s) => s.data);
  const theme = useDashboardStore((s) => s.theme);

  let legend = null;
  if (data) {
    legend = mapFilter === 'provinsi' ? data.provinceView.legend : data.legend;
  }

  const isProvince = mapFilter === 'provinsi';

  if (isLegendHidden) {
    return (
      <button
        type="button"
        class="map-toggle map-toggle-legend"
        id="legend"
        aria-label="Tampilkan legenda"
        onClick={() => dashboardStore.getState().toggleLegend()}
      >
        <span aria-hidden="true">▤</span> Tampilkan legenda
      </button>
    );
  }

  const title = isProvince
    ? 'Potensi Pemborosan Paket Pemprov per Provinsi'
    : 'Potensi Pemborosan per Kab/Kota';

  const zeroLabel = isProvince
    ? 'Tidak ada paket pemprov terdeteksi'
    : 'Tidak ada potensi terdeteksi';

  const note = isProvince
    ? 'Agregasi provinsi mendeduplikasi paket multi-kab/kota di provinsi yang sama.'
    : 'Map region menghitung penuh paket multi-lokasi, sehingga agregat region bisa lebih besar dari KPI nasional.';

  return (
    <div class="mlb" id="legend">
      <div class="lt" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>{title}</span>
        <button
          onClick={() => dashboardStore.getState().toggleLegend()}
          style={{ background: 'none', border: 'none', color: 'var(--t3)', cursor: 'pointer', marginLeft: '8px', fontSize: '12px', padding: '2px' }}
          title="Sembunyikan Legenda"
        >
          ✕
        </button>
      </div>
      {legend && (
        <>
          <div class="li">
            <div class="lsw" style={{ background: swatchBg(legend.zeroColor || '#4a5982', theme) }}></div>
            {zeroLabel}
          </div>
          {legend.ranges.map((range) => (
            <div class="li" key={range.key}>
              <div class="lsw" style={{ background: swatchBg(range.color, theme) }}></div>
              Rp {formatCompactCurrency(range.min)} &ndash; Rp {formatCompactCurrency(range.max)}
            </div>
          ))}
          <div class="legend-note">{note}</div>
        </>
      )}
    </div>
  );
}
