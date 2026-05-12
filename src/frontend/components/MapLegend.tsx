import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { formatCompactCurrency } from '../lib/format';

export function MapLegend() {
  const isLegendHidden = useDashboardStore((s) => s.isLegendHidden);
  const mapFilter = useDashboardStore((s) => s.mapFilter);
  const data = useDashboardStore((s) => s.data);

  let legend = null;
  if (data) {
    legend = mapFilter === 'provinsi' ? data.provinceView.legend : data.legend;
  }

  const isProvince = mapFilter === 'provinsi';

  if (isLegendHidden) {
    return (
      <div class="mlb" id="legend" style={{ padding: '6px 10px' }}>
        <button
          type="button"
          class="lt"
          style={{ margin: 0, cursor: 'pointer', color: 'var(--t2)', fontSize: '10px', textTransform: 'none', letterSpacing: 'normal', background: 'none', border: 'none', padding: 0 }}
          onClick={() => dashboardStore.getState().toggleLegend()}
        >
          🗒 Tampilkan Legenda
        </button>
      </div>
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
            <div class="lsw" style={{ background: legend.zeroColor || '#243155' }}></div>
            {zeroLabel}
          </div>
          {legend.ranges.map((range) => (
            <div class="li" key={range.key}>
              <div class="lsw" style={{ background: range.color }}></div>
              Rp {formatCompactCurrency(range.min)} &ndash; Rp {formatCompactCurrency(range.max)}
            </div>
          ))}
          <div class="legend-note">{note}</div>
        </>
      )}
    </div>
  );
}
