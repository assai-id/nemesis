import { useDashboardStore } from '../hooks/useDashboardStore';
import { formatCompactCurrency, formatNumber } from '../lib/format';

interface KpiCard {
  label: string;
  value: string;
  sublabel: string;
}

function KpiCard({ label, value, sublabel }: Readonly<KpiCard>) {
  return (
    <div class="kc">
      <div class="kl">{label}</div>
      <div class="kv">{value}</div>
      <div class="ks">{sublabel}</div>
    </div>
  );
}

export function KpiStrip() {
  const status = useDashboardStore((s) => s.bootstrapStatus);
  const summary = useDashboardStore((s) => s.data?.summary);
  const error = useDashboardStore((s) => s.bootstrapError);

  if (status === 'loading' || status === 'idle') {
    return (
      <div class="kpi">
        <KpiCard label="Total Potensi Pemborosan" value="..." sublabel="Menghitung agregat audit" />
        <KpiCard label="Paket Prioritas Audit" value="..." sublabel="Memuat daftar area" />
        <KpiCard label="Total Pagu Teraudit" value="..." sublabel="Menyiapkan peta kab/kota dan provinsi" />
        <KpiCard label="Paket Terpetakan" value="..." sublabel="Memeriksa cakupan lokasi" />
      </div>
    );
  }

  if (status === 'error' || !summary) {
    return (
      <div class="kpi">
        <KpiCard label="Total Potensi Pemborosan" value="-" sublabel="Backend belum siap" />
        <KpiCard label="Paket Prioritas Audit" value="-" sublabel="Periksa ingest hasil analyze" />
        <KpiCard label="Total Pagu Teraudit" value="-" sublabel={error ?? 'Ulangi db:reset bila perlu'} />
        <KpiCard label="Paket Terpetakan" value="-" sublabel="Map belum dapat dibuat" />
      </div>
    );
  }

  const mappedPackages = summary.totalPackages - summary.unmappedPackages;

  return (
    <div class="kpi">
      <KpiCard
        label="Total Potensi Pemborosan"
        value={`Rp ${formatCompactCurrency(summary.totalPotentialWaste)}`}
        sublabel="Nilai nasional raw, tanpa duplikasi multi-lokasi"
      />
      <KpiCard
        label="Paket Prioritas Audit"
        value={formatNumber(summary.totalPriorityPackages)}
        sublabel={`${formatNumber(summary.totalPackages)} paket teraudit`}
      />
      <KpiCard
        label="Total Pagu Teraudit"
        value={`Rp ${formatCompactCurrency(summary.totalBudget)}`}
        sublabel="Akumulasi pagu dari seluruh artifact audit"
      />
      <KpiCard
        label="Paket Terpetakan"
        value={`${formatNumber(mappedPackages)} / ${formatNumber(summary.totalPackages)}`}
        sublabel={`${formatNumber(summary.unmappedPackages)} unmapped | ${formatNumber(summary.multiLocationPackages)} multi-lokasi`}
      />
    </div>
  );
}
