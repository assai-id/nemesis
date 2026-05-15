import { useDashboardStore } from '../hooks/useDashboardStore';
import { formatCompactCurrency, formatNumber } from '../lib/format';

interface KpiCardProps {
  label: string;
  value: string;
  sublabel: string;
}

function KpiCard({ label, value, sublabel }: Readonly<KpiCardProps>) {
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
      <div class="kpi" id="kpi" aria-label="Bukti ringkas nasional">
        <KpiCard label="Potensi Pemborosan Nasional" value="..." sublabel="Menghitung agregat audit" />
        <KpiCard label="Paket Teraudit" value="..." sublabel="Memuat daftar area" />
        <KpiCard label="Pagu" value="..." sublabel="Menyiapkan agregat anggaran" />
      </div>
    );
  }

  if (status === 'error' || !summary) {
    return (
      <div class="kpi" id="kpi" aria-label="Bukti ringkas nasional">
        <KpiCard label="Potensi Pemborosan Nasional" value="-" sublabel="Backend belum siap" />
        <KpiCard label="Paket Teraudit" value="-" sublabel="Periksa ingest hasil analyze" />
        <KpiCard label="Pagu" value="-" sublabel={error ?? 'Ulangi db:reset bila perlu'} />
      </div>
    );
  }

  return (
    <div class="kpi" id="kpi" aria-label="Bukti ringkas nasional">
      <KpiCard
        label="Potensi Pemborosan Nasional"
        value={`Rp ${formatCompactCurrency(summary.totalPotentialWaste)}`}
        sublabel={`${formatNumber(summary.totalPriorityPackages)} paket prioritas`}
      />
      <KpiCard
        label="Paket Teraudit"
        value={formatNumber(summary.totalPackages)}
        sublabel={`${formatNumber(summary.unmappedPackages)} tidak terpetakan`}
      />
      <KpiCard
        label="Pagu"
        value={`Rp ${formatCompactCurrency(summary.totalBudget)}`}
        sublabel="Akumulasi seluruh artifact"
      />
    </div>
  );
}
