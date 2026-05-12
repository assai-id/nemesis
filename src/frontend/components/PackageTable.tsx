import { buildInaprocUrl, ownerTypeLabel, severityColor, severityBgColor, severityLabel, formatCurrencyLong } from '../lib/format';
import type { PackageRow } from '../types/api';

interface PackageProps {
  items: PackageRow[];
}


export function PackageTable({ items }: Readonly<PackageProps>) {
  return (
    <table class="rtbl">
      <thead>
        <tr>
          <th>ID</th>
          <th>Nama Paket</th>
          <th>Pemilik</th>
          <th>Satker / Lokasi</th>
          <th>Pagu</th>
          <th>Severity</th>
          <th>Alasan</th>
        </tr>
      </thead>
      <tbody>
        {items.length === 0 ? (
          <tr>
            <td colSpan={7} class="table-empty">
              Tidak ada paket untuk filter saat ini.
            </td>
          </tr>
        ) : (
          items.map((item) => {
            const url = buildInaprocUrl(item.sourceId);
            return (
              <tr key={item.id} class={url ? 'package-row-link' : undefined}>
                <td class="mono">
                  {url ? (
                    <a href={url} target="_blank" rel="noopener noreferrer" aria-label={`Buka ${item.packageName} di Inaproc`}>
                      {item.sourceId ?? String(item.id)}
                    </a>
                  ) : (item.sourceId ?? String(item.id))}
                </td>
                <td class="pkg">{item.packageName}</td>
                <td>
                  <div class="tbl-owner">{item.ownerName}</div>
                  <div class="tbl-sub">{ownerTypeLabel(item.ownerType)}</div>
                </td>
                <td>
                  <div class="tbl-owner">{item.satker ?? '-'}</div>
                  <div class="tbl-sub">{item.locationRaw ?? '-'}</div>
                </td>
                <td class="mono" style={{ color: 'var(--sage)' }}>
                  {item.budget === null ? '-' : formatCurrencyLong(item.budget)}
                </td>
                <td>
                  <span
                    class="sev-b"
                    style={{
                      background: severityBgColor(item.audit.severity),
                      color: severityColor(item.audit.severity),
                    }}
                  >
                    {severityLabel(item.audit.severity)}
                  </span>
                </td>
                <td class="reason">{item.audit.reason ?? '-'}</td>
              </tr>
            );
          })
        )}
      </tbody>
    </table>
  );
}
