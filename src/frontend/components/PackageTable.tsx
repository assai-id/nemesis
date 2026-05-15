import { useState, useEffect, useRef } from 'preact/hooks';
import {
  buildInaprocUrl,
  ownerTypeLabel,
  severityClass,
  severityLabel,
  formatCurrencyLong,
} from '../lib/format';
import type { PackageRow } from '../types/api';

interface PackageProps {
  items: PackageRow[];
}

interface CopyIdButtonProps {
  id: string;
}

function CopyIdButton({ id }: Readonly<CopyIdButtonProps>) {
  const [flash, setFlash] = useState<string | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
  }, []);

  function copy(e: MouseEvent) {
    e.stopPropagation();
    if (!id) return;
    const finish = (label: string) => {
      setFlash(label);
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      timeoutRef.current = setTimeout(() => setFlash(null), 1400);
    };
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(id).then(
        () => finish('✓'),
        () => finish('!'),
      );
      return;
    }
    const ta = document.createElement('textarea');
    ta.value = id;
    ta.setAttribute('readonly', '');
    ta.style.position = 'absolute';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy');
      finish('✓');
    } catch {
      finish('!');
    }
    document.body.removeChild(ta);
  }

  return (
    <button
      type="button"
      class={`copy-id-btn${flash ? ' copied' : ''}`}
      onClick={copy}
      aria-label="Salin ID paket"
      title="Salin ID"
    >
      {flash ?? '⧉'}
    </button>
  );
}

function openInaproc(sourceId: string | number | null | undefined) {
  const url = buildInaprocUrl(sourceId);
  if (!url) return;
  window.open(url, '_blank', 'noopener,noreferrer');
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
            const idText = String(item.sourceId || item.id);
            const onClick = url ? () => openInaproc(item.sourceId) : undefined;
            const onKeyDown = url
              ? (e: KeyboardEvent) => {
                  if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar') {
                    e.preventDefault();
                    openInaproc(item.sourceId);
                  }
                }
              : undefined;
            return (
              <tr
                key={item.id}
                class={url ? 'package-row-link' : undefined}
                tabIndex={url ? 0 : undefined}
                role={url ? 'link' : undefined}
                aria-label={url ? `Buka ${item.packageName} di Inaproc` : undefined}
                onClick={onClick}
                onKeyDown={onKeyDown}
              >
                <td class="mono">
                  <span class="id-cell">
                    {idText}
                    <CopyIdButton id={idText} />
                  </span>
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
                <td class="mono col-pagu">
                  {item.budget === null ? '-' : formatCurrencyLong(item.budget)}
                </td>
                <td>
                  <span class={`sev-b ${severityClass(item.audit.severity)}`}>
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
