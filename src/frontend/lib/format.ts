import type { RegionRow, ProvinceRow, Legend } from '../types/api';

const HTML_ESCAPE_MAP: Record<string, string> = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};
export function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => HTML_ESCAPE_MAP[c]);
}

export function formatCompactCurrency(value: number | null | undefined): string {
  const amount = Number(value) || 0;
  const abs = Math.abs(amount);
  if (abs >= 1e12) return `${(amount / 1e12).toFixed(amount % 1e12 === 0 ? 0 : 1)} T`;
  if (abs >= 1e9) return `${(amount / 1e9).toFixed(amount % 1e9 === 0 ? 0 : 1)} B`;
  if (abs >= 1e6) return `${(amount / 1e6).toFixed(amount % 1e6 === 0 ? 0 : 1)} M`;
  if (abs >= 1e3) return `${(amount / 1e3).toFixed(amount % 1e3 === 0 ? 0 : 1)} K`;
  return `${amount.toFixed(0)}`;
}

export function formatCurrencyLong(value: number | null | undefined): string {
  const number = Math.round(Number(value) || 0);
  return `Rp ${number.toString().replaceAll(/\B(?=(\d{3})+(?!\d))/g, '.')}`;
}

export function formatNumber(value: number | null | undefined): string {
  const number = Math.round(Number(value) || 0);
  return number.toString().replaceAll(/\B(?=(\d{3})+(?!\d))/g, '.');
}

export function formatDecimal(value: number | null | undefined): string {
  const amount = Number(value) || 0;
  return amount % 1 === 0 ? formatNumber(amount) : amount.toFixed(2).replace('.', ',');
}

export function ownerTypeLabel(value: string | null | undefined): string {
  if (value === 'central') return 'Kementerian/Lembaga';
  if (value === 'provinsi') return 'Pemprov';
  if (value === 'kabkota') return 'Pemkot';
  if (value === 'other') return 'Others';
  return 'Tidak diketahui';
}

export function severityColor(severity: string): string {
  if (severity === 'absurd') return 'var(--rose)';
  if (severity === 'high') return 'var(--brick)';
  if (severity === 'med') return 'var(--olive)';
  return 'var(--steel)';
}

export function severityBgColor(severity: string): string {
  if (severity === 'absurd') return 'rgba(212,169,153,.18)';
  if (severity === 'high') return 'rgba(168,60,46,.16)';
  if (severity === 'med') return 'rgba(139,115,50,.16)';
  return 'rgba(123,134,163,.16)';
}

export function severityLabel(severity: string): string {
  if (severity === 'absurd') return 'Absurd';
  if (severity === 'high') return 'High';
  if (severity === 'med') return 'Medium';
  return 'Low';
}

export function normalizeSourceId(sourceId: string | number | null | undefined): string | null {
  if (sourceId === null || sourceId === undefined) return null;
  const normalized = String(sourceId).trim();
  if (!/^\d+$/.test(normalized)) return null;
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return null;
  return String(parsed);
}

export function buildInaprocUrl(sourceId: string | number | null | undefined): string | null {
  const kode = normalizeSourceId(sourceId);
  return kode ? `https://data.inaproc.id/rup?kode=${encodeURIComponent(kode)}` : null;
}

export function getLegendColor(value: number | null | undefined, legend: Legend): string {
  if (!legend) return '#243155';
  if (!value || value <= 0) return legend.zeroColor || '#243155';
  const range = (legend.ranges || []).find((item) => value >= item.min && value <= item.max);
  return range ? range.color : legend.ranges.at(-1)?.color ?? '#a83c2e';
}

export function areaBadgeLabel(area: RegionRow | ProvinceRow): string {
  if (area.regionType === 'Provinsi') return 'Prov.';
  if (area.regionType === 'Kota') return 'Kota';
  return 'Kab.';
}

export function areaBadgeClass(area: RegionRow | ProvinceRow): string {
  return area.regionType === 'Kota' ? 'bk' : 'bp';
}
