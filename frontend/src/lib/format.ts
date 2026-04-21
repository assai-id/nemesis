import type { OwnerType, Severity } from "@/types/dashboard";

export function formatCompactCurrency(value: number | null | undefined): string {
  const amount = Number(value) || 0;
  const abs = Math.abs(amount);
  if (abs >= 1e12) {
    return `${(amount / 1e12).toFixed(amount % 1e12 === 0 ? 0 : 1)} T`;
  }
  if (abs >= 1e9) {
    return `${(amount / 1e9).toFixed(amount % 1e9 === 0 ? 0 : 1)} B`;
  }
  if (abs >= 1e6) {
    return `${(amount / 1e6).toFixed(amount % 1e6 === 0 ? 0 : 1)} M`;
  }
  if (abs >= 1e3) {
    return `${(amount / 1e3).toFixed(amount % 1e3 === 0 ? 0 : 1)} K`;
  }
  return amount.toFixed(0);
}

export function formatCurrencyLong(value: number | null | undefined): string {
  const number = Math.round(Number(value) || 0);
  return `Rp ${number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".")}`;
}

export function formatNumber(value: number | null | undefined): string {
  const number = Math.round(Number(value) || 0);
  return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

export function formatDecimal(value: number | null | undefined): string {
  const amount = Number(value) || 0;
  return amount % 1 === 0
    ? formatNumber(amount)
    : amount.toFixed(2).replace(".", ",");
}

export function ownerTypeLabel(value: OwnerType | string | undefined): string {
  if (value === "central") return "Kementerian/Lembaga";
  if (value === "provinsi") return "Pemprov";
  if (value === "kabkota") return "Pemkot";
  if (value === "other") return "Others";
  return "Tidak diketahui";
}

export function severityLabel(severity: Severity | string): string {
  if (severity === "absurd") return "Absurd";
  if (severity === "high") return "High";
  if (severity === "med") return "Medium";
  return "Low";
}

export function severityColor(severity: Severity | string): string {
  if (severity === "absurd") return "#d4a999";
  if (severity === "high") return "#a83c2e";
  if (severity === "med") return "#8b7332";
  return "#7b86a3";
}

export function severityBackground(severity: Severity | string): string {
  if (severity === "absurd") return "rgba(212,169,153,.18)";
  if (severity === "high") return "rgba(168,60,46,.16)";
  if (severity === "med") return "rgba(139,115,50,.16)";
  return "rgba(123,134,163,.16)";
}

export function buildInaprocUrl(sourceId: unknown): string | null {
  if (sourceId === null || sourceId === undefined) return null;
  const normalized = String(sourceId).trim();
  if (!/^\d+$/.test(normalized)) return null;
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return null;
  return `https://data.inaproc.id/rup?kode=${encodeURIComponent(String(parsed))}`;
}
