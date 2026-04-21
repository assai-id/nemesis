"use client";

import { createTheme } from "@mui/material/styles";
import type { ColorMode } from "./ColorModeContext";

/**
 * Pantau — Civic theme tokens
 *
 * Semua warna dibaca dari CSS variables di `globals.css` agar toggle light/dark
 * otomatis tanpa perlu render ulang pohon. Gunakan di prop sx:
 *   bgcolor: pantau.surface
 *   color: pantau.text
 *   border: `1px solid ${pantau.border}`
 */
export const pantau = {
  bg: "var(--bg)",
  bgMuted: "var(--bg-muted)",
  surface: "var(--surface)",
  surfaceAlt: "var(--surface-alt)",
  surfaceHover: "var(--surface-hover)",
  border: "var(--border)",
  borderStrong: "var(--border-strong)",

  text: "var(--text)",
  textMuted: "var(--text-muted)",
  textSubtle: "var(--text-subtle)",
  textFaint: "var(--text-faint)",

  primary: "var(--primary)",
  primaryHover: "var(--primary-hover)",
  primaryActive: "var(--primary-active)",
  primarySoft: "var(--primary-soft)",
  primarySofter: "var(--primary-softer)",
  onPrimary: "var(--on-primary)",

  slate: "var(--slate)",
  slateSoft: "var(--slate-soft)",

  success: "var(--success)",
  successSoft: "var(--success-soft)",
  warning: "var(--warning)",
  warningSoft: "var(--warning-soft)",
  danger: "var(--danger)",
  dangerSoft: "var(--danger-soft)",
  info: "var(--info)",
  infoSoft: "var(--info-soft)",

  sevLow: "var(--sev-low)",
  sevMed: "var(--sev-med)",
  sevHigh: "var(--sev-high)",
  sevAbsurd: "var(--sev-absurd)",

  glass: "var(--glass)",
  glassStrong: "var(--glass-strong)",
  glassBorder: "var(--glass-border)",

  shadowXs: "var(--shadow-xs)",
  shadowSm: "var(--shadow-sm)",
  shadowMd: "var(--shadow-md)",
  shadowLg: "var(--shadow-lg)",

  ring: "var(--ring)",
} as const;

/** Warna hex untuk MapLibre (tidak menerima CSS variables di paint layer) */
export function getMapChromeColors(mode: ColorMode) {
  if (mode === "light") {
    return {
      background: "#f4f4f5",
      areaNeutral: "#e5e7eb",
      areaBorder: "#9ca3af",
      selectedStroke: "#ce1126",
      popupMoney: "#ce1126",
      hoverStroke: "#111827",
    };
  }
  return {
    background: "#0a0a0a",
    areaNeutral: "#1c1c1c",
    areaBorder: "#525252",
    selectedStroke: "#ef4444",
    popupMoney: "#ef4444",
    hoverStroke: "#fafafa",
  };
}

export function severityVar(sev: "low" | "med" | "high" | "absurd"): string {
  switch (sev) {
    case "absurd":
      return pantau.sevAbsurd;
    case "high":
      return pantau.sevHigh;
    case "med":
      return pantau.sevMed;
    default:
      return pantau.sevLow;
  }
}

export function createAppTheme(mode: ColorMode) {
  const light = mode === "light";

  return createTheme({
    cssVariables: true,
    palette: {
      mode,
      primary: {
        main: light ? "#ce1126" : "#ef4444",
        dark: light ? "#a8101f" : "#dc2626",
        light: light ? "#ef4444" : "#fca5a5",
        contrastText: "#ffffff",
      },
      secondary: { main: light ? "#1f2937" : "#e5e7eb" },
      error: { main: light ? "#b91c1c" : "#f87171" },
      warning: { main: light ? "#b45309" : "#fbbf24" },
      info: { main: light ? "#1d4ed8" : "#60a5fa" },
      success: { main: light ? "#047857" : "#34d399" },
      background: {
        default: light ? "#fafafa" : "#0a0a0a",
        paper: light ? "#ffffff" : "#171717",
      },
      divider: light ? "#e5e7eb" : "#262626",
      text: {
        primary: light ? "#111827" : "#fafafa",
        secondary: light ? "#4b5563" : "#d4d4d4",
        disabled: light ? "#9ca3af" : "#737373",
      },
    },
    shape: { borderRadius: 8 },
    typography: {
      fontFamily:
        '"Plus Jakarta Sans", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
      h1: {
        fontSize: "1.75rem",
        fontWeight: 800,
        letterSpacing: "-0.03em",
        lineHeight: 1.2,
      },
      h2: {
        fontSize: "1.25rem",
        fontWeight: 700,
        letterSpacing: "-0.02em",
      },
      h3: { fontSize: "1rem", fontWeight: 700, letterSpacing: "-0.01em" },
      subtitle1: { fontSize: "0.9375rem", fontWeight: 500 },
      subtitle2: {
        fontSize: "0.75rem",
        fontWeight: 700,
        letterSpacing: "0.04em",
        textTransform: "uppercase",
      },
      body1: { fontSize: "0.9375rem", lineHeight: 1.55 },
      body2: { fontSize: "0.875rem", lineHeight: 1.55 },
      caption: { fontSize: "0.75rem", color: "var(--text-subtle)" },
      button: { textTransform: "none", fontWeight: 600 },
    },
    components: {
      MuiCssBaseline: {
        styleOverrides: {
          "*, *::before, *::after": {
            boxSizing: "border-box",
          },
        },
      },
      MuiPaper: {
        styleOverrides: {
          root: {
            backgroundImage: "none",
            backgroundColor: "var(--surface)",
            boxShadow: "var(--shadow-sm)",
          },
        },
      },
      MuiButton: {
        defaultProps: { disableElevation: true },
        styleOverrides: {
          root: {
            borderRadius: 8,
            fontWeight: 600,
            paddingInline: 14,
          },
          containedPrimary: {
            "&:hover": { backgroundColor: "var(--primary-hover)" },
          },
          outlined: {
            borderColor: "var(--border-strong)",
            "&:hover": {
              borderColor: "var(--primary)",
              backgroundColor: "var(--primary-softer)",
            },
          },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: { borderRadius: 999, fontWeight: 600, fontSize: 12 },
        },
      },
      MuiAlert: {
        styleOverrides: { root: { borderRadius: 8 } },
      },
      MuiTooltip: {
        styleOverrides: {
          tooltip: {
            backgroundColor: light ? "#111827" : "#fafafa",
            color: light ? "#ffffff" : "#111827",
            fontSize: 12,
            fontWeight: 500,
            padding: "6px 10px",
            borderRadius: 6,
          },
          arrow: {
            color: light ? "#111827" : "#fafafa",
          },
        },
      },
      MuiTabs: {
        styleOverrides: {
          indicator: { backgroundColor: "var(--primary)", height: 3 },
        },
      },
      MuiTab: {
        styleOverrides: {
          root: {
            textTransform: "none",
            fontWeight: 600,
            fontSize: 14,
            minHeight: 48,
            color: "var(--text-muted)",
            "&.Mui-selected": { color: "var(--text)" },
          },
        },
      },
    },
  });
}

/** Alias kompatibilitas — komponen lama dihapus; simpan untuk migrasi singkat. */
export const audit = pantau;
export const palette = pantau;
export const glass = {
  bg: pantau.glass,
  bgHover: pantau.glassStrong,
  border: pantau.glassBorder,
};
