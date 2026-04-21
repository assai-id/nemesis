"use client";

import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import {
  formatCompactCurrency,
  formatCurrencyLong,
  formatNumber,
} from "@/lib/format";
import type { Summary } from "@/types/dashboard";

interface KpiCard {
  key: string;
  label: string;
  valueNode: React.ReactNode;
  sublabel: string;
  tooltip: string;
  accent?: boolean;
}

function buildCards(summary: Summary): KpiCard[] {
  const mappingPct =
    summary.totalPackages > 0
      ? Math.round(
          ((summary.totalPackages - summary.unmappedPackages) /
            summary.totalPackages) *
            100
        )
      : 0;

  return [
    {
      key: "waste",
      label: "Potensi Pemborosan",
      valueNode: (
        <>
          <Box
            component="span"
            sx={{ fontSize: { xs: 12, md: 13 }, fontWeight: 600, mr: 0.5 }}
          >
            Rp
          </Box>
          {formatCompactCurrency(summary.totalPotentialWaste)}
        </>
      ),
      sublabel: formatCurrencyLong(summary.totalPotentialWaste),
      tooltip:
        "Estimasi pemborosan dihitung dari heuristik audit (jenis belanja, nilai pagu, dan pola paket). Bukan temuan audit final.",
      accent: true,
    },
    {
      key: "priority",
      label: "Paket Prioritas Audit",
      valueNode: formatNumber(summary.totalPriorityPackages),
      sublabel: `dari ${formatNumber(summary.totalPackages)} paket`,
      tooltip:
        "Paket dengan severity High atau Absurd — prioritas untuk audit mendalam.",
    },
    {
      key: "budget",
      label: "Total Pagu",
      valueNode: (
        <>
          <Box
            component="span"
            sx={{ fontSize: { xs: 12, md: 13 }, fontWeight: 600, mr: 0.5 }}
          >
            Rp
          </Box>
          {formatCompactCurrency(summary.totalBudget)}
        </>
      ),
      sublabel: formatCurrencyLong(summary.totalBudget),
      tooltip:
        "Nilai pagu nasional dari seluruh paket yang teragregasi.",
    },
    {
      key: "mapped",
      label: "Cakupan Peta",
      valueNode: `${mappingPct}%`,
      sublabel: `${formatNumber(
        summary.totalPackages - summary.unmappedPackages
      )} paket terpetakan · ${formatNumber(summary.unmappedPackages)} belum`,
      tooltip:
        "Persentase paket yang berhasil dipetakan ke kab/kota/provinsi berdasarkan kolom lokasi.",
    },
  ];
}

export default function HeroKpi() {
  const { data, loading } = useDashboard();

  const cards: KpiCard[] = data ? buildCards(data.summary) : [];

  return (
    <Box
      sx={{
        display: "grid",
        gap: { xs: 1.5, md: 2 },
        gridTemplateColumns: {
          xs: "1fr 1fr",
          md: "repeat(4, 1fr)",
        },
      }}
    >
      {(loading || !data
        ? [0, 1, 2, 3].map((i) => ({ key: `s-${i}`, skeleton: true } as const))
        : cards.map((c) => ({ ...c, skeleton: false } as const))
      ).map((card) => {
        if (card.skeleton) {
          return (
            <Box
              key={card.key}
              sx={{
                p: { xs: 2, md: 2.5 },
                bgcolor: pantau.surface,
                border: `1px solid ${pantau.border}`,
                borderRadius: 2,
              }}
            >
              <Skeleton variant="text" width="50%" height={14} />
              <Skeleton variant="text" width="70%" height={38} sx={{ mt: 1 }} />
              <Skeleton variant="text" width="90%" height={14} />
            </Box>
          );
        }
        return (
          <Box
            key={card.key}
            sx={{
              p: { xs: 2, md: 2.5 },
              bgcolor: pantau.surface,
              border: `1px solid ${pantau.border}`,
              borderLeft: card.accent
                ? `3px solid ${pantau.primary}`
                : `1px solid ${pantau.border}`,
              borderRadius: 2,
              transition: "border-color .15s, transform .15s",
              "&:hover": { borderColor: pantau.borderStrong },
            }}
          >
            <Box
              sx={{
                display: "flex",
                alignItems: "center",
                gap: 0.5,
                mb: 1,
              }}
            >
              <Typography
                sx={{
                  fontSize: 11,
                  fontWeight: 700,
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                  color: pantau.textMuted,
                }}
              >
                {card.label}
              </Typography>
              <Tooltip title={card.tooltip} arrow placement="top">
                <InfoOutlinedIcon
                  sx={{ fontSize: 13, color: pantau.textFaint, cursor: "help" }}
                />
              </Tooltip>
            </Box>
            <Typography
              className="num"
              sx={{
                fontFamily: "JetBrains Mono, monospace",
                fontSize: { xs: 22, md: 28 },
                fontWeight: 700,
                color: card.accent ? pantau.primary : pantau.text,
                letterSpacing: "-0.02em",
                lineHeight: 1.1,
                mb: 0.5,
              }}
            >
              {card.valueNode}
            </Typography>
            <Typography
              sx={{
                fontSize: 11,
                color: pantau.textSubtle,
                fontVariantNumeric: "tabular-nums",
                lineHeight: 1.4,
                overflow: "hidden",
                textOverflow: "ellipsis",
                display: "-webkit-box",
                WebkitLineClamp: 2,
                WebkitBoxOrient: "vertical",
              }}
            >
              {card.sublabel}
            </Typography>
          </Box>
        );
      })}
    </Box>
  );
}
