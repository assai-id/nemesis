"use client";

import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Typography from "@mui/material/Typography";
import { pantau } from "@/theme/theme";
import { formatCompactCurrency, formatNumber } from "@/lib/format";
import { ownerTypeCount } from "@/lib/dashboard-logic";
import type { RegionArea, ProvinceArea } from "@/types/dashboard";

interface Props {
  rank: number;
  area: RegionArea | ProvinceArea;
  selected?: boolean;
  onClick?: () => void;
}

function formatPct(n: number, total: number): string {
  if (!total) return "0%";
  return `${Math.round((n / total) * 100)}%`;
}

export default function AreaRow({ rank, area, selected, onClick }: Props) {
  const priorityPct = formatPct(
    area.totalPriorityPackages,
    Math.max(area.totalPackages, 1)
  );

  return (
    <Box
      component="tr"
      onClick={onClick}
      role="button"
      tabIndex={0}
      aria-selected={selected}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick?.();
        }
      }}
      sx={{
        cursor: "pointer",
        bgcolor: selected ? pantau.primarySofter : "transparent",
        borderBottom: `1px solid ${pantau.border}`,
        transition: "background .12s",
        "&:hover": {
          bgcolor: selected ? pantau.primarySofter : pantau.surfaceHover,
        },
        "& td": {
          padding: "12px 12px",
          fontSize: 13,
          verticalAlign: "middle",
          color: pantau.text,
        },
      }}
    >
      <Box component="td" sx={{ width: 48 }}>
        <Box
          sx={{
            width: 28,
            height: 28,
            borderRadius: 1,
            display: "grid",
            placeItems: "center",
            bgcolor: rank <= 3 ? pantau.primarySofter : pantau.surfaceAlt,
            color: rank <= 3 ? pantau.primary : pantau.textMuted,
            fontSize: 12,
            fontWeight: 700,
            fontFamily: "JetBrains Mono, monospace",
          }}
        >
          {rank}
        </Box>
      </Box>

      <Box component="td">
        <Typography
          sx={{
            fontSize: 13,
            fontWeight: 600,
            color: pantau.text,
            lineHeight: 1.3,
          }}
        >
          {area.displayName}
        </Typography>
        <Typography
          sx={{
            fontSize: 11,
            color: pantau.textSubtle,
            mt: 0.25,
          }}
        >
          {area.provinceName}
        </Typography>
      </Box>

      <Box component="td" sx={{ width: 110 }}>
        <Chip
          size="small"
          label={area.regionType}
          sx={{
            height: 22,
            fontSize: 11,
            fontWeight: 600,
            bgcolor: pantau.surfaceAlt,
            color: pantau.textMuted,
            border: `1px solid ${pantau.border}`,
          }}
        />
      </Box>

      <Box
        component="td"
        sx={{
          textAlign: "right",
          fontFamily: "JetBrains Mono, monospace",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {formatNumber(area.totalPackages)}
      </Box>
      <Box
        component="td"
        sx={{
          textAlign: "right",
          fontFamily: "JetBrains Mono, monospace",
          fontVariantNumeric: "tabular-nums",
          color: pantau.textMuted,
        }}
      >
        {formatNumber(area.totalPriorityPackages)}
        <Box
          component="span"
          sx={{
            ml: 0.5,
            fontSize: 11,
            color: pantau.textSubtle,
          }}
        >
          ({priorityPct})
        </Box>
      </Box>
      <Box
        component="td"
        sx={{
          textAlign: "right",
          fontFamily: "JetBrains Mono, monospace",
          fontVariantNumeric: "tabular-nums",
          fontWeight: 700,
          color: pantau.primary,
        }}
      >
        Rp {formatCompactCurrency(area.totalPotentialWaste)}
      </Box>
      <Box
        component="td"
        sx={{
          textAlign: "right",
          fontFamily: "JetBrains Mono, monospace",
          fontVariantNumeric: "tabular-nums",
          color: pantau.textMuted,
          display: { xs: "none", xl: "table-cell" },
        }}
      >
        Rp {formatCompactCurrency(area.totalBudget)}
      </Box>
      <Box
        component="td"
        sx={{
          fontSize: 11,
          color: pantau.textSubtle,
          display: { xs: "none", lg: "table-cell" },
          minWidth: 180,
        }}
      >
        K/L {formatNumber(ownerTypeCount(area, "central"))} · Prov{" "}
        {formatNumber(ownerTypeCount(area, "provinsi"))} · Kota/Kab{" "}
        {formatNumber(ownerTypeCount(area, "kabkota"))}
      </Box>
    </Box>
  );
}
