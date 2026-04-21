"use client";

import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Skeleton from "@mui/material/Skeleton";
import Typography from "@mui/material/Typography";
import Tooltip from "@mui/material/Tooltip";
import LaunchRoundedIcon from "@mui/icons-material/LaunchRounded";
import { pantau, severityVar } from "@/theme/theme";
import {
  buildInaprocUrl,
  formatCompactCurrency,
  ownerTypeLabel,
  severityLabel,
} from "@/lib/format";
import type { PackageItem } from "@/types/dashboard";

interface Props {
  items: PackageItem[];
  loading?: boolean;
}

export default function PackagesTable({ items, loading }: Props) {
  if (loading) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
        {[0, 1, 2, 3, 4].map((i) => (
          <Skeleton key={i} variant="rounded" height={72} />
        ))}
      </Box>
    );
  }

  if (items.length === 0) {
    return (
      <Box
        sx={{
          py: 4,
          textAlign: "center",
          border: `1px dashed ${pantau.borderStrong}`,
          borderRadius: 2,
        }}
      >
        <Typography sx={{ fontSize: 13, color: pantau.textMuted }}>
          Tidak ada paket pada kombinasi filter ini.
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
      {items.map((pkg) => {
        const sev = pkg.audit.severity;
        const url = buildInaprocUrl(pkg.sourceId);
        return (
          <Box
            key={`${pkg.id}-${pkg.sourceId}`}
            sx={{
              p: 1.5,
              border: `1px solid ${pantau.border}`,
              borderLeft: `3px solid ${severityVar(sev)}`,
              borderRadius: 1.5,
              bgcolor: pantau.surface,
            }}
          >
            <Box
              sx={{
                display: "flex",
                alignItems: "flex-start",
                justifyContent: "space-between",
                gap: 1.5,
                mb: 0.75,
              }}
            >
              <Typography
                sx={{
                  fontSize: 13,
                  fontWeight: 600,
                  color: pantau.text,
                  lineHeight: 1.4,
                  flex: 1,
                  minWidth: 0,
                }}
              >
                {pkg.packageName}
              </Typography>
              <Chip
                size="small"
                label={severityLabel(sev)}
                sx={{
                  height: 22,
                  fontSize: 10,
                  fontWeight: 700,
                  bgcolor: `${severityVar(sev)}22`,
                  color: severityVar(sev),
                  border: `1px solid ${severityVar(sev)}55`,
                  flexShrink: 0,
                }}
              />
            </Box>
            <Typography
              sx={{ fontSize: 11, color: pantau.textSubtle, mb: 0.75 }}
            >
              {pkg.ownerName} · {ownerTypeLabel(pkg.ownerType)}
              {pkg.satker ? ` · ${pkg.satker}` : ""}
            </Typography>
            <Box
              sx={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: 1,
                flexWrap: "wrap",
              }}
            >
              <Typography
                sx={{
                  fontSize: 11,
                  color: pantau.textMuted,
                  lineHeight: 1.4,
                  flex: "1 1 200px",
                  minWidth: 0,
                }}
              >
                {pkg.audit.reason || "—"}
              </Typography>
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1.25,
                  flexShrink: 0,
                }}
              >
                <Typography
                  sx={{
                    fontFamily: "JetBrains Mono, monospace",
                    fontSize: 13,
                    fontWeight: 700,
                    color: pantau.primary,
                    fontVariantNumeric: "tabular-nums",
                  }}
                >
                  Rp {formatCompactCurrency(pkg.budget ?? 0)}
                </Typography>
                {url ? (
                  <Tooltip title="Buka di data.inaproc.id" arrow>
                    <Box
                      component="a"
                      href={url}
                      target="_blank"
                      rel="noopener noreferrer"
                      sx={{
                        display: "inline-flex",
                        alignItems: "center",
                        color: pantau.textMuted,
                        "&:hover": { color: pantau.primary },
                      }}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <LaunchRoundedIcon sx={{ fontSize: 16 }} />
                    </Box>
                  </Tooltip>
                ) : null}
              </Box>
            </Box>
          </Box>
        );
      })}
    </Box>
  );
}
