"use client";

import { useState } from "react";
import Box from "@mui/material/Box";
import Collapse from "@mui/material/Collapse";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import IconButton from "@mui/material/IconButton";
import useMediaQuery from "@mui/material/useMediaQuery";
import { useTheme } from "@mui/material/styles";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import { getActiveLegend, isProvinceView } from "@/lib/dashboard-logic";
import { formatCompactCurrency } from "@/lib/format";

export default function LegendPanel() {
  const { data, mapFilter } = useDashboard();
  const theme = useTheme();
  const isSmall = useMediaQuery(theme.breakpoints.down("sm"));
  const [open, setOpen] = useState(!isSmall);

  if (!data) return null;

  const legend = getActiveLegend(data, mapFilter);
  const provinceView = isProvinceView(mapFilter);
  const title = provinceView
    ? "Potensi pemborosan per provinsi"
    : "Potensi pemborosan per kab/kota";

  return (
    <Box
      sx={{
        position: "absolute",
        bottom: 12,
        left: 12,
        zIndex: 4,
        maxWidth: { xs: "calc(100vw - 24px)", md: 280 },
      }}
    >
      <Box
        sx={{
          bgcolor: pantau.glass,
          border: `1px solid ${pantau.border}`,
          borderRadius: 2,
          boxShadow: pantau.shadowMd,
          backdropFilter: "blur(8px)",
          overflow: "hidden",
        }}
      >
        <Box
          onClick={() => setOpen((v) => !v)}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              setOpen((v) => !v);
            }
          }}
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            px: 1.5,
            py: 1,
            cursor: "pointer",
            "&:hover": { bgcolor: pantau.surfaceHover },
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
            {title}
          </Typography>
          <IconButton size="small" sx={{ color: pantau.textMuted }}>
            {open ? (
              <ExpandMoreIcon fontSize="small" />
            ) : (
              <ExpandLessIcon fontSize="small" />
            )}
          </IconButton>
        </Box>

        <Collapse in={open} timeout={180}>
          <Box sx={{ px: 1.5, pb: 1.5, pt: 0.25 }}>
            <Stack spacing={0.6}>
              <Stack
                direction="row"
                spacing={1}
                sx={{ alignItems: "center" }}
              >
                <Box
                  sx={{
                    width: 16,
                    height: 10,
                    borderRadius: 0.5,
                    bgcolor: legend.zeroColor || pantau.bgMuted,
                    border: `1px solid ${pantau.border}`,
                    flexShrink: 0,
                  }}
                />
                <Typography
                  sx={{
                    fontSize: 11,
                    color: pantau.textMuted,
                    lineHeight: 1.4,
                  }}
                >
                  Tidak terdeteksi
                </Typography>
              </Stack>
              {legend.ranges?.map((range, idx) => (
                <Stack
                  key={`${range.min}-${range.max}-${idx}`}
                  direction="row"
                  spacing={1}
                  sx={{ alignItems: "center" }}
                >
                  <Box
                    sx={{
                      width: 16,
                      height: 10,
                      borderRadius: 0.5,
                      bgcolor: range.color,
                      flexShrink: 0,
                    }}
                  />
                  <Typography
                    sx={{
                      fontSize: 11,
                      color: pantau.textMuted,
                      fontVariantNumeric: "tabular-nums",
                      lineHeight: 1.4,
                    }}
                  >
                    Rp {formatCompactCurrency(range.min)} – Rp{" "}
                    {formatCompactCurrency(range.max)}
                  </Typography>
                </Stack>
              ))}
            </Stack>
          </Box>
        </Collapse>
      </Box>
    </Box>
  );
}
