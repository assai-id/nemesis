"use client";

import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { pantau, severityVar } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import { getSeverityDistribution } from "@/lib/dashboard-logic";
import { formatNumber } from "@/lib/format";
import type { Severity } from "@/types/dashboard";

const SEV_ORDER: {
  key: Severity;
  label: string;
  desc: string;
}[] = [
  { key: "absurd", label: "Absurd", desc: "Pola sangat tidak wajar" },
  { key: "high", label: "High", desc: "Prioritas audit tinggi" },
  { key: "med", label: "Medium", desc: "Perlu ditinjau" },
  { key: "low", label: "Low", desc: "Risiko rendah" },
];

export default function SeverityDistribution() {
  const { data, loading } = useDashboard();
  const totals = getSeverityDistribution(data);
  const total =
    totals.low + totals.med + totals.high + totals.absurd || 1;

  return (
    <Box
      sx={{
        bgcolor: pantau.surface,
        border: `1px solid ${pantau.border}`,
        borderRadius: 2,
      }}
    >
      <Box
        sx={{
          px: { xs: 2, md: 2.5 },
          py: 1.75,
          borderBottom: `1px solid ${pantau.border}`,
          display: "flex",
          alignItems: "center",
          gap: 1,
        }}
      >
        <Typography
          sx={{
            fontSize: 14,
            fontWeight: 700,
            color: pantau.text,
            letterSpacing: "-0.01em",
          }}
        >
          Distribusi Severity
        </Typography>
        <Tooltip
          arrow
          placement="top"
          title="Severity mengklasifikasikan paket berdasarkan pola anomali — absurd (sangat tidak wajar), high, medium, low."
        >
          <InfoOutlinedIcon
            sx={{ fontSize: 14, color: pantau.textFaint, cursor: "help" }}
          />
        </Tooltip>
      </Box>

      <Box sx={{ p: { xs: 2, md: 2.5 } }}>
        {loading || !data ? (
          <Skeleton variant="rounded" height={120} />
        ) : (
          <>
            <Box
              sx={{
                display: "flex",
                height: 12,
                borderRadius: 6,
                overflow: "hidden",
                bgcolor: pantau.bgMuted,
                mb: 2,
              }}
            >
              {SEV_ORDER.map((sev) => {
                const count = totals[sev.key];
                const pct = (count / total) * 100;
                if (pct <= 0) return null;
                return (
                  <Tooltip
                    key={sev.key}
                    arrow
                    title={`${sev.label}: ${formatNumber(count)} (${pct.toFixed(1)}%)`}
                  >
                    <Box
                      sx={{
                        width: `${pct}%`,
                        height: "100%",
                        bgcolor: severityVar(sev.key),
                        transition: "opacity .15s",
                        "&:hover": { opacity: 0.85 },
                      }}
                    />
                  </Tooltip>
                );
              })}
            </Box>

            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: { xs: "1fr 1fr", md: "repeat(4, 1fr)" },
                gap: 1.25,
              }}
            >
              {SEV_ORDER.map((sev) => {
                const count = totals[sev.key];
                const pct = (count / total) * 100;
                return (
                  <Box key={sev.key} sx={{ minWidth: 0 }}>
                    <Box
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 0.75,
                        mb: 0.5,
                      }}
                    >
                      <Box
                        sx={{
                          width: 8,
                          height: 8,
                          borderRadius: 0.5,
                          bgcolor: severityVar(sev.key),
                          flexShrink: 0,
                        }}
                      />
                      <Typography
                        sx={{
                          fontSize: 11,
                          fontWeight: 700,
                          color: pantau.textMuted,
                          textTransform: "uppercase",
                          letterSpacing: "0.05em",
                        }}
                      >
                        {sev.label}
                      </Typography>
                    </Box>
                    <Typography
                      className="num"
                      sx={{
                        fontFamily: "JetBrains Mono, monospace",
                        fontSize: 18,
                        fontWeight: 700,
                        color: pantau.text,
                        lineHeight: 1.2,
                      }}
                    >
                      {formatNumber(count)}
                    </Typography>
                    <Typography
                      sx={{
                        fontSize: 11,
                        color: pantau.textSubtle,
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      {pct.toFixed(1)}% · {sev.desc}
                    </Typography>
                  </Box>
                );
              })}
            </Box>
          </>
        )}
      </Box>
    </Box>
  );
}
