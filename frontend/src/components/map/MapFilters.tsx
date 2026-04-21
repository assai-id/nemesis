"use client";

import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import { FILTERS } from "@/lib/dashboard-logic";

export default function MapFilters() {
  const { mapFilter, setMapFilter } = useDashboard();

  return (
    <Box
      sx={{
        position: "absolute",
        top: 12,
        left: 12,
        right: 12,
        zIndex: 4,
        display: "flex",
        justifyContent: "flex-start",
        pointerEvents: "none",
      }}
    >
      <Box
        sx={{
          pointerEvents: "auto",
          bgcolor: pantau.glass,
          border: `1px solid ${pantau.border}`,
          borderRadius: 2,
          boxShadow: pantau.shadowSm,
          p: 0.5,
          display: "flex",
          flexWrap: "wrap",
          gap: 0.25,
          backdropFilter: "blur(8px)",
          maxWidth: "100%",
          overflow: "hidden",
        }}
        role="radiogroup"
        aria-label="Tampilan peta"
      >
        {FILTERS.map((filter) => {
          const active = filter.key === mapFilter;
          return (
            <Box
              key={filter.key}
              component="button"
              role="radio"
              aria-checked={active}
              onClick={() => setMapFilter(filter.key)}
              sx={{
                all: "unset",
                cursor: "pointer",
                px: { xs: 1.25, md: 1.75 },
                py: 0.75,
                borderRadius: 1.5,
                fontSize: { xs: 11, md: 12 },
                fontWeight: 600,
                color: active ? pantau.onPrimary : pantau.textMuted,
                bgcolor: active ? pantau.primary : "transparent",
                transition: "background .15s, color .15s",
                whiteSpace: "nowrap",
                "&:hover": {
                  bgcolor: active ? pantau.primaryHover : pantau.surfaceHover,
                  color: active ? pantau.onPrimary : pantau.text,
                },
              }}
            >
              <Typography
                component="span"
                sx={{
                  fontSize: "inherit",
                  fontWeight: "inherit",
                  color: "inherit",
                }}
              >
                {filter.label}
              </Typography>
            </Box>
          );
        })}
      </Box>
    </Box>
  );
}
