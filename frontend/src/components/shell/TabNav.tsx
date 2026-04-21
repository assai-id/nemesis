"use client";

import Box from "@mui/material/Box";
import InsightsRoundedIcon from "@mui/icons-material/InsightsRounded";
import MapRoundedIcon from "@mui/icons-material/MapRounded";
import ListAltRoundedIcon from "@mui/icons-material/ListAltRounded";
import { pantau } from "@/theme/theme";
import {
  useDashboard,
  type ActiveTab,
} from "@/components/dashboard/DashboardContext";

export interface TabDef {
  id: ActiveTab;
  label: string;
  icon: React.ReactNode;
}

export const TAB_DEFS: TabDef[] = [
  {
    id: "overview",
    label: "Ringkasan",
    icon: <InsightsRoundedIcon fontSize="small" />,
  },
  { id: "map", label: "Peta", icon: <MapRoundedIcon fontSize="small" /> },
  {
    id: "list",
    label: "Daftar",
    icon: <ListAltRoundedIcon fontSize="small" />,
  },
];

export default function TabNav() {
  const { activeTab, setActiveTab } = useDashboard();

  return (
    <Box
      role="tablist"
      sx={{
        bgcolor: pantau.surface,
        borderBottom: `1px solid ${pantau.border}`,
        position: "sticky",
        top: 0,
        zIndex: 15,
      }}
    >
      <Box
        sx={{
          maxWidth: 1400,
          mx: "auto",
          px: { xs: 1, md: 3 },
          display: "flex",
          gap: 0,
        }}
      >
        {TAB_DEFS.map((tab) => {
          const active = tab.id === activeTab;
          return (
            <Box
              key={tab.id}
              component="button"
              role="tab"
              aria-selected={active}
              onClick={() => setActiveTab(tab.id)}
              sx={{
                all: "unset",
                cursor: "pointer",
                display: "inline-flex",
                alignItems: "center",
                gap: 1,
                px: { xs: 2, md: 2.5 },
                py: 1.5,
                color: active ? pantau.text : pantau.textMuted,
                fontSize: 14,
                fontWeight: active ? 700 : 500,
                borderBottom: "3px solid",
                borderColor: active ? pantau.primary : "transparent",
                transition: "color .15s, border-color .15s, background .15s",
                "&:hover": {
                  color: pantau.text,
                  bgcolor: pantau.surfaceHover,
                },
                "&:focus-visible": {
                  bgcolor: pantau.surfaceHover,
                  color: pantau.text,
                },
              }}
            >
              <Box sx={{ display: "inline-flex", color: active ? pantau.primary : "inherit" }}>
                {tab.icon}
              </Box>
              {tab.label}
            </Box>
          );
        })}
      </Box>
    </Box>
  );
}
