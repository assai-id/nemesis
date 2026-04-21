"use client";

import Box from "@mui/material/Box";
import { pantau } from "@/theme/theme";
import {
  useDashboard,
  type ActiveTab,
} from "@/components/dashboard/DashboardContext";
import { TAB_DEFS } from "./TabNav";

export default function BottomTabBar() {
  const { activeTab, setActiveTab } = useDashboard();

  return (
    <Box
      component="nav"
      role="tablist"
      sx={{
        position: "fixed",
        insetInline: 0,
        bottom: 0,
        zIndex: 20,
        bgcolor: pantau.surface,
        borderTop: `1px solid ${pantau.border}`,
        display: { xs: "grid", lg: "none" },
        gridTemplateColumns: `repeat(${TAB_DEFS.length}, 1fr)`,
        paddingBottom: "env(safe-area-inset-bottom)",
        boxShadow: pantau.shadowMd,
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
            onClick={() => setActiveTab(tab.id as ActiveTab)}
            sx={{
              all: "unset",
              cursor: "pointer",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              gap: 0.5,
              py: 1,
              color: active ? pantau.primary : pantau.textMuted,
              fontSize: 11,
              fontWeight: active ? 700 : 500,
              transition: "color .15s, background .15s",
              "&:active": { bgcolor: pantau.surfaceHover },
            }}
          >
            <Box
              sx={{
                display: "inline-flex",
                color: active ? pantau.primary : pantau.textMuted,
              }}
            >
              {tab.icon}
            </Box>
            {tab.label}
          </Box>
        );
      })}
    </Box>
  );
}
