"use client";

import dynamic from "next/dynamic";
import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import AppTopBar from "@/components/shell/AppTopBar";
import TabNav from "@/components/shell/TabNav";
import BottomTabBar from "@/components/shell/BottomTabBar";
import OverviewTab from "@/components/overview/OverviewTab";
import AreaListTab from "@/components/list/AreaListTab";
import AreaDetailDrawer from "@/components/detail/AreaDetailDrawer";

const MapPanel = dynamic(() => import("@/components/map/MapPanel"), {
  ssr: false,
  loading: () => (
    <Box
      sx={{
        width: "100%",
        height: "100%",
        bgcolor: pantau.bgMuted,
      }}
    >
      <Skeleton variant="rectangular" width="100%" height="100%" />
    </Box>
  ),
});

export default function DashboardShell() {
  const { activeTab } = useDashboard();

  return (
    <Box
      sx={{
        height: "100dvh",
        maxHeight: "100dvh",
        bgcolor: pantau.bg,
        color: pantau.text,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      <AppTopBar />
      <Box sx={{ display: { xs: "none", lg: "block" }, flexShrink: 0 }}>
        <TabNav />
      </Box>

      <Box
        component="main"
        sx={{
          flex: 1,
          minHeight: 0,
          paddingBottom: { xs: "calc(64px + env(safe-area-inset-bottom))", lg: 0 },
          overflow: activeTab === "map" ? "hidden" : "auto",
          display: "flex",
          flexDirection: "column",
          position: "relative",
        }}
      >
        {activeTab === "overview" ? <OverviewTab /> : null}
        {activeTab === "map" ? (
          <Box
            sx={{
              position: "relative",
              flex: 1,
              minHeight: 0,
              width: "100%",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <MapPanel />
          </Box>
        ) : null}
        {activeTab === "list" ? <AreaListTab /> : null}
      </Box>

      <BottomTabBar />
      <AreaDetailDrawer />
    </Box>
  );
}
