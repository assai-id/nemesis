"use client";

import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";
import MapRoundedIcon from "@mui/icons-material/MapRounded";
import ListAltRoundedIcon from "@mui/icons-material/ListAltRounded";
import ArrowForwardRoundedIcon from "@mui/icons-material/ArrowForwardRounded";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import { getTopAreas, getTopOwners } from "@/lib/dashboard-logic";
import { formatNumber } from "@/lib/format";
import HeroKpi from "./HeroKpi";
import TopRanking, { type RankingItem } from "./TopRanking";
import SeverityDistribution from "./SeverityDistribution";
import DataAttribution from "@/components/shared/DataAttribution";

export default function OverviewTab() {
  const {
    data,
    loading,
    error,
    setActiveTab,
    openAreaDetail,
    openOwnerDetail,
  } = useDashboard();

  const topAreas: RankingItem[] = getTopAreas(data, 5).map((area) => ({
    id: area.regionKey,
    label: area.displayName,
    sub: area.provinceName,
    waste: area.totalPotentialWaste,
    priority: area.totalPriorityPackages,
    onClick: () => {
      openAreaDetail(area.regionKey);
    },
  }));

  const topOwners: RankingItem[] = getTopOwners(data, 5).map((owner) => ({
    id: owner.ownerName,
    label: owner.ownerName,
    sub: `${formatNumber(owner.totalPackages)} paket`,
    waste: owner.totalPotentialWaste,
    priority: owner.totalPriorityPackages,
    onClick: () => {
      openOwnerDetail(owner.ownerName, owner.ownerType);
    },
  }));

  return (
    <Box
      sx={{
        maxWidth: 1400,
        mx: "auto",
        px: { xs: 2, md: 3 },
        py: { xs: 2, md: 3 },
      }}
    >
      {/* Page title */}
      <Box sx={{ mb: { xs: 2, md: 3 } }}>
        <Typography
          component="h1"
          sx={{
            fontSize: { xs: 22, md: 26 },
            fontWeight: 800,
            color: pantau.text,
            letterSpacing: "-0.02em",
            mb: 0.5,
          }}
        >
          Ringkasan Nasional
        </Typography>
        <Typography
          sx={{
            fontSize: 14,
            color: pantau.textMuted,
            lineHeight: 1.55,
            maxWidth: 720,
          }}
        >
          Potret cepat pengadaan barang/jasa nasional beserta wilayah &
          kementerian yang paling menonjol. Pilih tab{" "}
          <Box component="span" sx={{ fontWeight: 700, color: pantau.text }}>
            Peta
          </Box>{" "}
          untuk melihat sebaran geografis atau{" "}
          <Box component="span" sx={{ fontWeight: 700, color: pantau.text }}>
            Daftar
          </Box>{" "}
          untuk membandingkan dalam bentuk tabel.
        </Typography>
      </Box>

      {error ? (
        <Alert severity="error" sx={{ mb: 2 }}>
          Gagal memuat data: {error}
        </Alert>
      ) : null}

      <HeroKpi />

      {/* Rankings grid */}
      <Box
        sx={{
          mt: { xs: 2, md: 3 },
          display: "grid",
          gap: { xs: 2, md: 2.5 },
          gridTemplateColumns: { xs: "1fr", md: "1fr 1fr" },
        }}
      >
        <TopRanking
          title="Top 5 Wilayah — Potensi Pemborosan Tertinggi"
          items={topAreas}
          loading={loading}
          emptyLabel="Belum ada wilayah dengan potensi pemborosan"
          footerActionLabel="Lihat semua wilayah"
          onFooterAction={() => setActiveTab("list")}
        />
        <TopRanking
          title="Top 5 Kementerian/Lembaga — Potensi Pemborosan"
          items={topOwners}
          loading={loading}
          emptyLabel="Belum ada K/L dengan potensi pemborosan"
          footerActionLabel="Lihat semua K/L"
          onFooterAction={() => setActiveTab("list")}
        />
      </Box>

      <Box sx={{ mt: { xs: 2, md: 2.5 } }}>
        <SeverityDistribution />
      </Box>

      {/* Quick actions */}
      <Stack
        direction={{ xs: "column", sm: "row" }}
        spacing={1.5}
        sx={{ mt: { xs: 2, md: 3 } }}
      >
        <Button
          variant="contained"
          startIcon={<MapRoundedIcon />}
          endIcon={<ArrowForwardRoundedIcon fontSize="small" />}
          onClick={() => setActiveTab("map")}
          sx={{
            bgcolor: pantau.primary,
            color: pantau.onPrimary,
            "&:hover": { bgcolor: pantau.primaryHover },
          }}
        >
          Lihat di Peta
        </Button>
        <Button
          variant="outlined"
          startIcon={<ListAltRoundedIcon />}
          endIcon={<ArrowForwardRoundedIcon fontSize="small" />}
          onClick={() => setActiveTab("list")}
          sx={{
            borderColor: pantau.border,
            color: pantau.text,
            "&:hover": {
              borderColor: pantau.primary,
              bgcolor: pantau.primarySofter,
            },
          }}
        >
          Lihat Daftar Lengkap
        </Button>
      </Stack>

      <DataAttribution />
    </Box>
  );
}
