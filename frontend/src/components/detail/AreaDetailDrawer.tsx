"use client";

import { useMemo, useState } from "react";
import Drawer from "@mui/material/Drawer";
import SwipeableDrawer from "@mui/material/SwipeableDrawer";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import MenuItem from "@mui/material/MenuItem";
import Pagination from "@mui/material/Pagination";
import Select from "@mui/material/Select";
import Stack from "@mui/material/Stack";
import Switch from "@mui/material/Switch";
import TextField from "@mui/material/TextField";
import InputAdornment from "@mui/material/InputAdornment";
import FormControlLabel from "@mui/material/FormControlLabel";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";
import useMediaQuery from "@mui/material/useMediaQuery";
import { useTheme } from "@mui/material/styles";
import CloseIcon from "@mui/icons-material/Close";
import SearchIcon from "@mui/icons-material/Search";
import MapRoundedIcon from "@mui/icons-material/MapRounded";
import { pantau, severityVar } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import {
  formatCompactCurrency,
  formatCurrencyLong,
  formatNumber,
  ownerTypeLabel,
} from "@/lib/format";
import { SEVERITY_FILTERS } from "@/lib/dashboard-logic";
import type {
  OwnerPackagesPayload,
  OwnerType,
  ProvincePackagesPayload,
  RegionPackagesPayload,
  Severity,
} from "@/types/dashboard";
import PackagesTable from "./PackagesTable";

type Section = "summary" | "packages";

function getHeader(
  areaType: "region" | "province" | "owner",
  payload: RegionPackagesPayload | ProvincePackagesPayload | OwnerPackagesPayload | null
): { title: string; sub: string; badge: string } {
  if (!payload) return { title: "Memuat…", sub: "", badge: "" };
  if (areaType === "owner") {
    const p = payload as OwnerPackagesPayload;
    return {
      title: p.owner.ownerName,
      sub: ownerTypeLabel(p.owner.ownerType),
      badge: "K/L",
    };
  }
  if (areaType === "province") {
    const p = payload as ProvincePackagesPayload;
    return {
      title: p.province.displayName,
      sub: "Provinsi · Paket Pemprov",
      badge: "Prov.",
    };
  }
  const p = payload as RegionPackagesPayload;
  return {
    title: p.region.displayName,
    sub: p.region.provinceName,
    badge: p.region.regionType === "Kota" ? "Kota" : "Kab.",
  };
}

function getMetrics(
  payload: RegionPackagesPayload | ProvincePackagesPayload | OwnerPackagesPayload | null
) {
  if (!payload) {
    return {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
      severityCounts: { low: 0, med: 0, high: 0, absurd: 0 },
    };
  }
  if ("owner" in payload) {
    return {
      totalPackages: payload.owner.totalPackages,
      totalPriorityPackages: payload.owner.totalPriorityPackages,
      totalPotentialWaste: payload.owner.totalPotentialWaste,
      totalBudget: payload.owner.totalBudget,
      severityCounts: payload.owner.severityCounts,
    };
  }
  if ("province" in payload) {
    return {
      totalPackages: payload.province.totalPackages,
      totalPriorityPackages: payload.province.totalPriorityPackages,
      totalPotentialWaste: payload.province.totalPotentialWaste,
      totalBudget: payload.province.totalBudget,
      severityCounts: payload.province.severityCounts,
    };
  }
  return {
    totalPackages: payload.region.totalPackages,
    totalPriorityPackages: payload.region.totalPriorityPackages,
    totalPotentialWaste: payload.region.totalPotentialWaste,
    totalBudget: payload.region.totalBudget,
    severityCounts: payload.region.severityCounts,
  };
}

export default function AreaDetailDrawer() {
  const theme = useTheme();
  const isDesktop = useMediaQuery(theme.breakpoints.up("lg"));

  const {
    detail,
    detailOpen,
    detailPayload,
    detailLoading,
    detailError,
    closeDetail,
    setDetailSearch,
    setDetailOwnerType,
    setDetailSeverity,
    setDetailPriorityOnly,
    changeDetailPage,
    setActiveTab,
  } = useDashboard();

  const [section, setSection] = useState<Section>("summary");

  const header = getHeader(detail.areaType, detailPayload);
  const metrics = getMetrics(detailPayload);

  const items = useMemo(() => detailPayload?.items ?? [], [detailPayload]);
  const pagination = detailPayload?.pagination;

  const handleClose = () => {
    setSection("summary");
    closeDetail();
  };

  const content = (
    <Box
      sx={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        bgcolor: pantau.surface,
      }}
    >
      {/* Header */}
      <Box
        sx={{
          px: 2.5,
          py: 2,
          borderBottom: `1px solid ${pantau.border}`,
          display: "flex",
          alignItems: "flex-start",
          gap: 1.5,
        }}
      >
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Stack
            direction="row"
            spacing={1}
            alignItems="center"
            sx={{ mb: 0.5, flexWrap: "wrap" }}
          >
            <Chip
              size="small"
              label={header.badge}
              sx={{
                height: 22,
                fontSize: 11,
                fontWeight: 700,
                bgcolor: pantau.primarySofter,
                color: pantau.primary,
                border: `1px solid ${pantau.primarySoft}`,
              }}
            />
            <Typography
              sx={{
                fontSize: 11,
                color: pantau.textSubtle,
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                fontWeight: 700,
              }}
            >
              {header.sub}
            </Typography>
          </Stack>
          <Typography
            component="h2"
            sx={{
              fontSize: 18,
              fontWeight: 800,
              color: pantau.text,
              letterSpacing: "-0.02em",
              lineHeight: 1.25,
            }}
          >
            {header.title}
          </Typography>
        </Box>

        {detail.areaType !== "owner" ? (
          <IconButton
            size="small"
            onClick={() => {
              setActiveTab("map");
              handleClose();
            }}
            title="Buka di Peta"
            aria-label="Buka di peta"
            sx={{
              color: pantau.textMuted,
              "&:hover": { color: pantau.primary, bgcolor: pantau.surfaceHover },
            }}
          >
            <MapRoundedIcon fontSize="small" />
          </IconButton>
        ) : null}
        <IconButton
          size="small"
          onClick={handleClose}
          aria-label="Tutup"
          sx={{
            color: pantau.textMuted,
            "&:hover": { color: pantau.text, bgcolor: pantau.surfaceHover },
          }}
        >
          <CloseIcon fontSize="small" />
        </IconButton>
      </Box>

      {/* Section tabs */}
      <Box
        sx={{
          display: "flex",
          borderBottom: `1px solid ${pantau.border}`,
          px: 2.5,
          gap: 2,
        }}
      >
        {(
          [
            { id: "summary", label: "Ringkasan" },
            { id: "packages", label: "Paket" },
          ] as { id: Section; label: string }[]
        ).map((t) => {
          const active = t.id === section;
          return (
            <Box
              key={t.id}
              component="button"
              onClick={() => setSection(t.id)}
              role="tab"
              aria-selected={active}
              sx={{
                all: "unset",
                cursor: "pointer",
                py: 1.25,
                fontSize: 13,
                fontWeight: active ? 700 : 500,
                color: active ? pantau.text : pantau.textMuted,
                borderBottom: "2px solid",
                borderColor: active ? pantau.primary : "transparent",
                "&:hover": { color: pantau.text },
              }}
            >
              {t.label}
            </Box>
          );
        })}
      </Box>

      {/* Content */}
      <Box sx={{ flex: 1, overflowY: "auto", px: 2.5, py: 2 }}>
        {detailError ? (
          <Alert severity="error" sx={{ mb: 2 }}>
            {detailError}
          </Alert>
        ) : null}

        {section === "summary" ? (
          <Stack spacing={2}>
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 1.5,
              }}
            >
              <MetricCard
                label="Potensi pemborosan"
                value={`Rp ${formatCompactCurrency(metrics.totalPotentialWaste)}`}
                sub={formatCurrencyLong(metrics.totalPotentialWaste)}
                accent
              />
              <MetricCard
                label="Total pagu"
                value={`Rp ${formatCompactCurrency(metrics.totalBudget)}`}
                sub={formatCurrencyLong(metrics.totalBudget)}
              />
              <MetricCard
                label="Total paket"
                value={formatNumber(metrics.totalPackages)}
                sub={`${formatNumber(metrics.totalPriorityPackages)} prioritas`}
              />
              <MetricCard
                label="Severity high+absurd"
                value={formatNumber(
                  metrics.severityCounts.high + metrics.severityCounts.absurd
                )}
                sub={`Low ${formatNumber(metrics.severityCounts.low)} · Med ${formatNumber(metrics.severityCounts.med)}`}
              />
            </Box>

            <Box>
              <Typography
                sx={{
                  fontSize: 11,
                  fontWeight: 700,
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                  color: pantau.textMuted,
                  mb: 1,
                }}
              >
                Severity breakdown
              </Typography>
              <Stack spacing={0.75}>
                {(["absurd", "high", "med", "low"] as Severity[]).map((sev) => {
                  const count = metrics.severityCounts[sev] ?? 0;
                  const total =
                    metrics.severityCounts.absurd +
                      metrics.severityCounts.high +
                      metrics.severityCounts.med +
                      metrics.severityCounts.low || 1;
                  const pct = Math.round((count / total) * 100);
                  return (
                    <Box
                      key={sev}
                      sx={{
                        display: "grid",
                        gridTemplateColumns: "80px 1fr auto",
                        alignItems: "center",
                        gap: 1,
                      }}
                    >
                      <Typography
                        sx={{
                          fontSize: 11,
                          fontWeight: 700,
                          color: severityVar(sev),
                          textTransform: "uppercase",
                          letterSpacing: "0.05em",
                        }}
                      >
                        {sev === "med" ? "Medium" : sev}
                      </Typography>
                      <Box
                        sx={{
                          height: 6,
                          bgcolor: pantau.bgMuted,
                          borderRadius: 1,
                          overflow: "hidden",
                        }}
                      >
                        <Box
                          sx={{
                            width: `${pct}%`,
                            height: "100%",
                            bgcolor: severityVar(sev),
                          }}
                        />
                      </Box>
                      <Typography
                        sx={{
                          fontSize: 11,
                          fontVariantNumeric: "tabular-nums",
                          color: pantau.textMuted,
                          minWidth: 60,
                          textAlign: "right",
                        }}
                      >
                        {formatNumber(count)} ({pct}%)
                      </Typography>
                    </Box>
                  );
                })}
              </Stack>
            </Box>
          </Stack>
        ) : (
          <Stack spacing={1.5}>
            <TextField
              placeholder="Cari paket atau pemilik…"
              size="small"
              value={detail.search}
              onChange={(e) => setDetailSearch(e.target.value)}
              slotProps={{
                input: {
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon
                        fontSize="small"
                        sx={{ color: pantau.textSubtle }}
                      />
                    </InputAdornment>
                  ),
                },
              }}
              sx={{
                "& .MuiOutlinedInput-root": {
                  bgcolor: pantau.surfaceAlt,
                  "& fieldset": { borderColor: pantau.border },
                },
              }}
            />

            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              {detail.areaType === "region" ? (
                <Select
                  size="small"
                  value={detail.ownerType}
                  onChange={(e) =>
                    setDetailOwnerType(e.target.value as OwnerType | "")
                  }
                  displayEmpty
                  sx={{ minWidth: 160, fontSize: 13 }}
                >
                  <MenuItem value="" sx={{ fontSize: 13 }}>
                    Semua pemilik
                  </MenuItem>
                  <MenuItem value="central" sx={{ fontSize: 13 }}>
                    Kementerian/Lembaga
                  </MenuItem>
                  <MenuItem value="provinsi" sx={{ fontSize: 13 }}>
                    Pemprov
                  </MenuItem>
                  <MenuItem value="kabkota" sx={{ fontSize: 13 }}>
                    Pemkot
                  </MenuItem>
                  <MenuItem value="other" sx={{ fontSize: 13 }}>
                    Others
                  </MenuItem>
                </Select>
              ) : null}

              <Select
                size="small"
                value={detail.severity}
                onChange={(e) =>
                  setDetailSeverity(e.target.value as Severity | "")
                }
                displayEmpty
                sx={{ minWidth: 160, fontSize: 13 }}
              >
                {SEVERITY_FILTERS.map((f) => (
                  <MenuItem key={f.key} value={f.key} sx={{ fontSize: 13 }}>
                    {f.label}
                  </MenuItem>
                ))}
              </Select>

              <FormControlLabel
                control={
                  <Switch
                    size="small"
                    checked={detail.priorityOnly}
                    onChange={(_e, checked) => setDetailPriorityOnly(checked)}
                  />
                }
                label={
                  <Typography
                    sx={{ fontSize: 13, color: pantau.textMuted }}
                  >
                    Hanya prioritas
                  </Typography>
                }
              />
            </Stack>

            <PackagesTable items={items} loading={detailLoading} />

            {pagination && pagination.totalPages > 1 ? (
              <Box sx={{ display: "flex", justifyContent: "center", pt: 1 }}>
                <Pagination
                  size="small"
                  count={pagination.totalPages}
                  page={pagination.page}
                  onChange={(_e, page) => changeDetailPage(page)}
                  shape="rounded"
                  color="primary"
                />
              </Box>
            ) : null}

            {pagination ? (
              <Typography
                sx={{
                  textAlign: "center",
                  fontSize: 11,
                  color: pantau.textSubtle,
                  fontVariantNumeric: "tabular-nums",
                }}
              >
                Menampilkan {items.length} dari{" "}
                {formatNumber(pagination.totalItems)} paket
              </Typography>
            ) : null}
          </Stack>
        )}
      </Box>
    </Box>
  );

  if (isDesktop) {
    return (
      <Drawer
        anchor="right"
        open={detailOpen}
        onClose={handleClose}
        slotProps={{
          paper: {
            sx: {
              width: 500,
              maxWidth: "100vw",
              bgcolor: pantau.surface,
              borderLeft: `1px solid ${pantau.border}`,
              boxShadow: pantau.shadowLg,
            },
          },
        }}
      >
        {content}
      </Drawer>
    );
  }

  return (
    <SwipeableDrawer
      anchor="bottom"
      open={detailOpen}
      onClose={handleClose}
      onOpen={() => {
        /* noop */
      }}
      disableSwipeToOpen
      slotProps={{
        paper: {
          sx: {
            height: "92vh",
            borderTopLeftRadius: 16,
            borderTopRightRadius: 16,
            bgcolor: pantau.surface,
          },
        },
      }}
    >
      <Box
        sx={{
          width: 40,
          height: 4,
          borderRadius: 2,
          bgcolor: pantau.borderStrong,
          mx: "auto",
          mt: 1,
        }}
      />
      {content}
    </SwipeableDrawer>
  );
}

function MetricCard({
  label,
  value,
  sub,
  accent,
}: {
  label: string;
  value: string;
  sub?: string;
  accent?: boolean;
}) {
  return (
    <Box
      sx={{
        p: 1.5,
        bgcolor: pantau.surfaceAlt,
        border: `1px solid ${pantau.border}`,
        borderLeft: accent
          ? `3px solid ${pantau.primary}`
          : `1px solid ${pantau.border}`,
        borderRadius: 1.5,
      }}
    >
      <Typography
        sx={{
          fontSize: 10,
          fontWeight: 700,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          color: pantau.textMuted,
          mb: 0.5,
        }}
      >
        {label}
      </Typography>
      <Typography
        sx={{
          fontFamily: "JetBrains Mono, monospace",
          fontSize: 16,
          fontWeight: 700,
          color: accent ? pantau.primary : pantau.text,
          fontVariantNumeric: "tabular-nums",
          lineHeight: 1.2,
        }}
      >
        {value}
      </Typography>
      {sub ? (
        <Typography
          sx={{
            fontSize: 10,
            color: pantau.textSubtle,
            fontVariantNumeric: "tabular-nums",
            mt: 0.25,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {sub}
        </Typography>
      ) : null}
    </Box>
  );
}
