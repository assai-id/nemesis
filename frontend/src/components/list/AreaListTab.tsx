"use client";

import { useMemo, useState } from "react";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import InputAdornment from "@mui/material/InputAdornment";
import Typography from "@mui/material/Typography";
import Skeleton from "@mui/material/Skeleton";
import Alert from "@mui/material/Alert";
import SearchIcon from "@mui/icons-material/Search";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import AreaRow from "./AreaRow";
import { formatCompactCurrency, formatNumber } from "@/lib/format";
import type {
  OwnerSummary,
  RegionArea,
  ProvinceArea,
  SortKey,
} from "@/types/dashboard";

type ListKind = "regions" | "provinces" | "owners";

type RegionSubtype = "all" | "kota" | "kabupaten";

interface SortOption {
  key: SortKey;
  label: string;
}

const SORT_OPTIONS: SortOption[] = [
  { key: "waste", label: "Potensi pemborosan" },
  { key: "priority", label: "Paket prioritas" },
  { key: "packages", label: "Total paket" },
  { key: "budget", label: "Total pagu" },
];

function sortAreas(
  areas: (RegionArea | ProvinceArea)[],
  key: SortKey
): (RegionArea | ProvinceArea)[] {
  return [...areas].sort((a, b) => {
    let cmp = 0;
    switch (key) {
      case "waste":
        cmp = b.totalPotentialWaste - a.totalPotentialWaste;
        break;
      case "priority":
        cmp = b.totalPriorityPackages - a.totalPriorityPackages;
        break;
      case "packages":
        cmp = b.totalPackages - a.totalPackages;
        break;
      case "budget":
        cmp = b.totalBudget - a.totalBudget;
        break;
    }
    if (cmp !== 0) return cmp;
    return a.displayName.localeCompare(b.displayName, "id");
  });
}

function sortOwners(owners: OwnerSummary[], key: SortKey): OwnerSummary[] {
  return [...owners].sort((a, b) => {
    let cmp = 0;
    switch (key) {
      case "waste":
        cmp = b.totalPotentialWaste - a.totalPotentialWaste;
        break;
      case "priority":
        cmp = b.totalPriorityPackages - a.totalPriorityPackages;
        break;
      case "packages":
        cmp = b.totalPackages - a.totalPackages;
        break;
      case "budget":
        cmp = b.totalBudget - a.totalBudget;
        break;
    }
    if (cmp !== 0) return cmp;
    return a.ownerName.localeCompare(b.ownerName, "id");
  });
}

export default function AreaListTab() {
  const {
    data,
    loading,
    error,
    openAreaDetail,
    openOwnerDetail,
    selectedAreaKey,
    selectedOwnerKey,
  } = useDashboard();

  const [kind, setKind] = useState<ListKind>("regions");
  const [regionSub, setRegionSub] = useState<RegionSubtype>("all");
  const [sortKey, setSortKey] = useState<SortKey>("waste");
  const [search, setSearch] = useState("");

  const filteredRegions = useMemo(() => {
    if (!data) return [];
    let areas: RegionArea[] = data.regions;
    if (regionSub === "kota") {
      areas = areas.filter((a) => a.regionType === "Kota");
    } else if (regionSub === "kabupaten") {
      areas = areas.filter((a) => a.regionType === "Kabupaten");
    }
    const q = search.trim().toLowerCase();
    if (q) {
      areas = areas.filter(
        (a) =>
          a.displayName.toLowerCase().includes(q) ||
          a.provinceName.toLowerCase().includes(q)
      );
    }
    return sortAreas(areas, sortKey) as RegionArea[];
  }, [data, regionSub, search, sortKey]);

  const filteredProvinces = useMemo(() => {
    if (!data) return [];
    let areas: ProvinceArea[] = data.provinceView.provinces;
    const q = search.trim().toLowerCase();
    if (q) {
      areas = areas.filter((a) => a.displayName.toLowerCase().includes(q));
    }
    return sortAreas(areas, sortKey) as ProvinceArea[];
  }, [data, search, sortKey]);

  const filteredOwners = useMemo(() => {
    if (!data) return [];
    let owners = data.ownerLists.central ?? [];
    const q = search.trim().toLowerCase();
    if (q) {
      owners = owners.filter((o) => o.ownerName.toLowerCase().includes(q));
    }
    return sortOwners(owners, sortKey);
  }, [data, search, sortKey]);

  const KINDS: { key: ListKind; label: string; count: number }[] = [
    {
      key: "regions",
      label: "Kabupaten/Kota",
      count: data?.regions.length ?? 0,
    },
    {
      key: "provinces",
      label: "Provinsi",
      count: data?.provinceView.provinces.length ?? 0,
    },
    {
      key: "owners",
      label: "Kementerian/Lembaga",
      count: data?.ownerLists.central?.length ?? 0,
    },
  ];

  return (
    <Box
      sx={{
        maxWidth: 1400,
        mx: "auto",
        px: { xs: 2, md: 3 },
        py: { xs: 2, md: 3 },
      }}
    >
      <Box sx={{ mb: 2 }}>
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
          Daftar Wilayah & K/L
        </Typography>
        <Typography
          sx={{ fontSize: 14, color: pantau.textMuted, lineHeight: 1.55 }}
        >
          Cari, urutkan, lalu klik baris mana pun untuk membuka detail paket.
        </Typography>
      </Box>

      {error ? (
        <Alert severity="error" sx={{ mb: 2 }}>
          Gagal memuat data: {error}
        </Alert>
      ) : null}

      {/* Category tabs */}
      <Box
        sx={{
          display: "flex",
          gap: 0.5,
          mb: 2,
          overflowX: "auto",
          borderBottom: `1px solid ${pantau.border}`,
        }}
      >
        {KINDS.map((k) => {
          const active = k.key === kind;
          return (
            <Box
              key={k.key}
              component="button"
              onClick={() => {
                setKind(k.key);
                if (k.key !== "regions") setRegionSub("all");
              }}
              role="tab"
              aria-selected={active}
              sx={{
                all: "unset",
                cursor: "pointer",
                px: 2,
                py: 1.25,
                fontSize: 13,
                fontWeight: active ? 700 : 500,
                color: active ? pantau.text : pantau.textMuted,
                borderBottom: "3px solid",
                borderColor: active ? pantau.primary : "transparent",
                whiteSpace: "nowrap",
                "&:hover": { color: pantau.text },
              }}
            >
              {k.label}
              <Box
                component="span"
                sx={{
                  ml: 1,
                  fontSize: 11,
                  color: pantau.textSubtle,
                  fontFamily: "JetBrains Mono, monospace",
                  fontWeight: 500,
                }}
              >
                {formatNumber(k.count)}
              </Box>
            </Box>
          );
        })}
      </Box>

      {/* Controls */}
      <Stack
        direction={{ xs: "column", md: "row" }}
        spacing={1.5}
        sx={{ mb: 2, alignItems: { md: "center" } }}
      >
        <TextField
          placeholder="Cari nama…"
          size="small"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
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
            flex: 1,
            "& .MuiOutlinedInput-root": {
              bgcolor: pantau.surface,
              "& fieldset": { borderColor: pantau.border },
              "&:hover fieldset": { borderColor: pantau.borderStrong },
              "&.Mui-focused fieldset": { borderColor: pantau.primary },
            },
          }}
        />

        {kind === "regions" ? (
          <Stack direction="row" spacing={0.5}>
            {(["all", "kota", "kabupaten"] as RegionSubtype[]).map((s) => {
              const active = regionSub === s;
              const label =
                s === "all" ? "Semua" : s === "kota" ? "Kota" : "Kabupaten";
              return (
                <Chip
                  key={s}
                  label={label}
                  clickable
                  onClick={() => setRegionSub(s)}
                  sx={{
                    fontWeight: 600,
                    bgcolor: active ? pantau.primary : pantau.surfaceAlt,
                    color: active ? pantau.onPrimary : pantau.textMuted,
                    border: `1px solid ${active ? pantau.primary : pantau.border}`,
                    "&:hover": {
                      bgcolor: active ? pantau.primaryHover : pantau.surfaceHover,
                    },
                  }}
                />
              );
            })}
          </Stack>
        ) : null}

        <Stack direction="row" spacing={1} alignItems="center">
          <Typography
            sx={{ fontSize: 12, color: pantau.textMuted, whiteSpace: "nowrap" }}
          >
            Urutkan
          </Typography>
          <Select
            size="small"
            value={sortKey}
            onChange={(e) => setSortKey(e.target.value as SortKey)}
            sx={{
              minWidth: 180,
              fontSize: 13,
              bgcolor: pantau.surface,
              "& fieldset": { borderColor: pantau.border },
            }}
          >
            {SORT_OPTIONS.map((o) => (
              <MenuItem key={o.key} value={o.key} sx={{ fontSize: 13 }}>
                {o.label}
              </MenuItem>
            ))}
          </Select>
        </Stack>
      </Stack>

      {/* Content */}
      {loading ? (
        <Box
          sx={{
            bgcolor: pantau.surface,
            border: `1px solid ${pantau.border}`,
            borderRadius: 2,
            p: 1.5,
          }}
        >
          <Stack spacing={1}>
            {[0, 1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} variant="rounded" height={56} />
            ))}
          </Stack>
        </Box>
      ) : kind === "owners" ? (
        <OwnerList
          owners={filteredOwners}
          selectedOwnerKey={selectedOwnerKey}
          onOpen={(owner) => openOwnerDetail(owner.ownerName, owner.ownerType)}
        />
      ) : (
        <RegionTable
          areas={
            (kind === "regions" ? filteredRegions : filteredProvinces) as (
              | RegionArea
              | ProvinceArea
            )[]
          }
          selectedKey={selectedAreaKey}
          onOpen={(area) => {
            const key =
              "regionKey" in area
                ? (area as RegionArea).regionKey
                : (area as ProvinceArea).provinceKey;
            openAreaDetail(key);
          }}
        />
      )}
    </Box>
  );
}

function RegionTable({
  areas,
  selectedKey,
  onOpen,
}: {
  areas: (RegionArea | ProvinceArea)[];
  selectedKey: string | null;
  onOpen: (area: RegionArea | ProvinceArea) => void;
}) {
  if (areas.length === 0) {
    return <EmptyState />;
  }

  return (
    <>
      {/* Desktop table */}
      <Box
        sx={{
          display: { xs: "none", md: "block" },
          bgcolor: pantau.surface,
          border: `1px solid ${pantau.border}`,
          borderRadius: 2,
          overflow: "hidden",
        }}
      >
        <Box
          component="table"
          sx={{
            width: "100%",
            borderCollapse: "collapse",
            "& thead th": {
              textAlign: "left",
              padding: "10px 12px",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.04em",
              textTransform: "uppercase",
              color: pantau.textMuted,
              bgcolor: pantau.surfaceAlt,
              borderBottom: `1px solid ${pantau.border}`,
              whiteSpace: "nowrap",
            },
          }}
        >
          <thead>
            <tr>
              <th style={{ width: 48 }}>#</th>
              <th>Wilayah</th>
              <th>Tipe</th>
              <th style={{ textAlign: "right" }}>Paket</th>
              <th style={{ textAlign: "right" }}>Prioritas</th>
              <th style={{ textAlign: "right" }}>Potensi pemborosan</th>
              <Box
                component="th"
                sx={{ textAlign: "right", display: { xs: "none", xl: "table-cell" } }}
              >
                Total pagu
              </Box>
              <Box
                component="th"
                sx={{ display: { xs: "none", lg: "table-cell" } }}
              >
                Pemilik
              </Box>
            </tr>
          </thead>
          <tbody>
            {areas.map((area, idx) => {
              const key =
                "regionKey" in area
                  ? (area as RegionArea).regionKey
                  : (area as ProvinceArea).provinceKey;
              return (
                <AreaRow
                  key={key}
                  rank={idx + 1}
                  area={area}
                  selected={selectedKey === key}
                  onClick={() => onOpen(area)}
                />
              );
            })}
          </tbody>
        </Box>
      </Box>

      {/* Mobile cards */}
      <Box
        sx={{
          display: { xs: "flex", md: "none" },
          flexDirection: "column",
          gap: 1,
        }}
      >
        {areas.map((area, idx) => {
          const key =
            "regionKey" in area
              ? (area as RegionArea).regionKey
              : (area as ProvinceArea).provinceKey;
          return (
            <Box
              key={key}
              onClick={() => onOpen(area)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onOpen(area);
                }
              }}
              sx={{
                p: 1.5,
                bgcolor: pantau.surface,
                border: `1px solid ${pantau.border}`,
                borderRadius: 2,
                display: "grid",
                gridTemplateColumns: "32px 1fr auto",
                gap: 1.25,
                alignItems: "center",
                cursor: "pointer",
                "&:active": { bgcolor: pantau.surfaceHover },
              }}
            >
              <Box
                sx={{
                  width: 28,
                  height: 28,
                  borderRadius: 1,
                  display: "grid",
                  placeItems: "center",
                  bgcolor:
                    idx < 3 ? pantau.primarySofter : pantau.surfaceAlt,
                  color: idx < 3 ? pantau.primary : pantau.textMuted,
                  fontSize: 12,
                  fontWeight: 700,
                  fontFamily: "JetBrains Mono, monospace",
                }}
              >
                {idx + 1}
              </Box>
              <Box sx={{ minWidth: 0 }}>
                <Typography
                  sx={{
                    fontSize: 14,
                    fontWeight: 700,
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
                  }}
                >
                  {area.provinceName} ·{" "}
                  {formatNumber(area.totalPackages)} paket (
                  {formatNumber(area.totalPriorityPackages)} prioritas)
                </Typography>
              </Box>
              <Typography
                sx={{
                  fontFamily: "JetBrains Mono, monospace",
                  fontSize: 13,
                  fontWeight: 700,
                  color: pantau.primary,
                  whiteSpace: "nowrap",
                }}
              >
                Rp {formatCompactCurrency(area.totalPotentialWaste)}
              </Typography>
            </Box>
          );
        })}
      </Box>
    </>
  );
}

function OwnerList({
  owners,
  selectedOwnerKey,
  onOpen,
}: {
  owners: OwnerSummary[];
  selectedOwnerKey: string | null;
  onOpen: (owner: OwnerSummary) => void;
}) {
  if (owners.length === 0) return <EmptyState />;

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        gap: 1,
      }}
    >
      {owners.map((owner, idx) => {
        const key = `${owner.ownerType}::${owner.ownerName}`;
        const selected = selectedOwnerKey === key;
        return (
          <Box
            key={key}
            onClick={() => onOpen(owner)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onOpen(owner);
              }
            }}
            sx={{
              p: 1.75,
              bgcolor: selected ? pantau.primarySofter : pantau.surface,
              border: `1px solid ${selected ? pantau.primary : pantau.border}`,
              borderRadius: 2,
              display: "grid",
              gridTemplateColumns: {
                xs: "32px 1fr auto",
                md: "36px 1fr auto auto auto",
              },
              gap: { xs: 1.25, md: 2 },
              alignItems: "center",
              cursor: "pointer",
              transition: "background .12s, border-color .12s",
              "&:hover": {
                bgcolor: selected ? pantau.primarySofter : pantau.surfaceHover,
                borderColor: pantau.borderStrong,
              },
            }}
          >
            <Box
              sx={{
                width: 32,
                height: 32,
                borderRadius: 1,
                display: "grid",
                placeItems: "center",
                bgcolor: idx < 3 ? pantau.primarySofter : pantau.surfaceAlt,
                color: idx < 3 ? pantau.primary : pantau.textMuted,
                fontSize: 12,
                fontWeight: 700,
                fontFamily: "JetBrains Mono, monospace",
              }}
            >
              {idx + 1}
            </Box>
            <Box sx={{ minWidth: 0 }}>
              <Typography
                sx={{
                  fontSize: 14,
                  fontWeight: 700,
                  color: pantau.text,
                  lineHeight: 1.3,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
                title={owner.ownerName}
              >
                {owner.ownerName}
              </Typography>
              <Typography
                sx={{
                  fontSize: 11,
                  color: pantau.textSubtle,
                }}
              >
                Kementerian/Lembaga · {formatNumber(owner.totalPackages)} paket
              </Typography>
            </Box>
            <Box
              sx={{
                display: { xs: "none", md: "block" },
                textAlign: "right",
                fontFamily: "JetBrains Mono, monospace",
                fontVariantNumeric: "tabular-nums",
                fontSize: 12,
                color: pantau.textMuted,
              }}
            >
              <Box sx={{ fontWeight: 700, color: pantau.text, fontSize: 13 }}>
                {formatNumber(owner.totalPriorityPackages)}
              </Box>
              <Box sx={{ fontSize: 11 }}>prioritas</Box>
            </Box>
            <Box
              sx={{
                display: { xs: "none", md: "block" },
                textAlign: "right",
                fontFamily: "JetBrains Mono, monospace",
                fontVariantNumeric: "tabular-nums",
                fontSize: 12,
                color: pantau.textMuted,
              }}
            >
              <Box sx={{ fontWeight: 700, color: pantau.text, fontSize: 13 }}>
                Rp {formatCompactCurrency(owner.totalBudget)}
              </Box>
              <Box sx={{ fontSize: 11 }}>total pagu</Box>
            </Box>
            <Typography
              sx={{
                fontFamily: "JetBrains Mono, monospace",
                fontSize: { xs: 13, md: 14 },
                fontWeight: 700,
                color: pantau.primary,
                whiteSpace: "nowrap",
                textAlign: "right",
              }}
            >
              Rp {formatCompactCurrency(owner.totalPotentialWaste)}
            </Typography>
          </Box>
        );
      })}
    </Box>
  );
}

function EmptyState() {
  return (
    <Box
      sx={{
        bgcolor: pantau.surface,
        border: `1px dashed ${pantau.borderStrong}`,
        borderRadius: 2,
        py: 6,
        px: 3,
        textAlign: "center",
      }}
    >
      <Typography
        sx={{
          fontSize: 14,
          fontWeight: 700,
          color: pantau.text,
          mb: 0.5,
        }}
      >
        Tidak ada hasil
      </Typography>
      <Typography sx={{ fontSize: 13, color: pantau.textMuted }}>
        Coba ubah kata kunci pencarian atau pilih kategori lain.
      </Typography>
    </Box>
  );
}
