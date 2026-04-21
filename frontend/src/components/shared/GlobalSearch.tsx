"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import InputAdornment from "@mui/material/InputAdornment";
import Chip from "@mui/material/Chip";
import SearchIcon from "@mui/icons-material/Search";
import PlaceRoundedIcon from "@mui/icons-material/PlaceRounded";
import AccountBalanceRoundedIcon from "@mui/icons-material/AccountBalanceRounded";
import { pantau } from "@/theme/theme";
import { useDashboard } from "@/components/dashboard/DashboardContext";
import type { OwnerType } from "@/types/dashboard";
import { formatCompactCurrency } from "@/lib/format";

type Hit =
  | {
      kind: "area";
      id: string;
      label: string;
      sub: string;
      waste: number;
    }
  | {
      kind: "owner";
      id: string;
      label: string;
      sub: string;
      ownerName: string;
      ownerType: OwnerType;
      waste: number;
    };

export default function GlobalSearch() {
  const { data, openAreaDetail, openOwnerDetail, setActiveTab } = useDashboard();
  const [input, setInput] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      const mod = event.metaKey || event.ctrlKey;
      if (mod && event.key.toLowerCase() === "k") {
        event.preventDefault();
        inputRef.current?.focus();
        inputRef.current?.select();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  const options = useMemo<Hit[]>(() => {
    if (!data) return [];
    const hits: Hit[] = [];
    for (const region of data.regions) {
      hits.push({
        kind: "area",
        id: `area::${region.regionKey}`,
        label: region.displayName,
        sub: region.provinceName,
        waste: region.totalPotentialWaste,
      });
    }
    for (const owner of data.ownerLists.central ?? []) {
      hits.push({
        kind: "owner",
        id: `owner::central::${owner.ownerName}`,
        label: owner.ownerName,
        sub: "Kementerian/Lembaga",
        ownerName: owner.ownerName,
        ownerType: owner.ownerType,
        waste: owner.totalPotentialWaste,
      });
    }
    return hits;
  }, [data]);

  const handleSelect = (hit: Hit | null) => {
    if (!hit) return;
    if (hit.kind === "area") {
      const regionKey = hit.id.replace(/^area::/, "");
      openAreaDetail(regionKey);
      setActiveTab("map");
    } else {
      openOwnerDetail(hit.ownerName, hit.ownerType);
      setActiveTab("list");
    }
    setInput("");
  };

  return (
    <Autocomplete<Hit, false, false, false>
      options={options}
      value={null}
      inputValue={input}
      onInputChange={(_event, value, reason) => {
        if (reason !== "reset") setInput(value);
      }}
      onChange={(_event, value) => handleSelect(value)}
      clearOnBlur
      clearOnEscape
      blurOnSelect
      getOptionLabel={(opt) => opt.label}
      filterOptions={(opts, state) => {
        const q = state.inputValue.trim().toLowerCase();
        if (!q) return opts.slice(0, 12);
        const scored = opts
          .map((opt) => {
            const label = opt.label.toLowerCase();
            const sub = opt.sub.toLowerCase();
            const matches = label.includes(q) || sub.includes(q);
            if (!matches) return null;
            const starts = label.startsWith(q) ? 0 : 1;
            return { opt, starts };
          })
          .filter((v): v is { opt: Hit; starts: number } => v !== null);
        scored.sort((a, b) => a.starts - b.starts);
        return scored.slice(0, 30).map((v) => v.opt);
      }}
      isOptionEqualToValue={(o, v) => o.id === v.id}
      noOptionsText={input ? "Tidak ditemukan" : "Mulai ketik nama wilayah atau K/L"}
      renderInput={(params) => (
        <TextField
          {...params}
          inputRef={inputRef}
          placeholder="Cari wilayah atau Kementerian/Lembaga…   (⌘K)"
          size="small"
          fullWidth
          sx={{
            maxWidth: 560,
            mx: "auto",
            display: "block",
            "& .MuiOutlinedInput-root": {
              bgcolor: pantau.surfaceAlt,
              borderRadius: 1,
              fontSize: 14,
              "& fieldset": { borderColor: pantau.border },
              "&:hover fieldset": { borderColor: pantau.borderStrong },
              "&.Mui-focused fieldset": {
                borderColor: pantau.primary,
                borderWidth: 2,
              },
            },
          }}
          slotProps={{
            ...params.slotProps,
            input: {
              ...params.slotProps.input,
              startAdornment: (
                <>
                  <InputAdornment position="start" sx={{ ml: 0.5 }}>
                    <SearchIcon
                      fontSize="small"
                      sx={{ color: pantau.textSubtle }}
                    />
                  </InputAdornment>
                  {params.slotProps.input.startAdornment}
                </>
              ),
            },
          }}
        />
      )}
      renderOption={(props, option) => (
        <Box
          component="li"
          {...props}
          sx={{
            display: "flex",
            alignItems: "center",
            gap: 1.25,
            py: 1,
          }}
          key={option.id}
        >
          <Box
            sx={{
              width: 28,
              height: 28,
              borderRadius: 1,
              display: "grid",
              placeItems: "center",
              bgcolor:
                option.kind === "area"
                  ? pantau.primarySofter
                  : pantau.slateSoft,
              color:
                option.kind === "area" ? pantau.primary : pantau.text,
              flexShrink: 0,
            }}
          >
            {option.kind === "area" ? (
              <PlaceRoundedIcon fontSize="small" />
            ) : (
              <AccountBalanceRoundedIcon fontSize="small" />
            )}
          </Box>
          <Box sx={{ minWidth: 0, flex: 1 }}>
            <Typography
              sx={{
                fontSize: 13,
                fontWeight: 600,
                color: pantau.text,
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
              }}
            >
              {option.label}
            </Typography>
            <Typography
              sx={{
                fontSize: 11,
                color: pantau.textSubtle,
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
              }}
            >
              {option.sub}
            </Typography>
          </Box>
          <Chip
            size="small"
            label={`Rp ${formatCompactCurrency(option.waste)}`}
            sx={{
              bgcolor: pantau.primarySofter,
              color: pantau.primary,
              fontWeight: 700,
              fontFamily: "JetBrains Mono, monospace",
              fontSize: 11,
              height: 22,
              flexShrink: 0,
            }}
          />
        </Box>
      )}
    />
  );
}
