"use client";

import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import DarkModeOutlinedIcon from "@mui/icons-material/DarkModeOutlined";
import LightModeOutlinedIcon from "@mui/icons-material/LightModeOutlined";
import GitHubIcon from "@mui/icons-material/GitHub";
import { pantau } from "@/theme/theme";
import { useColorMode } from "@/theme/ColorModeContext";
import GlobalSearch from "../shared/GlobalSearch";

export default function AppTopBar() {
  const { mode, toggle } = useColorMode();

  return (
    <Box
      component="header"
      sx={{
        position: "sticky",
        top: 0,
        zIndex: 20,
        bgcolor: pantau.surface,
        borderBottom: `1px solid ${pantau.border}`,
        boxShadow: "none",
      }}
    >
      <Box
        sx={{
          maxWidth: 1400,
          mx: "auto",
          px: { xs: 2, md: 3 },
          py: { xs: 1.25, md: 1.5 },
          display: "grid",
          gridTemplateColumns: { xs: "auto 1fr auto", md: "auto 1fr auto" },
          alignItems: "center",
          gap: { xs: 1.5, md: 3 },
        }}
      >
        {/* Brand */}
        <Stack
          direction="row"
          spacing={1.5}
          sx={{ minWidth: 0, alignItems: "center" }}
        >
          <Box
            sx={{
              width: 36,
              height: 36,
              borderRadius: 1,
              bgcolor: pantau.primary,
              color: pantau.onPrimary,
              display: "grid",
              placeItems: "center",
              fontWeight: 800,
              fontSize: 14,
              letterSpacing: "0.06em",
              flexShrink: 0,
            }}
          >
            PT
          </Box>
          <Box sx={{ minWidth: 0, display: { xs: "none", sm: "block" } }}>
            <Typography
              component="h1"
              sx={{
                fontSize: { xs: 14, md: 15 },
                fontWeight: 800,
                color: pantau.text,
                letterSpacing: "-0.02em",
                lineHeight: 1.2,
              }}
            >
              Pantau Pengadaan
            </Typography>
            <Typography
              sx={{
                fontSize: 11,
                color: pantau.textSubtle,
                fontWeight: 500,
                lineHeight: 1.3,
              }}
            >
              Dashboard transparansi pengadaan · LKPP/SiRUP TA 2026
            </Typography>
          </Box>
        </Stack>

        {/* Search */}
        <Box sx={{ minWidth: 0, width: "100%" }}>
          <GlobalSearch />
        </Box>

        {/* Actions */}
        <Stack
          direction="row"
          spacing={0.5}
          sx={{ minWidth: 0, alignItems: "center" }}
        >
          <Tooltip
            title={mode === "light" ? "Mode gelap" : "Mode terang"}
            arrow
          >
            <IconButton
              onClick={toggle}
              size="small"
              sx={{
                color: pantau.textMuted,
                "&:hover": {
                  color: pantau.text,
                  bgcolor: pantau.surfaceHover,
                },
              }}
              aria-label="Toggle color mode"
            >
              {mode === "light" ? (
                <DarkModeOutlinedIcon fontSize="small" />
              ) : (
                <LightModeOutlinedIcon fontSize="small" />
              )}
            </IconButton>
          </Tooltip>
          <Tooltip title="Kode sumber" arrow>
            <IconButton
              size="small"
              component="a"
              href="https://github.com/assai-id/nemesis"
              target="_blank"
              rel="noopener noreferrer"
              sx={{
                color: pantau.textMuted,
                display: { xs: "none", md: "inline-flex" },
                "&:hover": {
                  color: pantau.text,
                  bgcolor: pantau.surfaceHover,
                },
              }}
              aria-label="Source code"
            >
              <GitHubIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Stack>
      </Box>
    </Box>
  );
}
