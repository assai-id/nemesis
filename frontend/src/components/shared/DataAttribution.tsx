"use client";

import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { pantau } from "@/theme/theme";

export default function DataAttribution() {
  return (
    <Box
      sx={{
        mt: 3,
        px: 2.5,
        py: 2,
        bgcolor: pantau.surfaceAlt,
        border: `1px solid ${pantau.border}`,
        borderRadius: 2,
        display: "flex",
        gap: 1.5,
        alignItems: "flex-start",
      }}
    >
      <InfoOutlinedIcon
        fontSize="small"
        sx={{ color: pantau.textSubtle, mt: 0.25, flexShrink: 0 }}
      />
      <Box>
        <Typography
          sx={{
            fontSize: 13,
            fontWeight: 600,
            color: pantau.text,
            mb: 0.5,
          }}
        >
          Sumber data
        </Typography>
        <Typography
          sx={{
            fontSize: 12,
            color: pantau.textMuted,
            lineHeight: 1.55,
          }}
        >
          Kumpulan data pengadaan nasional LKPP / SiRUP Tahun Anggaran 2026.
          Agregasi per wilayah mendeduplikasi paket multi-lokasi pada level
          provinsi, namun map kab/kota menghitung penuh tiap kemunculan lokasi.
          Angka &quot;potensi pemborosan&quot; merupakan estimasi heuristik —
          bukan temuan audit final.
        </Typography>
      </Box>
    </Box>
  );
}
