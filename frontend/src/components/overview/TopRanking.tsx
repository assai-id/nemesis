"use client";

import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import Typography from "@mui/material/Typography";
import { pantau } from "@/theme/theme";
import { formatCompactCurrency, formatNumber } from "@/lib/format";

export interface RankingItem {
  id: string;
  label: string;
  sub: string;
  waste: number;
  priority: number;
  onClick?: () => void;
}

interface Props {
  title: string;
  items: RankingItem[];
  loading?: boolean;
  emptyLabel?: string;
  footerActionLabel?: string;
  onFooterAction?: () => void;
}

export default function TopRanking({
  title,
  items,
  loading,
  emptyLabel = "Belum ada data",
  footerActionLabel,
  onFooterAction,
}: Props) {
  const max = items.reduce((acc, it) => Math.max(acc, it.waste), 0) || 1;

  return (
    <Box
      sx={{
        bgcolor: pantau.surface,
        border: `1px solid ${pantau.border}`,
        borderRadius: 2,
        display: "flex",
        flexDirection: "column",
      }}
    >
      <Box
        sx={{
          px: { xs: 2, md: 2.5 },
          py: 1.75,
          borderBottom: `1px solid ${pantau.border}`,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 1,
        }}
      >
        <Typography
          sx={{
            fontSize: 14,
            fontWeight: 700,
            color: pantau.text,
            letterSpacing: "-0.01em",
          }}
        >
          {title}
        </Typography>
      </Box>

      <Box sx={{ p: { xs: 1.5, md: 2 }, flex: 1 }}>
        {loading ? (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 1.25 }}>
            {[0, 1, 2, 3, 4].map((i) => (
              <Skeleton key={i} variant="rounded" height={52} />
            ))}
          </Box>
        ) : items.length === 0 ? (
          <Typography
            sx={{
              textAlign: "center",
              fontSize: 12,
              color: pantau.textSubtle,
              py: 4,
            }}
          >
            {emptyLabel}
          </Typography>
        ) : (
          <Box
            component="ol"
            sx={{
              listStyle: "none",
              m: 0,
              p: 0,
              display: "flex",
              flexDirection: "column",
              gap: 0.75,
            }}
          >
            {items.map((item, idx) => {
              const pct = Math.max(2, Math.round((item.waste / max) * 100));
              const clickable = Boolean(item.onClick);
              return (
                <Box
                  key={item.id}
                  component="li"
                  onClick={item.onClick}
                  role={clickable ? "button" : undefined}
                  tabIndex={clickable ? 0 : -1}
                  onKeyDown={(e) => {
                    if (!clickable) return;
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      item.onClick?.();
                    }
                  }}
                  sx={{
                    display: "grid",
                    gridTemplateColumns: "28px 1fr auto",
                    columnGap: 1.5,
                    alignItems: "center",
                    p: 1.25,
                    borderRadius: 1.5,
                    cursor: clickable ? "pointer" : "default",
                    transition: "background .15s",
                    "&:hover": clickable
                      ? { bgcolor: pantau.surfaceHover }
                      : undefined,
                  }}
                >
                  <Box
                    sx={{
                      width: 26,
                      height: 26,
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
                        fontSize: 13,
                        fontWeight: 600,
                        color: pantau.text,
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        lineHeight: 1.3,
                      }}
                      title={item.label}
                    >
                      {item.label}
                    </Typography>
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                      <Box
                        sx={{
                          height: 3,
                          flex: 1,
                          bgcolor: pantau.bgMuted,
                          borderRadius: 2,
                          overflow: "hidden",
                          maxWidth: 160,
                        }}
                      >
                        <Box
                          sx={{
                            height: "100%",
                            width: `${pct}%`,
                            bgcolor: pantau.primary,
                            borderRadius: 2,
                          }}
                        />
                      </Box>
                      <Typography
                        sx={{
                          fontSize: 11,
                          color: pantau.textSubtle,
                          fontVariantNumeric: "tabular-nums",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {item.sub} · {formatNumber(item.priority)} prioritas
                      </Typography>
                    </Box>
                  </Box>

                  <Typography
                    sx={{
                      fontFamily: "JetBrains Mono, monospace",
                      fontSize: 13,
                      fontWeight: 700,
                      color: pantau.primary,
                      fontVariantNumeric: "tabular-nums",
                      whiteSpace: "nowrap",
                    }}
                  >
                    Rp {formatCompactCurrency(item.waste)}
                  </Typography>
                </Box>
              );
            })}
          </Box>
        )}
      </Box>

      {footerActionLabel && onFooterAction ? (
        <Box
          sx={{
            px: { xs: 2, md: 2.5 },
            py: 1.5,
            borderTop: `1px solid ${pantau.border}`,
            textAlign: "right",
          }}
        >
          <Box
            component="button"
            onClick={onFooterAction}
            sx={{
              all: "unset",
              cursor: "pointer",
              fontSize: 12,
              fontWeight: 700,
              color: pantau.primary,
              "&:hover": { color: pantau.primaryHover },
            }}
          >
            {footerActionLabel} →
          </Box>
        </Box>
      ) : null}
    </Box>
  );
}
