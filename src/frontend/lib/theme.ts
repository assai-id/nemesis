import type { Legend } from '../types/api';
import type { Theme } from '../types/store';

interface ChoroplethThemeColors {
  palette: string[];
  zeroColor: string;
  strokeDefault: string;
  strokeSelected: string;
}

export const CHOROPLETH_THEMES: Record<Theme, ChoroplethThemeColors> = {
  // Light palette: cream → gold → orange → red-brown → maroon. Hue bergeser
  // tegas dan langkah lightness ~0.16 per tier supaya tier tengah (orange vs
  // red-brown) tidak menyatu dengan tier sebelahnya seperti pada palette
  // monokromatik beige-brown sebelumnya.
  light: {
    palette: ['#f5dca0', '#e8a14a', '#cc6224', '#a83c1d', '#6d1808'],
    zeroColor: '#eeeae1',
    strokeDefault: '#aaa49a',
    strokeSelected: '#a83c1d',
  },
  // Dark palette: cream → gold → orange → red → crimson. Langkah lightness
  // dipisah ~0.13 per tier sehingga di atas navy bg (dengan fill opacity 0.52)
  // tetap mudah dibedakan termasuk tier merah dan merah pekat.
  //
  // zeroColor harus tampak terhadap surface-hi (#243155) yang dipakai panel
  // legenda — kalau dipasang #243155, swatch hilang. Pakai navy-grey yang
  // lebih terang sehingga kontras vs panel maupun terhadap basemap dark.
  dark: {
    palette: ['#f5e4a8', '#f0b568', '#e88a3c', '#d6452a', '#8a1e18'],
    zeroColor: '#4a5982',
    strokeDefault: '#3d4f78',
    strokeSelected: '#e88a3c',
  },
};

export function getThemeColors(theme: Theme): ChoroplethThemeColors {
  return CHOROPLETH_THEMES[theme];
}

export function applyChoroplethPalette(legend: Legend | null | undefined, theme: Theme): Legend {
  const colors = getThemeColors(theme);
  if (!legend || !Array.isArray(legend.ranges) || legend.ranges.length === 0) {
    return { zeroColor: colors.zeroColor, ranges: [] };
  }
  const palette = colors.palette;
  const n = legend.ranges.length;
  const ranges = legend.ranges.map((r, i) => ({
    ...r,
    color:
      palette[
        Math.min(
          palette.length - 1,
          Math.round((i / Math.max(n - 1, 1)) * (palette.length - 1)),
        )
      ],
  }));
  return { ...legend, zeroColor: colors.zeroColor, ranges };
}
