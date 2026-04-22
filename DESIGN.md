# Nemesis Design System

This document is the source of truth for the Nemesis project's UI design system. AI agents and developers must strictly adhere to these tokens, rules, and guidelines when creating or modifying UI components.

## 1. Global Rules
- **Aesthetic:** Modern, sleek dark mode, data-heavy but clean interface, using "glassmorphism" for overlays.
- **Responsiveness:** Fluid and responsive. Elements should gracefully adapt to smaller screens.
- **Interactivity:** Elements must feel alive. Always include subtle micro-animations (e.g., hover states, transitions).
- **Icons:** Use emojis or simple modern SVG icons sparingly and consistently.

## 2. Typography
Use Google Fonts **Outfit** for headings and **Inter** for body text.

- **Font Families:**
  - Headings: `'Outfit', sans-serif`
  - Body & Data: `'Inter', sans-serif`
  - Monospace (IDs, Code): `'JetBrains Mono', monospace`
- **Font Sizes:**
  - H1: `24px` (Bold)
  - H2: `18px` (Semi-bold)
  - Body Text: `13px` (Regular)
  - Small Text / Labels: `11px` (Medium)
  - Micro Text: `9.5px`
- **Line Height:** `1.35` to `1.5` for readability.

## 3. Color Palette (Dark Theme)

### Base Backgrounds & Surfaces
- `--bg-base`: `#0F1115` (Deepest background)
- `--bg-surface`: `#1A1D24` (Cards, panels, sidebars)
- `--bg-surface-elevated`: `#242831` (Hover states on cards)
- `--bg-glass`: `rgba(26, 29, 36, 0.65)` (Backdrop filter: blur(12px) for glassmorphism)

### Text Colors
- `--text-primary`: `#F8F9FA` (Main text, titles)
- `--text-secondary`: `#A9ADC1` (Subtitles, labels, descriptions)
- `--text-muted`: `#686D7F` (Borders, deactivated text)

### Accent & Severity Colors
- `--accent-primary`: `#F0D8A8` (Gold/Sand accent for selected states and focus)
- `--accent-secondary`: `#B5A882` (Muted gold for borders)
- `--sev-low`: `#7B86A3` (Steel blue)
- `--sev-med`: `#8B7332` (Olive/Bronze)
- `--sev-high`: `#A83C2E` (Brick Red)
- `--sev-absurd`: `#D4A999` (Rose/Dust)
- `--success-sage`: `#7DB894` (Budget metrics)

### Map Legend Scale
1. `#243155` (Zero / Empty)
2. `#7B86A3`
3. `#B5A882`
4. `#D4A999`
5. `#8B7332`
6. `#A83C2E` (Highest Waste)

## 4. Spacing & Grid System
- `--space-xs`: `4px`
- `--space-sm`: `8px`
- `--space-md`: `16px`
- `--space-lg`: `24px`
- `--space-xl`: `32px`

## 5. Shape & Elevation
- **Border Radius:**
  - `--radius-sm`: `4px` (Tags, badges)
  - `--radius-md`: `8px` (Cards, modals, inputs)
  - `--radius-lg`: `12px` (Large panels)
- **Shadows:**
  - `--shadow-sm`: `0 2px 4px rgba(0, 0, 0, 0.2)`
  - `--shadow-md`: `0 4px 12px rgba(0, 0, 0, 0.3)`
  - `--shadow-glow`: `0 0 16px rgba(240, 216, 168, 0.1)` (For active elements)
- **Borders:**
  - `--border-subtle`: `1px solid rgba(255, 255, 255, 0.08)`

## 6. Components Specs

### Modals & Overlays
- **Background:** Use `--bg-glass` with `backdrop-filter: blur(12px)`.
- **Border:** `1px solid rgba(255, 255, 255, 0.1)`.
- **Shadow:** `--shadow-md`.

### Cards & Sidebar Items (`.pi`, `.kc`)
- **Default:** `--bg-surface`, `--border-subtle`, `--radius-md`.
- **Hover:** Background changes to `--bg-surface-elevated`, border subtly brightens, translation `transform: translateY(-1px)`, transition `0.2s ease`.
- **Active/Selected:** Add left border or outline in `--accent-primary`, and `--shadow-glow`.

### Buttons & Chips (`.fc`, `.stb`, `.pager-btn`)
- **Default:** `--bg-surface-elevated`, text `--text-secondary`, border `--border-subtle`.
- **Hover:** Brighter background, text `--text-primary`.
- **Active:** Background `--accent-primary`, text `#000` (dark for contrast).

### Animations & Transitions
- **Standard Transition:** `all 0.2s ease-in-out`
- **Modal Opening:** Fade-in and slight scale up (0.95 -> 1.0).
- **Loading State:** Smooth pulse animation for skeleton loading elements.
