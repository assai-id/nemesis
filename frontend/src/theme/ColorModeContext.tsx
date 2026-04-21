"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

export type ColorMode = "light" | "dark";

const STORAGE_KEY = "nemesis-dashboard-color-mode";

type Ctx = {
  mode: ColorMode;
  setMode: (m: ColorMode) => void;
  toggle: () => void;
};

const ColorModeContext = createContext<Ctx | null>(null);

export function ColorModeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ColorMode>("light");

  useEffect(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw === "light" || raw === "dark") {
        // Preferensi tersimpan; sinkronkan setelah mount (hindari mismatch SSR).
        // eslint-disable-next-line react-hooks/set-state-in-effect -- sync from localStorage once
        setModeState(raw);
      }
    } catch {
      /* ignore */
    }
  }, []);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", mode);
    try {
      localStorage.setItem(STORAGE_KEY, mode);
    } catch {
      /* ignore */
    }
  }, [mode]);

  const setMode = useCallback((m: ColorMode) => setModeState(m), []);
  const toggle = useCallback(
    () => setModeState((m) => (m === "light" ? "dark" : "light")),
    []
  );

  const value = useMemo(
    () => ({ mode, setMode, toggle }),
    [mode, setMode, toggle]
  );

  return (
    <ColorModeContext.Provider value={value}>
      {children}
    </ColorModeContext.Provider>
  );
}

export function useColorMode(): Ctx {
  const ctx = useContext(ColorModeContext);
  if (!ctx) {
    throw new Error("useColorMode harus di dalam ColorModeProvider");
  }
  return ctx;
}
