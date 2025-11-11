import React, { createContext, useContext, useEffect, useState } from "react";

type Theme = "light" | "dark" | "system";
type Ctx = {
  theme: Theme;
  setTheme: (t: Theme) => void;
  isDark: boolean;
};

const ThemeContext = createContext<Ctx | null>(null);

function getSystemPrefersDark() {
  return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
}

function applyThemeClass(theme: Theme) {
  const root = document.documentElement;
  const wantDark = theme === "dark" || (theme === "system" && getSystemPrefersDark());
  root.classList.toggle("dark", wantDark);
}

export function ThemeProvider({ defaultTheme = "system", storageKey = "ui-theme", children }:{
  defaultTheme?: Theme;
  storageKey?: string;
  children: React.ReactNode;
}) {
  const [theme, setThemeState] = useState<Theme>(() => {
    try {
      return (localStorage.getItem(storageKey) as Theme) || defaultTheme;
    } catch {
      return defaultTheme;
    }
  });

  const isDark = theme === "dark" || (theme === "system" && typeof window !== "undefined" && getSystemPrefersDark());

  useEffect(() => {
    applyThemeClass(theme);
    try { localStorage.setItem(storageKey, theme); } catch {}
    if (theme === "system") {
      const mql = window.matchMedia("(prefers-color-scheme: dark)");
      const handler = () => applyThemeClass("system");
      mql.addEventListener("change", handler);
      return () => mql.removeEventListener("change", handler);
    }
  }, [theme, storageKey]);

  const setTheme = (t: Theme) => setThemeState(t);

  return (
    <ThemeContext.Provider value={{ theme, setTheme, isDark }}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider");
  return ctx;
}
