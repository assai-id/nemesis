import { useState, useEffect } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { KpiStrip } from './KpiStrip';
import { MapFilterChips } from './MapFilterChips';

export function Header() {
  const isDark = useDashboardStore((s) => s.theme === 'dark');
  const [shareLabel, setShareLabel] = useState<string | null>(null);

  useEffect(() => {
    if (!shareLabel) return;
    const id = setTimeout(() => setShareLabel(null), 1600);
    return () => clearTimeout(id);
  }, [shareLabel]);

  const handleNavigateHome = (e: Event) => {
    e.preventDefault();
    dashboardStore.getState().navigateHome();
  };

  const handleShare = () => {
    const url = typeof location !== 'undefined' ? location.href : '';
    const finish = (ok: boolean) => setShareLabel(ok ? '✓ Tersalin' : '! Gagal');
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(url).then(
        () => finish(true),
        () => finish(false),
      );
    } else {
      finish(false);
    }
  };

  return (
    <header class="appbar">
      <div class="appbar-row appbar-top">
        <div class="brand">
          <a
            href="/"
            class="brand-link"
            onClick={handleNavigateHome}
            aria-label="Kembali ke beranda"
          >
            <div class="brand-mark" aria-hidden="true">N</div>
            <div class="brand-text">
              <strong>Nemesis</strong>
              <span>Audit Pengadaan Nasional &middot; TA 2026</span>
            </div>
          </a>
        </div>
        <div class="appbar-spacer" aria-hidden="true"></div>
        <div class="appbar-actions">
          <span class="appbar-source">Sumber: SiRUP / LKPP</span>
          <button
            type="button"
            class="btn-ghost"
            id="btnMethods"
            onClick={() => dashboardStore.getState().openMethods()}
            aria-haspopup="dialog"
          >
            Tentang metode
          </button>
          <button
            type="button"
            class="btn-theme"
            id="btnTheme"
            onClick={() => dashboardStore.getState().toggleTheme()}
            aria-label={isDark ? 'Ganti ke tema terang' : 'Ganti ke tema gelap'}
            title="Ganti tema"
          >
            <svg
              class="icon-moon"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true"
            >
              <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
            </svg>
            <svg
              class="icon-sun"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true"
            >
              <circle cx="12" cy="12" r="4" />
              <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" />
            </svg>
          </button>
          <button
            type="button"
            class={`btn-share${shareLabel ? ' copied' : ''}`}
            id="btnShare"
            onClick={handleShare}
          >
            {shareLabel ? (
              <span>{shareLabel}</span>
            ) : (
              <>
                <span aria-hidden="true">↗</span> Salin tautan
              </>
            )}
          </button>
        </div>
      </div>
      <div class="appbar-row appbar-ticker">
        <KpiStrip />
        <div class="appbar-filters">
          <MapFilterChips />
        </div>
      </div>
      <div class="appbar-row appbar-disclaimer" role="note" aria-label="Peringatan akurasi AI">
        <span class="appbar-disclaimer-text">
          <strong>Peringatan:</strong> hasil klasifikasi AI ini dapat keliru. Gunakan sebagai
          acuan awal, bukan satu-satunya dasar penilaian.
        </span>
      </div>
    </header>
  );
}
