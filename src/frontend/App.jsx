import { useEffect } from 'preact/hooks';

export function App() {
  useEffect(() => {
    let cancelled = false;
    (async () => {
      await import('./assets/js/map.js');
      if (cancelled) return;
      await import('./assets/js/app.js');
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleToggleMap = () => {
    if (window['dashboardActions']) window['dashboardActions'].toggleMap();
  };

  const handleShare = () => {
    if (window['dashboardActions']) window['dashboardActions'].copyShareLink();
  };

  const handleNavigateHome = (e) => {
    if (e && e.preventDefault) e.preventDefault();
    if (window['dashboardActions']) window['dashboardActions'].navigateHome();
  };

  const handleOpenMethods = () => {
    if (window['dashboardActions']) window['dashboardActions'].openMethods();
  };

  const handleCloseMethods = () => {
    if (window['dashboardActions']) window['dashboardActions'].closeMethods();
  };

  const handleToggleTheme = () => {
    if (window['dashboardActions']) window['dashboardActions'].toggleTheme();
  };

  return (
    <div id="preact-wrapper" data-view="dashboard">
      <header class="appbar">
        <div class="appbar-row appbar-top">
          <div class="brand">
            <a href="/" class="brand-link" onClick={handleNavigateHome} aria-label="Kembali ke beranda">
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
              onClick={handleOpenMethods}
              aria-haspopup="dialog"
            >
              Tentang metode
            </button>
            <button
              type="button"
              class="btn-theme"
              id="btnTheme"
              onClick={handleToggleTheme}
              aria-label="Ganti tema terang/gelap"
              title="Ganti tema"
            >
              <svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
              </svg>
              <svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <circle cx="12" cy="12" r="4" />
                <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" />
              </svg>
            </button>
            <button type="button" class="btn-share" id="btnShare" onClick={handleShare}>
              <span aria-hidden="true">↗</span> Salin tautan
            </button>
          </div>
        </div>
        <div class="appbar-row appbar-ticker">
          <div class="kpi" id="kpi" aria-label="Bukti ringkas nasional"></div>
          <div class="appbar-filters">
            <div class="moc" id="mf" role="tablist" aria-label="Filter pemilik"></div>
          </div>
        </div>
        <div class="appbar-row appbar-disclaimer" role="note" aria-label="Peringatan akurasi AI">
          <span class="appbar-disclaimer-text">
            <strong>Peringatan:</strong> hasil klasifikasi AI ini dapat keliru. Gunakan sebagai acuan awal, bukan satu-satunya dasar penilaian.
          </span>
        </div>
      </header>

      {/* Dashboard view: map + sidebar */}
      <main class="ml view-dashboard" aria-label="Investigasi peta dan daftar wilayah">
        <div class="mc">
          <div id="map" role="application" aria-label="Peta choropleth audit pengadaan"></div>
          <div class="mlb" id="legend" aria-label="Legenda peta"></div>
          <button
            type="button"
            class="map-toggle map-toggle-hide"
            id="toggleMapBtn"
            onClick={handleToggleMap}
            aria-pressed="false"
            title="Sembunyikan peta untuk fokus ke daftar wilayah"
          >
            <span aria-hidden="true">⇲</span> Sembunyikan peta
          </button>
        </div>
        <aside class="sb">
          <div class="sbh">
            <div class="sbh-row sbh-toolbar">
              <h2 class="list-title">
                Wilayah
                <span class="list-title-count" id="listCount" aria-live="polite"></span>
              </h2>
              <div class="sbt" id="tabs" role="tablist" aria-label="Filter jenis wilayah"></div>
              <button
                type="button"
                class="map-toggle map-toggle-show"
                id="toggleMapBtnShow"
                onClick={handleToggleMap}
                aria-pressed="true"
              >
                <span aria-hidden="true">▦</span> Tampilkan peta
              </button>
            </div>
            <div class="sbh-row sbh-controls" id="sidebarControls"></div>
            <div class="sbh-row sbh-mfsb">
              <div class="moc-sb" id="mfsb" role="tablist" aria-label="Filter pemilik"></div>
            </div>
          </div>
          <div class="sbc" id="sbc"></div>
        </aside>
      </main>

      {/* Case-file view: real page for /wilayah/, /provinsi/, /owner/ */}
      <article class="casefile view-casefile" aria-label="Berkas wilayah">
        <nav class="breadcrumb" aria-label="Breadcrumb">
          <a class="breadcrumb-back" href="/" onClick={handleNavigateHome} title="Kembali ke beranda">
            <span class="breadcrumb-back-icon" aria-hidden="true">‹</span>
            <span class="breadcrumb-back-label">Kembali<span class="breadcrumb-back-rest"> ke beranda</span></span>
          </a>
          <span class="breadcrumb-sep" aria-hidden="true">›</span>
          <span class="breadcrumb-here" id="breadcrumbHere">Berkas</span>
        </nav>
        <h1 class="visually-hidden" id="modalTop">Berkas</h1>
        <div class="casefile-body" id="modalBody"></div>
        <footer class="casefile-foot">
          Peta wilayah memakai agregasi penuh untuk paket multi-lokasi · KPI nasional tidak menduplikasi paket multi-lokasi.
        </footer>
      </article>

      {/* Methods overlay — only true overlay paradigm in app */}
      <div class="methods-overlay" id="methodsOverlay" role="dialog" aria-modal="true" aria-hidden="true" aria-labelledby="methodsTitle">
        <div class="methods-card">
          <header class="methods-head">
            <h2 id="methodsTitle">Tentang metode</h2>
            <button
              type="button"
              class="methods-close"
              aria-label="Tutup"
              onClick={handleCloseMethods}
            >
              <span aria-hidden="true">×</span>
            </button>
          </header>
          <div class="methods-body">
            <section class="methods-section">
              <h3>Sumber data</h3>
              <p>Diolah dari Sistem Informasi Rencana Umum Pengadaan (<strong>SiRUP</strong>) milik LKPP, tahun anggaran 2026, total <strong id="methodsTotalPackages">3.009.760</strong> paket teraudit dari semua kementerian/lembaga, pemerintah provinsi, dan kabupaten/kota.</p>
            </section>
            <section class="methods-section">
              <h3>Cara audit</h3>
              <p>Setiap paket dianalisis oleh model <strong>GPT-5.4-mini</strong> untuk mendeteksi indikasi pemborosan: ketidaksesuaian fungsi belanja, justifikasi nilai yang lemah, deskripsi terlalu umum, atau pola pemecahan paket. Ini bukan keputusan hukum; ini sinyal awal yang masih perlu verifikasi manusia.</p>
            </section>
            <section class="methods-section">
              <h3>Tingkat severity</h3>
              <dl class="severity-glossary">
                <div><dt><span class="sev-b sev-low">Low</span></dt><dd>Indikasi minor, perlu konfirmasi</dd></div>
                <div><dt><span class="sev-b sev-med">Medium</span></dt><dd>Pola mencurigakan, layak dicermati</dd></div>
                <div><dt><span class="sev-b sev-high">High</span></dt><dd>Indikasi kuat ketidaksesuaian, prioritas audit</dd></div>
                <div><dt><span class="sev-b sev-absurd">Absurd</span></dt><dd>Anomali ekstrem, justifikasi nilai sangat lemah</dd></div>
              </dl>
            </section>
            <section class="methods-section">
              <h3>Glosarium</h3>
              <dl class="glossary">
                <div><dt>Pagu</dt><dd>Anggaran maksimal yang dialokasikan untuk paket pengadaan, dari APBN/APBD.</dd></div>
                <div><dt>Potensi Pemborosan</dt><dd>Estimasi nilai pengadaan yang berisiko tidak efisien atau tidak sesuai peruntukan.</dd></div>
                <div><dt>Paket Prioritas</dt><dd>Paket dengan severity Medium ke atas yang dianggap layak diperiksa lebih lanjut.</dd></div>
                <div><dt>Pemilik</dt><dd>Lembaga yang menyelenggarakan pengadaan: K/L (pusat), Pemprov (provinsi), Pemkot/Pemkab (kabupaten/kota).</dd></div>
              </dl>
            </section>
            <section class="methods-section methods-disclaimer">
              <p>Hasil audit otomatis tidak menggantikan investigasi formal. Untuk verifikasi, gunakan tautan ke inaproc.id pada setiap baris paket.</p>
            </section>
          </div>
        </div>
      </div>
    </div>
  );
}
