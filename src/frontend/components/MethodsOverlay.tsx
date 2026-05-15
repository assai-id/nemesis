import { useEffect, useRef } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { formatNumber } from '../lib/format';

export function MethodsOverlay() {
  const isOpen = useDashboardStore((s) => s.isMethodsOpen);
  const totalPackages = useDashboardStore((s) => s.data?.summary.totalPackages);
  const lastFocusedRef = useRef<HTMLElement | null>(null);
  const closeBtnRef = useRef<HTMLButtonElement>(null);
  const overlayRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isOpen) return;
    lastFocusedRef.current =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    requestAnimationFrame(() => closeBtnRef.current?.focus());
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        dashboardStore.getState().closeMethods();
      }
    };
    document.addEventListener('keydown', onKeyDown);

    return () => {
      document.removeEventListener('keydown', onKeyDown);
      document.body.style.overflow = prevOverflow;
      const target = lastFocusedRef.current;
      if (target && document.contains(target)) target.focus();
      lastFocusedRef.current = null;
    };
  }, [isOpen]);

  const handleBackdropClick = (e: MouseEvent) => {
    if (e.target === overlayRef.current) {
      dashboardStore.getState().closeMethods();
    }
  };

  return (
    <div
      ref={overlayRef}
      class={`methods-overlay${isOpen ? ' open' : ''}`}
      id="methodsOverlay"
      role="dialog"
      aria-modal="true"
      aria-hidden={isOpen ? 'false' : 'true'}
      aria-labelledby="methodsTitle"
      onClick={handleBackdropClick}
    >
      <div class="methods-card">
        <header class="methods-head">
          <h2 id="methodsTitle">Tentang metode</h2>
          <button
            ref={closeBtnRef}
            type="button"
            class="methods-close"
            aria-label="Tutup"
            onClick={() => dashboardStore.getState().closeMethods()}
          >
            <span aria-hidden="true">×</span>
          </button>
        </header>
        <div class="methods-body">
          <section class="methods-section">
            <h3>Sumber data</h3>
            <p>
              Diolah dari Sistem Informasi Rencana Umum Pengadaan (<strong>SiRUP</strong>) milik
              LKPP, tahun anggaran 2026, total{' '}
              <strong id="methodsTotalPackages">
                {totalPackages ? formatNumber(totalPackages) : '3.009.760'}
              </strong>{' '}
              paket teraudit dari semua kementerian/lembaga, pemerintah provinsi, dan
              kabupaten/kota.
            </p>
          </section>
          <section class="methods-section">
            <h3>Cara audit</h3>
            <p>
              Setiap paket dianalisis oleh model <strong>GPT-5.4-mini</strong> untuk mendeteksi
              indikasi pemborosan: ketidaksesuaian fungsi belanja, justifikasi nilai yang lemah,
              deskripsi terlalu umum, atau pola pemecahan paket. Ini bukan keputusan hukum; ini
              sinyal awal yang masih perlu verifikasi manusia.
            </p>
          </section>
          <section class="methods-section">
            <h3>Tingkat severity</h3>
            <dl class="severity-glossary">
              <div>
                <dt>
                  <span class="sev-b sev-low">Low</span>
                </dt>
                <dd>Indikasi minor, perlu konfirmasi</dd>
              </div>
              <div>
                <dt>
                  <span class="sev-b sev-med">Medium</span>
                </dt>
                <dd>Pola mencurigakan, layak dicermati</dd>
              </div>
              <div>
                <dt>
                  <span class="sev-b sev-high">High</span>
                </dt>
                <dd>Indikasi kuat ketidaksesuaian, prioritas audit</dd>
              </div>
              <div>
                <dt>
                  <span class="sev-b sev-absurd">Absurd</span>
                </dt>
                <dd>Anomali ekstrem, justifikasi nilai sangat lemah</dd>
              </div>
            </dl>
          </section>
          <section class="methods-section">
            <h3>Glosarium</h3>
            <dl class="glossary">
              <div>
                <dt>Pagu</dt>
                <dd>Anggaran maksimal yang dialokasikan untuk paket pengadaan, dari APBN/APBD.</dd>
              </div>
              <div>
                <dt>Potensi Pemborosan</dt>
                <dd>Estimasi nilai pengadaan yang berisiko tidak efisien atau tidak sesuai peruntukan.</dd>
              </div>
              <div>
                <dt>Paket Prioritas</dt>
                <dd>Paket dengan severity Medium ke atas yang dianggap layak diperiksa lebih lanjut.</dd>
              </div>
              <div>
                <dt>Pemilik</dt>
                <dd>
                  Lembaga yang menyelenggarakan pengadaan: K/L (pusat), Pemprov (provinsi),
                  Pemkot/Pemkab (kabupaten/kota).
                </dd>
              </div>
            </dl>
          </section>
          <section class="methods-section methods-disclaimer">
            <p>
              Hasil audit otomatis tidak menggantikan investigasi formal. Untuk verifikasi, gunakan
              tautan ke inaproc.id pada setiap baris paket.
            </p>
          </section>
        </div>
      </div>
    </div>
  );
}
