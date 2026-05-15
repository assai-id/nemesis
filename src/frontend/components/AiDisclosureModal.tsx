import { useState, useEffect, useRef } from 'preact/hooks';
import { dashboardStore } from '../store/dashboard.store';

const STORAGE_KEY = 'nemesis-ai-disclosure-ack';

function readAck(): boolean {
  if (typeof localStorage === 'undefined') return false;
  try {
    return localStorage.getItem(STORAGE_KEY) === '1';
  } catch {
    return false;
  }
}

function writeAck() {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(STORAGE_KEY, '1');
  } catch {
    /* ignore quota/privacy errors */
  }
}

export function AiDisclosureModal() {
  const [open, setOpen] = useState(() => !readAck());
  const acknowledgeBtnRef = useRef<HTMLButtonElement>(null);
  const overlayRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    requestAnimationFrame(() => acknowledgeBtnRef.current?.focus());
    return () => {
      document.body.style.overflow = prev;
    };
  }, [open]);

  if (!open) return null;

  const acknowledge = () => {
    writeAck();
    setOpen(false);
  };

  const openMethods = () => {
    writeAck();
    setOpen(false);
    // Defer ke effect berikut supaya overlay disclosure unmount dulu sebelum
    // MethodsOverlay membuka focus trap-nya.
    requestAnimationFrame(() => dashboardStore.getState().openMethods());
  };

  return (
    <div
      ref={overlayRef}
      class="methods-overlay open"
      role="dialog"
      aria-modal="true"
      aria-labelledby="aiDisclosureTitle"
      aria-describedby="aiDisclosureBody"
    >
      <div class="methods-card ai-disclosure-card">
        <div class="ai-disclosure-pill">
          <span aria-hidden="true">!</span> Peringatan
        </div>
        <h2 id="aiDisclosureTitle" class="ai-disclosure-title">
          Data pada website ini merupakan hasil klasifikasi AI
        </h2>
        <div id="aiDisclosureBody" class="ai-disclosure-body">
          <p>
            Isi data pada website ini merupakan hasil klasifikasi dari AI, khususnya{' '}
            <strong>Large Language Model</strong>, dan dapat keliru.
          </p>
          <p>
            Harap gunakan data di website ini hanya sebagai acuan dan bantuan semata untuk
            mendukung pemantauan publik.
          </p>
          <p>
            Lihat{' '}
            <button type="button" class="ai-disclosure-link" onClick={openMethods}>
              Transparansi Algoritma
            </button>{' '}
            untuk detail model dan prompt kami agar Anda dapat memahami bagaimana klasifikasi
            dilakukan.
          </p>
        </div>
        <div class="ai-disclosure-actions">
          <button
            ref={acknowledgeBtnRef}
            type="button"
            class="ai-disclosure-ack"
            onClick={acknowledge}
          >
            Saya Mengerti
          </button>
        </div>
      </div>
    </div>
  );
}
