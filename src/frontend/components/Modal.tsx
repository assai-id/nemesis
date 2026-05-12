import { useEffect } from 'preact/hooks';
import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';
import { ModalBody } from './ModalBody';

export function Modal() {
  const isOpen = useDashboardStore((s) => s.modal.isOpen);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') dashboardStore.getState().closeModal();
    }
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, []);

  useEffect(() => {
    document.body.style.overflow = isOpen ? 'hidden' : '';
    return () => {
      document.body.style.overflow = '';
    };
  }, [isOpen]);

  return (
    <div class={`modal-overlay${isOpen ? ' open' : ''}`} id="rupModal">
      <button
        type="button"
        style={{ position: 'absolute', inset: 0, background: 'transparent', border: 'none', cursor: 'default' }}
        onClick={() => dashboardStore.getState().closeModal()}
        aria-label="Tutup modal"
      />
      <div class="modal">
        <ModalBody />
        <div class="modal-footer">
          Map memakai agregasi penuh untuk paket multi-lokasi &middot; KPI nasional tidak
          menduplikasi paket multi-lokasi
        </div>
      </div>
    </div>
  );
}
