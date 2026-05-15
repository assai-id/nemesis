import { useDashboardStore } from '../hooks/useDashboardStore';
import { Breadcrumb } from './Breadcrumb';
import { ModalBody } from './ModalBody';

export function Modal() {
  const caseTitle = useDashboardStore((s) => s.breadcrumbLabel);

  return (
    <article class="casefile view-casefile" aria-label="Berkas wilayah">
      <Breadcrumb />
      <h1 class="visually-hidden" id="modalTop">{caseTitle}</h1>
      <div class="casefile-body" id="modalBody">
        <ModalBody />
      </div>
      <footer class="casefile-foot">
        Peta wilayah memakai agregasi penuh untuk paket multi-lokasi · KPI nasional tidak menduplikasi paket multi-lokasi.
      </footer>
    </article>
  );
}
