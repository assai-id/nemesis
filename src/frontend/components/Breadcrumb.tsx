import { useDashboardStore } from '../hooks/useDashboardStore';
import { dashboardStore } from '../store/dashboard.store';

export function Breadcrumb() {
  const label = useDashboardStore((s) => s.breadcrumbLabel);

  const handleBack = (e: Event) => {
    e.preventDefault();
    dashboardStore.getState().navigateHome();
  };

  return (
    <nav class="breadcrumb" aria-label="Breadcrumb">
      <a class="breadcrumb-back" href="/" onClick={handleBack} title="Kembali ke beranda">
        <span class="breadcrumb-back-icon" aria-hidden="true">‹</span>
        <span class="breadcrumb-back-label">
          Kembali<span class="breadcrumb-back-rest"> ke beranda</span>
        </span>
      </a>
      <span class="breadcrumb-sep" aria-hidden="true">›</span>
      <span class="breadcrumb-here" id="breadcrumbHere">{label}</span>
    </nav>
  );
}
