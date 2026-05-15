import {
  buildInaprocUrl,
  formatCurrencyLong,
  severityClass,
  severityLabel,
} from '../lib/format';
import type { PackageRow } from '../types/api';

const SEVERITY_RANK: Record<string, number> = { absurd: 4, high: 3, med: 2, low: 1 };

export function pickFeaturedItem(items: PackageRow[]): PackageRow | null {
  if (!Array.isArray(items) || items.length === 0) return null;
  const candidates = items.filter(
    (item) =>
      item &&
      item.audit &&
      item.audit.reason &&
      String(item.audit.reason).trim().length >= 30,
  );
  if (candidates.length === 0) return null;
  return [...candidates].sort(
    (a, b) =>
      (SEVERITY_RANK[b.audit.severity] || 0) - (SEVERITY_RANK[a.audit.severity] || 0),
  )[0];
}

interface Props {
  featured: PackageRow | null;
}

export function CaseFileHero({ featured }: Readonly<Props>) {
  const inaprocUrl = featured ? buildInaprocUrl(featured.sourceId) : null;

  return (
    <section class="case-feature">
      <details class="case-feature-collapsible" open>
        <summary class="case-section-label case-feature-summary">
          <span>Sorotan</span>
          <span class="case-feature-chevron" aria-hidden="true"></span>
        </summary>
        <div class="case-feature-body">
          {featured ? (
            <aside class="featured-reason" aria-label="Sorotan paket prioritas">
              <span class="featured-mark" aria-hidden="true">"</span>
              <div class="featured-content">
                <p class="featured-quote">{featured.audit.reason}</p>
                <div class="featured-cite">
                  <strong>{featured.packageName}</strong>
                  <span class={`sev-b ${severityClass(featured.audit.severity)}`}>
                    {severityLabel(featured.audit.severity)}
                  </span>
                  {featured.budget !== null && featured.budget !== undefined && (
                    <span class="featured-budget">{formatCurrencyLong(featured.budget)}</span>
                  )}
                  {inaprocUrl && (
                    <a
                      class="featured-link"
                      href={inaprocUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      Lihat di inaproc.id <span aria-hidden="true">↗</span>
                    </a>
                  )}
                </div>
              </div>
            </aside>
          ) : (
            <div class="case-feature-empty">
              Belum ada paket dengan deskripsi yang menonjol di halaman ini. Coba telusuri tabel
              atau ubah filter severity untuk melihat detail audit lainnya.
            </div>
          )}
        </div>
      </details>
    </section>
  );
}
