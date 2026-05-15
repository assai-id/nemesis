export interface SummaryItem {
  label: string;
  value: string;
  muted?: boolean;
}

export interface SummarySection {
  title: string;
  items: SummaryItem[];
}

interface Props {
  heroLabel: string;
  heroValue: string;
  heroSub?: string;
  secondary?: SummaryItem[];
  sections?: SummarySection[];
}

function StatItem({ item }: Readonly<{ item: SummaryItem }>) {
  return (
    <div class={item.muted ? 'muted' : undefined}>
      <dt>{item.label}</dt>
      <dd>{item.value}</dd>
    </div>
  );
}

export function CaseFileSummary({
  heroLabel,
  heroValue,
  heroSub,
  secondary,
  sections,
}: Readonly<Props>) {
  return (
    <aside class="case-summary">
      <div class="case-section-label">Ringkasan</div>
      <dl class="stack-stats">
        <div class="stat-hero">
          <dt>{heroLabel}</dt>
          <dd>
            <strong>{heroValue}</strong>
            {heroSub && <span>{heroSub}</span>}
          </dd>
        </div>
        {(secondary ?? []).map((item) => (
          <StatItem key={item.label} item={item} />
        ))}
      </dl>
      {(sections ?? []).map((section) =>
        section.items.length === 0 ? null : (
          <div key={section.title}>
            <div class="case-section-label small">{section.title}</div>
            <dl class="stack-stats">
              {section.items.map((item) => (
                <StatItem key={item.label} item={item} />
              ))}
            </dl>
          </div>
        ),
      )}
    </aside>
  );
}
