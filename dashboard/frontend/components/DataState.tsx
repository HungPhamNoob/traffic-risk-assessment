export function FallbackBanner({ active }: { active: boolean }) {
  if (!active) return null;
  return (
    <div className="fallback-banner">
      Demo fallback data is visible because the API returned no usable data or is
      currently unavailable.
    </div>
  );
}

export function RiskBadge({ level }: { level?: string }) {
  const normalized = level || "low";
  return <span className={`status-pill risk-${normalized}`}>{normalized}</span>;
}

export function KpiCard({
  label,
  value,
  tone,
  detail
}: {
  label: string;
  value: string | number;
  tone?: "low" | "medium" | "high";
  detail?: string;
}) {
  return (
    <section className="card">
      <div className="card-title">{label}</div>
      <div className={`card-value ${tone ? `risk-${tone}` : ""}`}>{value}</div>
      {detail ? <div className="muted">{detail}</div> : null}
    </section>
  );
}
