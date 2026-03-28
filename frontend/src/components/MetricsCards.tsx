import type { QueryMetrics } from "../types";

interface Props {
  metrics: QueryMetrics;
}

export default function MetricsCards({ metrics }: Props) {
  const cards: { label: string; value: string; sub?: string }[] = [
    {
      label: "Rows Read",
      value: fmt(metrics.read_rows),
      sub: metrics.produced_rows != null ? `${fmt(metrics.produced_rows)} produced` : undefined,
    },
    {
      label: "Data Read",
      value: humanBytes(metrics.read_bytes),
      sub:
        metrics.written_bytes != null && metrics.written_bytes > 0
          ? `${humanBytes(metrics.written_bytes)} written`
          : undefined,
    },
    {
      label: "Files Read",
      value: fmt(metrics.read_files),
      sub:
        metrics.pruned_files != null
          ? `${fmt(metrics.pruned_files)} pruned`
          : undefined,
    },
    {
      label: "Partitions Read",
      value: fmt(metrics.read_partitions),
    },
    {
      label: "Spill to Disk",
      value: humanBytes(metrics.spilled_local_bytes),
    },
    {
      label: "IO Cache Hit",
      value:
        metrics.read_io_cache_percent != null
          ? `${metrics.read_io_cache_percent}%`
          : "N/A",
    },
    {
      label: "Shuffle Read",
      value: humanBytes(metrics.shuffle_read_bytes),
    },
    {
      label: "Result Cache",
      value: metrics.from_result_cache ? "Yes" : "No",
    },
  ];

  return (
    <div className="panel metrics-cards">
      <h2>Execution Metrics</h2>
      <div className="metrics-cards__grid">
        {cards.map((c) => (
          <div key={c.label} className="metric-card">
            <div className="metric-card__label">{c.label}</div>
            <div className="metric-card__value">{c.value}</div>
            {c.sub && <div className="metric-card__sub">{c.sub}</div>}
          </div>
        ))}
      </div>
    </div>
  );
}

function fmt(n: number | null | undefined): string {
  if (n == null) return "N/A";
  return n.toLocaleString();
}

function humanBytes(b: number | null | undefined): string {
  if (b == null || b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let val = b;
  while (Math.abs(val) >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
