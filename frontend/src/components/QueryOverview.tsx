import type { QueryMetrics } from "../types";

interface Props {
  metrics: QueryMetrics;
}

export default function QueryOverview({ metrics }: Props) {
  const segments = [
    { label: "Waiting for compute", ms: metrics.waiting_for_compute_duration_ms, color: "#94a3b8" },
    { label: "Waiting at capacity", ms: metrics.waiting_at_capacity_duration_ms, color: "#f59e0b" },
    { label: "Compilation", ms: metrics.compilation_duration_ms, color: "#8b5cf6" },
    { label: "Execution", ms: metrics.execution_duration_ms, color: "#3b82f6" },
    { label: "Result fetch", ms: metrics.result_fetch_duration_ms, color: "#10b981" },
  ].filter((s) => s.ms && s.ms > 0);

  const totalBar = segments.reduce((sum, s) => sum + (s.ms || 0), 0) || 1;

  return (
    <div className="panel query-overview">
      <h2>Query Overview</h2>

      <div className="query-overview__status">
        <span
          className={`badge badge--${metrics.execution_status === "FINISHED" ? "success" : "error"}`}
        >
          {metrics.execution_status}
        </span>
        {metrics.total_duration_ms != null && (
          <span className="query-overview__duration">
            {formatMs(metrics.total_duration_ms)}
          </span>
        )}
      </div>

      <div className="query-overview__sql">
        <pre><code>{metrics.statement_text}</code></pre>
      </div>

      {segments.length > 0 && (
        <div className="query-overview__timeline">
          <h3>Duration Breakdown</h3>
          <div className="timeline-bar">
            {segments.map((s) => (
              <div
                key={s.label}
                className="timeline-bar__segment"
                style={{
                  width: `${((s.ms || 0) / totalBar) * 100}%`,
                  backgroundColor: s.color,
                }}
                title={`${s.label}: ${formatMs(s.ms || 0)}`}
              />
            ))}
          </div>
          <div className="timeline-legend">
            {segments.map((s) => (
              <span key={s.label} className="timeline-legend__item">
                <span
                  className="timeline-legend__dot"
                  style={{ backgroundColor: s.color }}
                />
                {s.label}: {formatMs(s.ms || 0)}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const mins = Math.floor(ms / 60_000);
  const secs = ((ms % 60_000) / 1000).toFixed(0);
  return `${mins}m ${secs}s`;
}
