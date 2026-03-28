import type { QueryMetrics } from "../types";

interface Props {
  metrics: QueryMetrics;
}

export default function QueryOverview({ metrics }: Props) {
  const segments = [
    { label: "Waiting for compute", ms: metrics.waiting_for_compute_duration_ms, color: "#9aa0a6" },
    { label: "Waiting at capacity", ms: metrics.waiting_at_capacity_duration_ms, color: "#e37400" },
    { label: "Compilation", ms: metrics.compilation_duration_ms, color: "#7b1fa2" },
    { label: "Execution", ms: metrics.execution_duration_ms, color: "#1b73e8" },
    { label: "Result fetch", ms: metrics.result_fetch_duration_ms, color: "#188038" },
  ].filter((s) => s.ms && s.ms > 0);

  const totalBar = segments.reduce((sum, s) => sum + (s.ms || 0), 0) || 1;

  const statusBadge =
    metrics.execution_status === "FINISHED"
      ? "bg-green-100 text-green-800"
      : "bg-red-100 text-red-800";

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">Query Overview</h2>

      <div className="flex items-center gap-3 mb-3">
        <span className={`text-xs font-medium px-2.5 py-0.5 rounded-full capitalize ${statusBadge}`}>
          {metrics.execution_status}
        </span>
        {metrics.total_duration_ms != null && (
          <span className="text-2xl font-semibold text-gray-900">
            {formatMs(metrics.total_duration_ms)}
          </span>
        )}
      </div>

      <div className="bg-gray-900 text-gray-300 rounded-lg p-4 overflow-x-auto mb-4">
        <pre className="m-0 whitespace-pre-wrap break-words text-[0.8rem] leading-relaxed font-mono">
          <code>{metrics.statement_text}</code>
        </pre>
      </div>

      <div>
        <h3 className="text-sm font-semibold mt-3 mb-2">Duration Breakdown</h3>
        {segments.length > 0 ? (
          <>
            <div
              className="flex h-5 rounded overflow-hidden mb-2"
              role="img"
              aria-label="Duration breakdown bar chart"
            >
              {segments.map((s) => (
                <div
                  key={s.label}
                  className="min-w-[2px] transition-all duration-300"
                  style={{
                    width: `${((s.ms || 0) / totalBar) * 100}%`,
                    backgroundColor: s.color,
                  }}
                  title={`${s.label}: ${formatMs(s.ms || 0)}`}
                />
              ))}
            </div>
            <div className="flex flex-wrap gap-3 text-xs text-gray-500">
              {segments.map((s) => (
                <span key={s.label} className="inline-flex items-center gap-1">
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{ backgroundColor: s.color }}
                  />
                  {s.label}: {formatMs(s.ms || 0)}
                </span>
              ))}
            </div>
          </>
        ) : (
          <p className="text-gray-400 italic text-sm">
            No duration breakdown available for this query.
          </p>
        )}
      </div>
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
