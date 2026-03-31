import { useState } from "react";
import type { QueryMetrics } from "../types";
import FullScreenModal, { ExpandButton } from "./FullScreenModal";

interface Props {
  metrics: QueryMetrics;
}

function durationColor(ms: number): string {
  if (ms < 5000) return "text-cyan-400";
  if (ms < 30000) return "text-amber-400";
  return "text-rose-400";
}

export default function QueryOverview({ metrics }: Props) {
  const [queryFullScreen, setQueryFullScreen] = useState(false);
  const segments = [
    { label: "Compute wait", ms: metrics.waiting_for_compute_duration_ms, color: "#64748b" },
    { label: "Capacity wait", ms: metrics.waiting_at_capacity_duration_ms, color: "#f59e0b" },
    { label: "Compilation", ms: metrics.compilation_duration_ms, color: "#a855f7" },
    { label: "Execution", ms: metrics.execution_duration_ms, color: "#3b82f6" },
    { label: "Result fetch", ms: metrics.result_fetch_duration_ms, color: "#22d3ee" },
  ].filter((s) => s.ms && s.ms > 0);

  const totalBar = segments.reduce((sum, s) => sum + (s.ms || 0), 0) || 1;
  const lines = metrics.statement_text.split("\n").length;

  const statusBadge =
    metrics.execution_status === "FINISHED"
      ? "bg-cyan-500/15 text-cyan-400 ring-1 ring-cyan-500/25"
      : "bg-rose-500/15 text-rose-400 ring-1 ring-rose-500/25";

  return (
    <div className="glass-card p-6">
      <div className="flex flex-col gap-1 mb-4">
        <span className={`text-[0.65rem] font-medium px-2 py-0.5 rounded-full capitalize w-fit ${statusBadge}`}>
          {metrics.execution_status}
        </span>
        {metrics.total_duration_ms != null && (
          <span className={`text-5xl font-light tracking-tight ${durationColor(metrics.total_duration_ms)}`}>
            {formatMs(metrics.total_duration_ms)}
          </span>
        )}
      </div>

      {/* Mini code editor SQL block */}
      <div className="relative rounded-xl overflow-hidden mb-5 border border-white/[0.06] group">
        <div className="flex items-center justify-between px-4 py-2 bg-white/[0.03] border-b border-white/[0.06]">
          <div className="flex items-center gap-2">
            <span className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500">SQL</span>
            <span className="text-[0.6rem] text-slate-600">{lines} lines</span>
          </div>
          <div className="opacity-0 group-hover:opacity-100 transition-opacity">
            <ExpandButton onClick={() => setQueryFullScreen(true)} />
          </div>
        </div>
        <pre className="m-0 whitespace-pre-wrap break-words text-[0.8rem] leading-relaxed font-mono p-4 text-slate-300 max-h-[200px] overflow-y-auto bg-[#0a0f1e]">
          <code>{metrics.statement_text}</code>
        </pre>
      </div>
      <FullScreenModal
        title="SQL Query"
        open={queryFullScreen}
        onClose={() => setQueryFullScreen(false)}
      >
        <pre className="m-0 whitespace-pre-wrap break-words text-sm leading-relaxed font-mono">
          <code>{metrics.statement_text}</code>
        </pre>
      </FullScreenModal>

      {/* Duration breakdown - pill segments */}
      <div>
        <h3 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-3">Duration Breakdown</h3>
        {segments.length > 0 ? (
          <>
            <div className="flex h-8 rounded-full overflow-hidden mb-3 bg-white/[0.03]" role="img" aria-label="Duration breakdown">
              {segments.map((s) => {
                const pct = ((s.ms || 0) / totalBar) * 100;
                return (
                  <div
                    key={s.label}
                    className="min-w-[3px] flex items-center justify-center text-[0.55rem] font-medium text-white/80 transition-all duration-500 overflow-hidden"
                    style={{ width: `${pct}%`, backgroundColor: s.color }}
                    title={`${s.label}: ${formatMs(s.ms || 0)}`}
                  >
                    {pct > 15 && <span className="truncate px-1">{formatMs(s.ms || 0)}</span>}
                  </div>
                );
              })}
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500">
              {segments.map((s) => (
                <span key={s.label} className="inline-flex items-center gap-1.5">
                  <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: s.color }} />
                  {s.label}: {formatMs(s.ms || 0)}
                </span>
              ))}
            </div>
          </>
        ) : (
          <p className="text-slate-600 italic text-sm">No duration breakdown available.</p>
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
