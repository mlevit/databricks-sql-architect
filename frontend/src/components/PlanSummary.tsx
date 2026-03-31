import { useState, useMemo } from "react";
import type { PlanSummary as PlanSummaryType, PlanHighlight } from "../types";
import FullScreenModal, { ExpandButton } from "./FullScreenModal";
import { RecommendationCard } from "./shared/recommendation";

interface Props {
  plan: PlanSummaryType;
}

type HighlightMap = Map<number, PlanHighlight[]>;

function buildHighlightMap(highlights: PlanHighlight[]): HighlightMap {
  const map: HighlightMap = new Map();
  for (const h of highlights) {
    for (let i = h.line_start; i <= h.line_end; i++) {
      const existing = map.get(i);
      if (existing) existing.push(h);
      else map.set(i, [h]);
    }
  }
  return map;
}

function severityBg(severity: string): string {
  switch (severity) {
    case "critical": return "bg-rose-500/10 shadow-[inset_3px_0_0_theme(colors.rose.500)]";
    case "warning": return "bg-amber-500/10 shadow-[inset_3px_0_0_theme(colors.amber.500)]";
    default: return "bg-blue-500/10 shadow-[inset_3px_0_0_theme(colors.blue.500)]";
  }
}

function severityBadgeClass(severity: string): string {
  switch (severity) {
    case "critical": return "bg-rose-500/20 text-rose-300 border border-rose-500/30";
    case "warning": return "bg-amber-500/20 text-amber-300 border border-amber-500/30";
    default: return "bg-blue-500/20 text-blue-300 border border-blue-500/30";
  }
}

function HighlightedPlan({ rawPlan, highlights }: { rawPlan: string; highlights: PlanHighlight[] }) {
  const lines = rawPlan.split("\n");
  const highlightMap = useMemo(() => buildHighlightMap(highlights), [highlights]);
  const [expandedLines, setExpandedLines] = useState<Set<number>>(new Set());

  const toggleLine = (lineIdx: number) => {
    setExpandedLines((prev) => {
      const next = new Set(prev);
      if (next.has(lineIdx)) next.delete(lineIdx);
      else next.add(lineIdx);
      return next;
    });
  };

  return (
    <div className="font-mono text-xs leading-relaxed">
      {lines.map((line, idx) => {
        const lineHighlights = highlightMap.get(idx);
        const isHighlighted = !!lineHighlights;
        const isFirstLineOfHighlight = lineHighlights?.some((h) => h.line_start === idx);
        const isExpanded = expandedLines.has(idx);

        return (
          <div key={idx}>
            <div
              className={`flex items-start ${isHighlighted ? severityBg(lineHighlights[0].severity) : ""} ${isFirstLineOfHighlight ? "cursor-pointer hover:bg-white/[0.03]" : ""}`}
              onClick={isFirstLineOfHighlight ? () => toggleLine(idx) : undefined}
              title={isFirstLineOfHighlight ? "Click to see details" : undefined}
            >
              <span className="select-none text-slate-600 text-right w-10 pr-3 shrink-0">{idx + 1}</span>
              <span className="flex-1 whitespace-pre text-slate-300">{line || " "}</span>
              {isFirstLineOfHighlight && (
                <span className="select-none shrink-0 pl-2 pr-1 text-slate-500">{isExpanded ? "▾" : "▸"}</span>
              )}
            </div>
            {isFirstLineOfHighlight && isExpanded && lineHighlights && (
              <div className="pl-10 pr-2 py-1.5 space-y-1">
                {lineHighlights.map((h, hIdx) => (
                  <div key={hIdx} className={`inline-flex items-center gap-1.5 text-[0.7rem] px-2 py-0.5 rounded ${severityBadgeClass(h.severity)}`}>
                    <span className="font-semibold uppercase tracking-wide text-[0.6rem]">{h.severity}</span>
                    <span className="opacity-40">|</span>
                    <span>{h.reason}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default function PlanSummary({ plan }: Props) {
  const [showRaw, setShowRaw] = useState(false);
  const [planFullScreen, setPlanFullScreen] = useState(false);
  const hasHighlights = plan.highlights && plan.highlights.length > 0;
  const totalScans = plan.scans.reduce((sum, s) => sum + s.count, 0);
  const totalJoins = plan.join_types.length;
  const recs = plan.recommendations ?? [];

  return (
    <div className="glass-card p-6">
      <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-4">Execution Plan</h2>

      <div className="flex gap-2 flex-wrap mb-4">
        {plan.has_filter_pushdown ? (
          <span className="text-[0.6rem] font-semibold uppercase tracking-wider px-2.5 py-0.5 rounded-full text-white bg-gradient-to-r from-cyan-500 to-teal-500">Filter Pushdown</span>
        ) : (
          <span className="text-[0.6rem] font-semibold uppercase tracking-wider px-2.5 py-0.5 rounded-full text-amber-300 bg-amber-500/20 ring-1 ring-amber-500/30">No Filter Pushdown</span>
        )}
        {plan.has_partition_pruning ? (
          <span className="text-[0.6rem] font-semibold uppercase tracking-wider px-2.5 py-0.5 rounded-full text-white bg-gradient-to-r from-cyan-500 to-teal-500">Partition Pruning</span>
        ) : (
          <span className="text-[0.6rem] font-semibold uppercase tracking-wider px-2.5 py-0.5 rounded-full text-amber-300 bg-amber-500/20 ring-1 ring-amber-500/30">No Partition Pruning</span>
        )}
      </div>

      {(totalScans > 0 || totalJoins > 0) && (
        <p className="text-xs text-slate-500 mb-3">
          {[
            totalScans > 0 && `${totalScans} scan${totalScans !== 1 ? "s" : ""} (${plan.scans.length} unique)`,
            totalJoins > 0 && `${totalJoins} join strateg${totalJoins !== 1 ? "ies" : "y"}`,
            recs.length > 0 && `${recs.length} warning${recs.length !== 1 ? "s" : ""}`,
          ].filter(Boolean).join(" · ")}
        </p>
      )}

      {recs.length > 0 && (
        <div className="flex flex-col gap-2 mb-4">
          {recs.map((r, i) => (
            <RecommendationCard key={i} recommendation={r} variant="compact" />
          ))}
        </div>
      )}

      {plan.join_types.length > 0 && (
        <div className="mb-3">
          <h3 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mt-3 mb-2">Join Strategies</h3>
          <div className="flex gap-1.5 flex-wrap">
            {plan.join_types.map((j) => (
              <span key={j} className="bg-white/[0.05] text-slate-300 text-xs font-medium px-2.5 py-0.5 rounded-full border border-white/[0.06]">{j}</span>
            ))}
          </div>
        </div>
      )}

      {plan.scans.length > 0 && (
        <div className="mb-3">
          <h3 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mt-3 mb-2">Scans</h3>
          <div className="flex gap-1.5 flex-wrap">
            {plan.scans.map((s) => (
              <span key={`${s.operator}-${s.format}-${s.table_name ?? ""}`} className="bg-white/[0.05] text-slate-300 text-xs font-medium px-2.5 py-0.5 rounded-full border border-white/[0.06] inline-flex items-center gap-1">
                <span>{s.operator}: {s.format}{s.table_name && <span className="text-slate-500 ml-0.5">({s.table_name.split(".").pop()})</span>}</span>
                {s.count > 1 && (
                  <span className="bg-white/[0.08] text-slate-400 text-[0.65rem] font-semibold rounded-full px-1.5 py-px leading-none">×{s.count}</span>
                )}
              </span>
            ))}
          </div>
        </div>
      )}

      <button
        className="mt-3 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3.5 py-1.5 text-[0.78rem] cursor-pointer text-blue-400 font-medium hover:bg-white/[0.06] transition-colors"
        onClick={() => setShowRaw(!showRaw)}
        aria-expanded={showRaw}
        aria-controls="plan-raw-output"
      >
        {showRaw ? "Hide" : "Show"} Raw Plan
        {hasHighlights && !showRaw && (
          <span className="ml-1.5 text-amber-400 text-[0.7rem]">({plan.highlights.length} issue{plan.highlights.length !== 1 ? "s" : ""} highlighted)</span>
        )}
      </button>
      {showRaw && (
        <>
          {hasHighlights && (
            <div className="mt-3 flex items-center gap-3 text-[0.7rem] text-slate-500">
              <span className="flex items-center gap-1">
                <span className="inline-block w-3 h-3 rounded-sm bg-rose-500/20 border border-rose-500/30" /> Critical
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-3 h-3 rounded-sm bg-amber-500/20 border border-amber-500/30" /> Warning
              </span>
              <span className="text-slate-600">Click highlighted rows for details</span>
            </div>
          )}
          <div className="relative mt-2 bg-[#0a0f1e] rounded-xl border border-white/[0.06] p-4 overflow-x-auto max-h-[500px] overflow-y-auto group" id="plan-raw-output">
            <div className="sticky top-0 float-right opacity-0 group-hover:opacity-100 transition-opacity z-10">
              <ExpandButton onClick={() => setPlanFullScreen(true)} />
            </div>
            {hasHighlights ? (
              <HighlightedPlan rawPlan={plan.raw_plan} highlights={plan.highlights} />
            ) : (
              <pre className="text-xs leading-relaxed text-slate-300"><code>{plan.raw_plan}</code></pre>
            )}
          </div>
        </>
      )}
      <FullScreenModal title="Execution Plan" open={planFullScreen} onClose={() => setPlanFullScreen(false)}>
        {hasHighlights && (
          <div className="mb-3 flex items-center gap-3 text-[0.7rem] text-slate-500">
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded-sm bg-rose-500/20 border border-rose-500/30" /> Critical
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded-sm bg-amber-500/20 border border-amber-500/30" /> Warning
            </span>
            <span className="text-slate-600">Click highlighted rows for details</span>
          </div>
        )}
        {hasHighlights ? (
          <HighlightedPlan rawPlan={plan.raw_plan} highlights={plan.highlights} />
        ) : (
          <pre className="text-sm leading-relaxed font-mono text-slate-300"><code>{plan.raw_plan}</code></pre>
        )}
      </FullScreenModal>
    </div>
  );
}
