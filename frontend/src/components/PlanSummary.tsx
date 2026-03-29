import { useState, useMemo } from "react";
import type {
  PlanSummary as PlanSummaryType,
  PlanHighlight,
} from "../types";
import FullScreenModal, { ExpandButton } from "./FullScreenModal";

interface Props {
  plan: PlanSummaryType;
}

type HighlightMap = Map<number, PlanHighlight[]>;

function buildHighlightMap(highlights: PlanHighlight[]): HighlightMap {
  const map: HighlightMap = new Map();
  for (const h of highlights) {
    for (let i = h.line_start; i <= h.line_end; i++) {
      const existing = map.get(i);
      if (existing) {
        existing.push(h);
      } else {
        map.set(i, [h]);
      }
    }
  }
  return map;
}

function severityBg(severity: string): string {
  switch (severity) {
    case "critical":
      return "bg-red-500/20 border-l-2 border-red-400";
    case "warning":
      return "bg-yellow-500/15 border-l-2 border-yellow-400";
    default:
      return "bg-blue-500/15 border-l-2 border-blue-400";
  }
}

function severityBadgeClass(severity: string): string {
  switch (severity) {
    case "critical":
      return "bg-red-900/60 text-red-300 border border-red-500/40";
    case "warning":
      return "bg-yellow-900/60 text-yellow-300 border border-yellow-500/40";
    default:
      return "bg-blue-900/60 text-blue-300 border border-blue-500/40";
  }
}

function HighlightedPlan({
  rawPlan,
  highlights,
}: {
  rawPlan: string;
  highlights: PlanHighlight[];
}) {
  const lines = rawPlan.split("\n");
  const highlightMap = useMemo(() => buildHighlightMap(highlights), [highlights]);
  const [expandedLines, setExpandedLines] = useState<Set<number>>(new Set());

  const toggleLine = (lineIdx: number) => {
    setExpandedLines((prev) => {
      const next = new Set(prev);
      if (next.has(lineIdx)) {
        next.delete(lineIdx);
      } else {
        next.add(lineIdx);
      }
      return next;
    });
  };

  return (
    <div className="font-mono text-xs leading-relaxed">
      {lines.map((line, idx) => {
        const lineHighlights = highlightMap.get(idx);
        const isHighlighted = !!lineHighlights;
        const isFirstLineOfHighlight = lineHighlights?.some(
          (h) => h.line_start === idx
        );
        const isExpanded = expandedLines.has(idx);

        return (
          <div key={idx}>
            <div
              className={`flex items-start ${
                isHighlighted ? severityBg(lineHighlights[0].severity) : ""
              } ${isFirstLineOfHighlight ? "cursor-pointer" : ""}`}
              onClick={isFirstLineOfHighlight ? () => toggleLine(idx) : undefined}
              title={
                isFirstLineOfHighlight
                  ? "Click to see why this is highlighted"
                  : undefined
              }
            >
              <span className="select-none text-gray-600 text-right w-10 pr-3 shrink-0">
                {idx + 1}
              </span>
              <span className="flex-1 whitespace-pre">
                {line || " "}
              </span>
              {isFirstLineOfHighlight && (
                <span className="select-none shrink-0 pl-2 pr-1 text-gray-400">
                  {isExpanded ? "▾" : "▸"}
                </span>
              )}
            </div>
            {isFirstLineOfHighlight && isExpanded && lineHighlights && (
              <div className="pl-10 pr-2 py-1.5 space-y-1">
                {lineHighlights.map((h, hIdx) => (
                  <div
                    key={hIdx}
                    className={`inline-flex items-center gap-1.5 text-[0.7rem] px-2 py-0.5 rounded ${severityBadgeClass(
                      h.severity
                    )}`}
                  >
                    <span className="font-semibold uppercase tracking-wide text-[0.6rem]">
                      {h.severity}
                    </span>
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

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">Execution Plan</h2>

      <div className="flex gap-1.5 flex-wrap mb-3">
        {plan.has_filter_pushdown ? (
          <span className="bg-green-100 text-green-800 text-xs font-medium px-2.5 py-0.5 rounded-full">
            Filter Pushdown
          </span>
        ) : (
          <span className="bg-yellow-100 text-yellow-800 text-xs font-medium px-2.5 py-0.5 rounded-full">
            No Filter Pushdown
          </span>
        )}
        {plan.has_partition_pruning ? (
          <span className="bg-green-100 text-green-800 text-xs font-medium px-2.5 py-0.5 rounded-full">
            Partition Pruning
          </span>
        ) : (
          <span className="bg-yellow-100 text-yellow-800 text-xs font-medium px-2.5 py-0.5 rounded-full">
            No Partition Pruning
          </span>
        )}
      </div>

      {plan.join_types.length > 0 && (
        <div className="mb-2">
          <h3 className="text-sm font-semibold mt-3 mb-1.5">Join Strategies</h3>
          <div className="flex gap-1.5 flex-wrap">
            {plan.join_types.map((j) => (
              <span
                key={j}
                className="bg-gray-100 text-gray-600 text-xs font-medium px-2.5 py-0.5 rounded-full"
              >
                {j}
              </span>
            ))}
          </div>
        </div>
      )}

      {plan.scan_types.length > 0 && (
        <div className="mb-2">
          <h3 className="text-sm font-semibold mt-3 mb-1.5">Scans</h3>
          <div className="flex gap-1.5 flex-wrap">
            {plan.scan_types.map((s, i) => (
              <span
                key={i}
                className="bg-gray-100 text-gray-600 text-xs font-medium px-2.5 py-0.5 rounded-full"
              >
                {s}
              </span>
            ))}
          </div>
        </div>
      )}

      {plan.warnings.length > 0 && (
        <div className="mb-2">
          <h3 className="text-sm font-semibold mt-3 mb-1.5">Warnings</h3>
          <ul className="list-disc pl-5 text-amber-700 text-sm space-y-0.5">
            {plan.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      )}

      <button
        className="mt-3 border border-gray-300 rounded-lg px-3.5 py-1.5 text-[0.78rem] cursor-pointer text-blue-600 font-medium hover:bg-blue-50 transition-colors"
        onClick={() => setShowRaw(!showRaw)}
        aria-expanded={showRaw}
        aria-controls="plan-raw-output"
      >
        {showRaw ? "Hide" : "Show"} Raw Plan
        {hasHighlights && !showRaw && (
          <span className="ml-1.5 text-amber-600 text-[0.7rem]">
            ({plan.highlights.length} issue{plan.highlights.length !== 1 ? "s" : ""} highlighted)
          </span>
        )}
      </button>
      {showRaw && (
        <>
          {hasHighlights && (
            <div className="mt-3 flex items-center gap-3 text-[0.7rem] text-gray-500">
              <span className="flex items-center gap-1">
                <span className="inline-block w-3 h-3 rounded-sm bg-red-500/30 border border-red-400/50" />
                Critical
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-3 h-3 rounded-sm bg-yellow-500/30 border border-yellow-400/50" />
                Warning
              </span>
              <span className="text-gray-400">Click highlighted rows for details</span>
            </div>
          )}
          <div
            className="relative mt-2 bg-gray-900 text-gray-300 rounded-lg p-4 overflow-x-auto max-h-[500px] overflow-y-auto group"
            id="plan-raw-output"
          >
            <div className="sticky top-0 float-right opacity-0 group-hover:opacity-100 transition-opacity z-10">
              <ExpandButton onClick={() => setPlanFullScreen(true)} />
            </div>
            {hasHighlights ? (
              <HighlightedPlan rawPlan={plan.raw_plan} highlights={plan.highlights} />
            ) : (
              <pre className="text-xs leading-relaxed">
                <code>{plan.raw_plan}</code>
              </pre>
            )}
          </div>
        </>
      )}
      <FullScreenModal
        title="Execution Plan"
        open={planFullScreen}
        onClose={() => setPlanFullScreen(false)}
      >
        {hasHighlights && (
          <div className="mb-3 flex items-center gap-3 text-[0.7rem] text-gray-500">
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded-sm bg-red-500/30 border border-red-400/50" />
              Critical
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded-sm bg-yellow-500/30 border border-yellow-400/50" />
              Warning
            </span>
            <span className="text-gray-400">Click highlighted rows for details</span>
          </div>
        )}
        {hasHighlights ? (
          <HighlightedPlan rawPlan={plan.raw_plan} highlights={plan.highlights} />
        ) : (
          <pre className="text-sm leading-relaxed font-mono">
            <code>{plan.raw_plan}</code>
          </pre>
        )}
      </FullScreenModal>
    </div>
  );
}
