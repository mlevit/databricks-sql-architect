import { useMemo, useState } from "react";
import type { Category, Severity } from "../types";
import {
  type ImpactTier,
  CATEGORY_LABELS,
  IMPACT_TIER_LABELS,
  SEVERITY_ICONS,
  RecommendationCard,
  impactTier,
} from "./shared/recommendation";
import type { Recommendation } from "../types";

interface Props {
  recommendations: Recommendation[];
}

const SEVERITY_ORDER: Severity[] = ["critical", "warning", "info"];

function toggleInSet<T>(set: Set<T>, value: T): Set<T> {
  const next = new Set(set);
  if (next.has(value)) next.delete(value);
  else next.add(value);
  return next;
}

const CHIP_ACTIVE_SEVERITY: Record<string, string> = {
  critical: "bg-gradient-to-r from-rose-500/20 to-red-500/20 border-rose-500/40 text-rose-300",
  warning: "bg-gradient-to-r from-amber-500/20 to-orange-500/20 border-amber-500/40 text-amber-300",
  info: "bg-gradient-to-r from-blue-500/20 to-violet-500/20 border-blue-500/40 text-blue-300",
};

const CHIP_ACTIVE_IMPACT: Record<string, string> = {
  high: "bg-gradient-to-r from-rose-500/20 to-red-500/20 border-rose-500/40 text-rose-300",
  medium: "bg-gradient-to-r from-amber-500/20 to-orange-500/20 border-amber-500/40 text-amber-300",
  low: "bg-gradient-to-r from-blue-500/20 to-violet-500/20 border-blue-500/40 text-blue-300",
};

export default function Recommendations({ recommendations }: Props) {
  const [severityFilter, setSeverityFilter] = useState<Set<Severity>>(new Set());
  const [categoryFilter, setCategoryFilter] = useState<Set<Category>>(new Set());
  const [impactFilter, setImpactFilter] = useState<Set<ImpactTier>>(new Set());

  const presentSeverities = useMemo(() => new Set(recommendations.map((r) => r.severity)), [recommendations]);
  const presentCategories = useMemo(() => new Set(recommendations.map((r) => r.category)), [recommendations]);
  const presentImpactTiers = useMemo(() => new Set(recommendations.map((r) => impactTier(r.impact))), [recommendations]);

  const filtered = useMemo(() => {
    return recommendations.filter((r) => {
      if (severityFilter.size > 0 && !severityFilter.has(r.severity)) return false;
      if (categoryFilter.size > 0 && !categoryFilter.has(r.category)) return false;
      if (impactFilter.size > 0 && !impactFilter.has(impactTier(r.impact))) return false;
      return true;
    });
  }, [recommendations, severityFilter, categoryFilter, impactFilter]);

  const hasActiveFilters = severityFilter.size > 0 || categoryFilter.size > 0 || impactFilter.size > 0;

  if (recommendations.length === 0) {
    return (
      <div className="glass-card p-6">
        <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-2">Recommendations</h2>
        <p className="text-cyan-400 font-medium text-sm">No issues detected. Your query looks good!</p>
      </div>
    );
  }

  return (
    <div className="glass-card p-6">
      <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-3">
        Recommendations{" "}
        <span className="bg-gradient-to-r from-rose-500 to-red-600 text-white text-[0.6rem] font-semibold px-2 py-0.5 rounded-full align-middle ml-1">
          {hasActiveFilters ? `${filtered.length} / ${recommendations.length}` : recommendations.length}
        </span>
      </h2>

      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 py-3 mb-3 border-b border-white/[0.06]">
        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider mr-0.5">Severity</span>
          {SEVERITY_ORDER.filter((s) => presentSeverities.has(s)).map((s) => (
            <button
              key={s}
              className={`inline-flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded-full border cursor-pointer capitalize transition-all ${
                severityFilter.has(s)
                  ? CHIP_ACTIVE_SEVERITY[s]
                  : "border-white/[0.08] bg-white/[0.03] text-slate-500 hover:border-white/[0.15] hover:text-slate-300"
              }`}
              onClick={() => setSeverityFilter((prev) => toggleInSet(prev, s))}
            >
              {SEVERITY_ICONS[s]} {s}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider mr-0.5">Category</span>
          {(Object.keys(CATEGORY_LABELS) as Category[]).filter((c) => presentCategories.has(c)).map((c) => (
            <button
              key={c}
              className={`inline-flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded-full border cursor-pointer transition-all ${
                categoryFilter.has(c)
                  ? "bg-gradient-to-r from-blue-500/20 to-violet-500/20 border-blue-500/40 text-blue-300"
                  : "border-white/[0.08] bg-white/[0.03] text-slate-500 hover:border-white/[0.15] hover:text-slate-300"
              }`}
              onClick={() => setCategoryFilter((prev) => toggleInSet(prev, c))}
            >
              {CATEGORY_LABELS[c]}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider mr-0.5">Impact</span>
          {(["high", "medium", "low"] as ImpactTier[]).filter((t) => presentImpactTiers.has(t)).map((t) => (
            <button
              key={t}
              className={`inline-flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded-full border cursor-pointer transition-all ${
                impactFilter.has(t)
                  ? CHIP_ACTIVE_IMPACT[t]
                  : "border-white/[0.08] bg-white/[0.03] text-slate-500 hover:border-white/[0.15] hover:text-slate-300"
              }`}
              onClick={() => setImpactFilter((prev) => toggleInSet(prev, t))}
            >
              {IMPACT_TIER_LABELS[t]}
            </button>
          ))}
        </div>

        {hasActiveFilters && (
          <button
            className="text-[0.7rem] font-medium text-blue-400 bg-transparent border-none cursor-pointer px-1.5 py-1 rounded hover:bg-white/[0.04] ml-auto"
            onClick={() => { setSeverityFilter(new Set()); setCategoryFilter(new Set()); setImpactFilter(new Set()); }}
          >
            Clear filters
          </button>
        )}
      </div>

      <div className="flex flex-col gap-2">
        {filtered.length === 0 ? (
          <p className="text-cyan-400 font-medium text-sm">No recommendations match the current filters.</p>
        ) : (
          filtered.map((r, i) => <RecommendationCard key={i} recommendation={r} variant="full" />)
        )}
      </div>
    </div>
  );
}
