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
  critical: "border-red-500 bg-red-50 text-red-700",
  warning: "border-amber-500 bg-amber-50 text-amber-700",
  info: "border-blue-500 bg-blue-50 text-blue-700",
};

const CHIP_ACTIVE_IMPACT: Record<string, string> = {
  high: "border-red-500 bg-red-50 text-red-700",
  medium: "border-amber-500 bg-amber-50 text-amber-700",
  low: "border-blue-500 bg-blue-50 text-blue-700",
};

export default function Recommendations({ recommendations }: Props) {
  const [severityFilter, setSeverityFilter] = useState<Set<Severity>>(new Set());
  const [categoryFilter, setCategoryFilter] = useState<Set<Category>>(new Set());
  const [impactFilter, setImpactFilter] = useState<Set<ImpactTier>>(new Set());

  const presentSeverities = useMemo(
    () => new Set(recommendations.map((r) => r.severity)),
    [recommendations],
  );
  const presentCategories = useMemo(
    () => new Set(recommendations.map((r) => r.category)),
    [recommendations],
  );
  const presentImpactTiers = useMemo(
    () => new Set(recommendations.map((r) => impactTier(r.impact))),
    [recommendations],
  );

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
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h2 className="text-base font-semibold mb-3">Recommendations</h2>
        <p className="text-green-700 font-medium text-sm">No issues detected. Your query looks good!</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">
        Recommendations{" "}
        <span className="bg-red-600 text-white text-[0.68rem] font-semibold px-2 py-0.5 rounded-full align-middle">
          {hasActiveFilters ? `${filtered.length} / ${recommendations.length}` : recommendations.length}
        </span>
      </h2>

      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 py-2.5 mb-1.5 border-b border-gray-200">
        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[0.7rem] font-semibold text-gray-400 uppercase tracking-wide mr-0.5">
            Severity
          </span>
          {SEVERITY_ORDER.filter((s) => presentSeverities.has(s)).map((s) => (
            <button
              key={s}
              className={`inline-flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded-full border cursor-pointer capitalize transition-all ${
                severityFilter.has(s)
                  ? CHIP_ACTIVE_SEVERITY[s]
                  : "border-gray-300 bg-white text-gray-500 hover:border-gray-400 hover:bg-gray-50"
              }`}
              onClick={() => setSeverityFilter((prev) => toggleInSet(prev, s))}
            >
              {SEVERITY_ICONS[s]} {s}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[0.7rem] font-semibold text-gray-400 uppercase tracking-wide mr-0.5">
            Category
          </span>
          {(Object.keys(CATEGORY_LABELS) as Category[])
            .filter((c) => presentCategories.has(c))
            .map((c) => (
              <button
                key={c}
                className={`inline-flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded-full border cursor-pointer transition-all ${
                  categoryFilter.has(c)
                    ? "border-blue-500 bg-blue-50 text-blue-700"
                    : "border-gray-300 bg-white text-gray-500 hover:border-gray-400 hover:bg-gray-50"
                }`}
                onClick={() => setCategoryFilter((prev) => toggleInSet(prev, c))}
              >
                {CATEGORY_LABELS[c]}
              </button>
            ))}
        </div>

        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[0.7rem] font-semibold text-gray-400 uppercase tracking-wide mr-0.5">
            Impact
          </span>
          {(["high", "medium", "low"] as ImpactTier[])
            .filter((t) => presentImpactTiers.has(t))
            .map((t) => (
              <button
                key={t}
                className={`inline-flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded-full border cursor-pointer transition-all ${
                  impactFilter.has(t)
                    ? CHIP_ACTIVE_IMPACT[t]
                    : "border-gray-300 bg-white text-gray-500 hover:border-gray-400 hover:bg-gray-50"
                }`}
                onClick={() => setImpactFilter((prev) => toggleInSet(prev, t))}
              >
                {IMPACT_TIER_LABELS[t]}
              </button>
            ))}
        </div>

        {hasActiveFilters && (
          <button
            className="text-[0.7rem] font-medium text-blue-700 bg-transparent border-none cursor-pointer px-1.5 py-1 rounded hover:bg-blue-50 ml-auto"
            onClick={() => {
              setSeverityFilter(new Set());
              setCategoryFilter(new Set());
              setImpactFilter(new Set());
            }}
          >
            Clear filters
          </button>
        )}
      </div>

      <div className="flex flex-col gap-2">
        {filtered.length === 0 ? (
          <p className="text-green-700 font-medium text-sm">No recommendations match the current filters.</p>
        ) : (
          filtered.map((r, i) => (
            <RecommendationCard key={i} recommendation={r} variant="full" />
          ))
        )}
      </div>
    </div>
  );
}
