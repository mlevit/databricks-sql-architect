import { useMemo, useState } from "react";
import type { Category, Recommendation, Severity } from "../types";

interface Props {
  recommendations: Recommendation[];
}

const SEVERITY_ICONS: Record<Severity, string> = {
  critical: "\u26D4",
  warning: "\u26A0\uFE0F",
  info: "\u2139\uFE0F",
};

const SEVERITY_ORDER: Severity[] = ["critical", "warning", "info"];

const CATEGORY_LABELS: Record<Category, string> = {
  query: "Query",
  execution: "Execution",
  table: "Table",
  warehouse: "Warehouse",
  storage: "Storage",
  data_modeling: "Data Modeling",
};

type ImpactTier = "high" | "medium" | "low";

const IMPACT_TIER_LABELS: Record<ImpactTier, string> = {
  high: "High",
  medium: "Medium",
  low: "Low",
};

function impactTier(impact: number): ImpactTier {
  if (impact >= 7) return "high";
  if (impact >= 4) return "medium";
  return "low";
}

function toggleInSet<T>(set: Set<T>, value: T): Set<T> {
  const next = new Set(set);
  if (next.has(value)) next.delete(value);
  else next.add(value);
  return next;
}

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
      <div className="panel recommendations">
        <h2>Recommendations</h2>
        <p className="recommendations__empty">No issues detected. Your query looks good!</p>
      </div>
    );
  }

  return (
    <div className="panel recommendations">
      <h2>
        Recommendations{" "}
        <span className="recommendations__count">
          {hasActiveFilters ? `${filtered.length} / ${recommendations.length}` : recommendations.length}
        </span>
      </h2>

      <div className="rec-filters">
        <div className="rec-filters__group">
          <span className="rec-filters__label">Severity</span>
          {SEVERITY_ORDER.filter((s) => presentSeverities.has(s)).map((s) => (
            <button
              key={s}
              className={`rec-filters__chip rec-filters__chip--${s}${severityFilter.has(s) ? " rec-filters__chip--active" : ""}`}
              onClick={() => setSeverityFilter((prev) => toggleInSet(prev, s))}
            >
              {SEVERITY_ICONS[s]} {s}
            </button>
          ))}
        </div>

        <div className="rec-filters__group">
          <span className="rec-filters__label">Category</span>
          {(Object.keys(CATEGORY_LABELS) as Category[])
            .filter((c) => presentCategories.has(c))
            .map((c) => (
              <button
                key={c}
                className={`rec-filters__chip${categoryFilter.has(c) ? " rec-filters__chip--active" : ""}`}
                onClick={() => setCategoryFilter((prev) => toggleInSet(prev, c))}
              >
                {CATEGORY_LABELS[c]}
              </button>
            ))}
        </div>

        <div className="rec-filters__group">
          <span className="rec-filters__label">Impact</span>
          {(["high", "medium", "low"] as ImpactTier[])
            .filter((t) => presentImpactTiers.has(t))
            .map((t) => (
              <button
                key={t}
                className={`rec-filters__chip rec-filters__chip--impact-${t}${impactFilter.has(t) ? " rec-filters__chip--active" : ""}`}
                onClick={() => setImpactFilter((prev) => toggleInSet(prev, t))}
              >
                {IMPACT_TIER_LABELS[t]}
              </button>
            ))}
        </div>

        {hasActiveFilters && (
          <button
            className="rec-filters__clear"
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

      <div className="recommendations__list">
        {filtered.length === 0 ? (
          <p className="recommendations__empty">No recommendations match the current filters.</p>
        ) : (
          filtered.map((r, i) => {
            const tier = impactTier(r.impact);
            return (
              <div key={i} className={`rec rec--${r.severity}`}>
                <div className="rec__header">
                  <span className="rec__icon">{SEVERITY_ICONS[r.severity]}</span>
                  <span className="rec__title">{r.title}</span>
                  <span className={`badge badge--${r.severity}`}>{r.category}</span>
                  <span className={`rec__impact rec__impact--${tier}`} title={`Impact: ${r.impact}/10`}>
                    <span className="rec__impact-bar">
                      {Array.from({ length: 10 }, (_, j) => (
                        <span key={j} className={`rec__impact-pip${j < r.impact ? " rec__impact-pip--filled" : ""}`} />
                      ))}
                    </span>
                    <span className="rec__impact-label">{IMPACT_TIER_LABELS[tier]}</span>
                  </span>
                </div>
                <p className="rec__desc">{r.description}</p>
                {r.snippet && (
                  <pre className="rec__snippet"><code>{r.snippet}</code></pre>
                )}
                {r.action && (
                  <div className="rec__action">
                    <strong>Suggested action:</strong>{" "}
                    <code>{r.action}</code>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
