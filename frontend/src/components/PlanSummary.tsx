import { useState } from "react";
import type { PlanSummary as PlanSummaryType } from "../types";

interface Props {
  plan: PlanSummaryType;
}

export default function PlanSummary({ plan }: Props) {
  const [showRaw, setShowRaw] = useState(false);

  return (
    <div className="panel plan-summary">
      <h2>Execution Plan</h2>

      <div className="plan-summary__chips">
        {plan.has_filter_pushdown && (
          <span className="badge badge--success">Filter Pushdown</span>
        )}
        {!plan.has_filter_pushdown && (
          <span className="badge badge--warning">No Filter Pushdown</span>
        )}
        {plan.has_partition_pruning && (
          <span className="badge badge--success">Partition Pruning</span>
        )}
      </div>

      {plan.join_types.length > 0 && (
        <div className="plan-summary__section">
          <h3>Join Strategies</h3>
          <div className="plan-summary__tags">
            {plan.join_types.map((j) => (
              <span key={j} className="badge badge--neutral">
                {j}
              </span>
            ))}
          </div>
        </div>
      )}

      {plan.scan_types.length > 0 && (
        <div className="plan-summary__section">
          <h3>Scans</h3>
          <div className="plan-summary__tags">
            {plan.scan_types.map((s, i) => (
              <span key={i} className="badge badge--neutral">
                {s}
              </span>
            ))}
          </div>
        </div>
      )}

      {plan.warnings.length > 0 && (
        <div className="plan-summary__section">
          <h3>Warnings</h3>
          <ul className="plan-summary__warnings">
            {plan.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      )}

      <button
        className="plan-summary__toggle"
        onClick={() => setShowRaw(!showRaw)}
      >
        {showRaw ? "Hide" : "Show"} Raw Plan
      </button>
      {showRaw && (
        <pre className="plan-summary__raw">
          <code>{plan.raw_plan}</code>
        </pre>
      )}
    </div>
  );
}
