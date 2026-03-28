import { useState } from "react";
import type { PlanSummary as PlanSummaryType } from "../types";

interface Props {
  plan: PlanSummaryType;
}

export default function PlanSummary({ plan }: Props) {
  const [showRaw, setShowRaw] = useState(false);

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
      </button>
      {showRaw && (
        <pre
          className="mt-3 bg-gray-900 text-gray-300 rounded-lg p-4 overflow-x-auto text-xs leading-relaxed max-h-[400px] overflow-y-auto"
          id="plan-raw-output"
        >
          <code>{plan.raw_plan}</code>
        </pre>
      )}
    </div>
  );
}
