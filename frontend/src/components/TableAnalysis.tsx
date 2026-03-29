import { useState } from "react";
import type { TableInfo } from "../types";
import { humanBytes } from "../utils";
import { RecommendationCard } from "./shared/recommendation";

interface Props {
  tables: TableInfo[];
}

export default function TableAnalysis({ tables }: Props) {
  if (tables.length === 0) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h2 className="text-base font-semibold mb-3">Table Analysis</h2>
        <p className="text-gray-400 text-sm">
          No table metadata available. The query may not reference any catalog tables,
          or DESCRIBE DETAIL was not able to retrieve information.
        </p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">Table Analysis</h2>
      <div className="flex flex-col gap-2">
        {tables.map((t) => (
          <TableCard key={t.full_name} table={t} />
        ))}
      </div>
    </div>
  );
}

function TableCard({ table }: { table: TableInfo }) {
  const [expanded, setExpanded] = useState(false);
  const panelId = `table-detail-${table.full_name.replace(/[.\s]/g, "-")}`;

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      <button
        className="w-full flex items-center gap-2.5 px-3.5 py-2.5 bg-gray-50 border-none cursor-pointer text-left text-sm hover:bg-gray-100 transition-colors"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
        aria-controls={panelId}
      >
        <span className="font-semibold font-mono text-[0.8rem] min-w-0 break-all">
          {table.full_name}
        </span>
        <span className="flex gap-2 items-center ml-auto text-xs text-gray-400 shrink-0">
          {table.format && (
            <span className="bg-gray-100 text-gray-500 text-xs font-medium px-2.5 py-0.5 rounded-full">
              {table.format}
            </span>
          )}
          {table.num_files != null && <span>{table.num_files.toLocaleString()} files</span>}
          {table.size_in_bytes != null && <span>{humanBytes(table.size_in_bytes)}</span>}
        </span>
        <svg
          className={`w-3 h-3 text-gray-400 transition-transform duration-200 shrink-0 ${expanded ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 10 6"
          aria-hidden="true"
        >
          <path
            stroke="currentColor"
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth="2"
            d="M1 1l4 4 4-4"
          />
        </svg>
      </button>

      {expanded && (
        <div className="px-3.5 py-3 border-t border-gray-200 flex flex-col gap-1.5" id={panelId}>
          <div className="text-[0.8rem]">
            <strong>Clustering:</strong>{" "}
            {table.clustering_columns.length > 0
              ? table.clustering_columns.join(", ")
              : "None"}
          </div>
          <div className="text-[0.8rem]">
            <strong>Partitioning:</strong>{" "}
            {table.partition_columns.length > 0
              ? table.partition_columns.join(", ")
              : "None"}
          </div>

          {table.recommendations.length > 0 && (
            <div className="mt-1.5 flex flex-col gap-2">
              {table.recommendations.map((r, i) => (
                <RecommendationCard key={i} recommendation={r} variant="compact" tableName={table.full_name} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
