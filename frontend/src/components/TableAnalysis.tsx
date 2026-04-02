import { useState } from "react";
import type { TableInfo } from "../types";
import { humanBytes, formatNumber } from "../utils";
import { RecommendationCard } from "./shared/recommendation";

interface Props {
  tables: TableInfo[];
}

export default function TableAnalysis({ tables }: Props) {
  if (tables.length === 0) {
    return (
      <div className="glass-card p-6">
        <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-2">Table Analysis</h2>
        <p className="text-slate-500 text-sm">
          No table metadata available. The query may not reference any catalog tables.
        </p>
      </div>
    );
  }

  return (
    <div className="glass-card p-6">
      <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-4">Table Analysis</h2>
      <div className="flex flex-col gap-2">
        {tables.map((t) => (
          <TableCard key={t.full_name} table={t} depth={0} />
        ))}
      </div>
    </div>
  );
}

const FORMAT_COLORS: Record<string, string> = {
  DELTA: "from-teal-500 to-cyan-500",
  PARQUET: "from-blue-500 to-indigo-500",
  CSV: "from-amber-500 to-orange-500",
  JSON: "from-violet-500 to-purple-500",
};

const TYPE_BADGE: Record<string, { label: string; className: string }> = {
  VIEW: { label: "VIEW", className: "from-fuchsia-500 to-pink-500" },
};

function isView(table: TableInfo): boolean {
  return table.table_type?.toUpperCase() === "VIEW";
}

function TableCard({ table, depth }: { table: TableInfo; depth: number }) {
  const [expanded, setExpanded] = useState(false);
  const [showDef, setShowDef] = useState(false);
  const panelId = `table-detail-${table.full_name.replace(/[.\s]/g, "-")}-${depth}`;
  const gradientClass = FORMAT_COLORS[table.format?.toUpperCase() ?? ""] ?? "from-slate-500 to-slate-600";
  const typeBadge = table.table_type ? TYPE_BADGE[table.table_type.toUpperCase()] : null;
  const hasChildren = table.underlying_tables && table.underlying_tables.length > 0;

  return (
    <div className={`border border-white/[0.06] rounded-xl overflow-hidden transition-all hover:border-white/[0.1] group ${depth > 0 ? "ml-4 border-l-2 border-l-fuchsia-500/30" : ""}`}>
      <button
        className="w-full flex items-center gap-2.5 px-4 py-3 bg-white/[0.02] border-none cursor-pointer text-left text-sm hover:bg-white/[0.04] transition-colors"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
        aria-controls={panelId}
      >
        <span className="font-semibold font-mono text-[0.8rem] text-slate-200 min-w-0 break-all">
          {table.full_name}
        </span>
        <span className="flex gap-2 items-center ml-auto text-xs text-slate-500 shrink-0">
          {typeBadge && (
            <span className={`text-[0.6rem] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-full text-white bg-gradient-to-r ${typeBadge.className}`}>
              {typeBadge.label}
            </span>
          )}
          {table.format && (
            <span className={`text-[0.6rem] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-full text-white bg-gradient-to-r ${gradientClass}`}>
              {table.format}
            </span>
          )}
          {table.num_files != null && <span>{table.num_files.toLocaleString()} files</span>}
          {table.size_in_bytes != null && <span>{humanBytes(table.size_in_bytes)}</span>}
        </span>
        <svg
          className={`w-3 h-3 text-slate-500 transition-transform duration-200 shrink-0 ${expanded ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 10 6"
          aria-hidden="true"
        >
          <path stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M1 1l4 4 4-4" />
        </svg>
      </button>

      {expanded && (
        <div className="px-4 py-3 border-t border-white/[0.06] flex flex-col gap-1.5" id={panelId}>
          {!isView(table) && (
            <>
              <div className="text-[0.8rem] text-slate-300">
                <strong className="text-slate-400">Clustering:</strong>{" "}
                {table.clustering_columns.length > 0 ? table.clustering_columns.join(", ") : "None"}
              </div>
              <div className="text-[0.8rem] text-slate-300">
                <strong className="text-slate-400">Partitioning:</strong>{" "}
                {table.partition_columns.length > 0 ? table.partition_columns.join(", ") : "None"}
              </div>
              <div className="text-[0.8rem] text-slate-300">
                <strong className="text-slate-400">Statistics:</strong>{" "}
                {table.has_cbo_stats ? (
                  <span className="text-cyan-400">
                    {table.stats_num_rows != null && <>{formatNumber(table.stats_num_rows)} rows</>}
                    {table.stats_num_rows != null && table.stats_total_size != null && " · "}
                    {table.stats_total_size != null && <>{humanBytes(table.stats_total_size)}</>}
                  </span>
                ) : (
                  <span className="text-amber-400">Not collected</span>
                )}
              </div>
            </>
          )}

          {isView(table) && table.view_definition && (
            <div className="mt-1">
              <button
                className="text-[0.75rem] text-fuchsia-400 hover:text-fuchsia-300 transition-colors cursor-pointer bg-transparent border-none p-0"
                onClick={() => setShowDef(!showDef)}
              >
                {showDef ? "Hide" : "Show"} view definition
              </button>
              {showDef && (
                <pre className="mt-1.5 p-3 rounded-lg bg-black/30 border border-white/[0.06] text-[0.7rem] text-slate-300 overflow-x-auto whitespace-pre-wrap break-all">
                  {table.view_definition}
                </pre>
              )}
            </div>
          )}

          {table.recommendations.length > 0 && (
            <div className="mt-1.5 flex flex-col gap-2">
              {table.recommendations.map((r, i) => (
                <RecommendationCard key={i} recommendation={r} variant="compact" tableName={table.full_name} />
              ))}
            </div>
          )}

          {hasChildren && (
            <div className="mt-2">
              <div className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-2">
                Underlying Tables
              </div>
              <div className="flex flex-col gap-2">
                {table.underlying_tables.map((child) => (
                  <TableCard key={child.full_name} table={child} depth={depth + 1} />
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
