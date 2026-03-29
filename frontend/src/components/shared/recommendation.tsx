import { useState } from "react";
import type { Category, Recommendation, Severity } from "../../types";

export const SEVERITY_ICONS: Record<Severity, string> = {
  critical: "\u26D4",
  warning: "\u26A0\uFE0F",
  info: "\u2139\uFE0F",
};

export const CATEGORY_LABELS: Record<Category, string> = {
  query: "Query",
  execution: "Execution",
  table: "Table",
  warehouse: "Warehouse",
  storage: "Storage",
  data_modeling: "Data Modeling",
};

export type ImpactTier = "high" | "medium" | "low";

export const IMPACT_TIER_LABELS: Record<ImpactTier, string> = {
  high: "High",
  medium: "Medium",
  low: "Low",
};

export function impactTier(impact: number): ImpactTier {
  if (impact >= 7) return "high";
  if (impact >= 4) return "medium";
  return "low";
}

export const REC_CARD_STYLES: Record<string, string> = {
  critical: "bg-red-50 border-l-red-500",
  warning: "bg-amber-50 border-l-amber-500",
  info: "bg-blue-50 border-l-blue-600",
};

const BADGE_STYLES: Record<string, string> = {
  critical: "bg-red-100 text-red-800",
  warning: "bg-amber-100 text-amber-800",
  info: "bg-blue-100 text-blue-800",
};

const IMPACT_COLOR: Record<string, string> = {
  high: "text-red-600",
  medium: "text-amber-500",
  low: "text-blue-500",
};

const SQL_KEYWORDS_RE =
  /^\s*(ALTER|OPTIMIZE|VACUUM|ANALYZE|CREATE|CONVERT|SELECT|DROP|INSERT|UPDATE|DELETE|MERGE|WITH|SET|USE|GRANT|REVOKE|DESCRIBE|EXPLAIN|SHOW|COMPUTE|REFRESH)\b/i;

function isSqlLine(line: string): boolean {
  return SQL_KEYWORDS_RE.test(line);
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  return (
    <button
      onClick={handleCopy}
      className="inline-flex items-center gap-1 text-[0.65rem] font-medium px-2 py-0.5 rounded border border-gray-300 bg-white text-gray-600 hover:bg-gray-100 hover:text-gray-900 cursor-pointer transition-colors shrink-0"
    >
      {copied ? (
        <>
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
          </svg>
          Copied
        </>
      ) : (
        <>
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
            <path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
          </svg>
          Copy
        </>
      )}
    </button>
  );
}

function SqlBlock({ code }: { code: string }) {
  return (
    <div className="relative mt-1 group">
      <pre className="bg-black/5 rounded px-3 py-2 text-xs leading-relaxed overflow-x-auto whitespace-pre-wrap break-words pr-16">
        <code className="font-mono text-gray-900">{code}</code>
      </pre>
      <div className="absolute top-1.5 right-1.5">
        <CopyButton text={code} />
      </div>
    </div>
  );
}

function ActionBlock({ action }: { action: string }) {
  const lines = action.split("\n");
  const segments: { type: "text" | "sql"; content: string }[] = [];

  let currentSql: string[] = [];
  const flushSql = () => {
    if (currentSql.length > 0) {
      segments.push({ type: "sql", content: currentSql.join("\n") });
      currentSql = [];
    }
  };

  for (const line of lines) {
    if (isSqlLine(line)) {
      currentSql.push(line);
    } else {
      flushSql();
      if (line.trim()) {
        segments.push({ type: "text", content: line });
      }
    }
  }
  flushSql();

  return (
    <div className="text-[0.78rem] text-gray-900 flex flex-col gap-1.5">
      <strong>Suggested action:</strong>
      {segments.map((seg, i) =>
        seg.type === "sql" ? (
          <SqlBlock key={i} code={seg.content} />
        ) : (
          <span key={i} className="text-gray-700">{seg.content}</span>
        ),
      )}
    </div>
  );
}

function PerTableActions({ perTableActions }: { perTableActions: Record<string, string> }) {
  const entries = Object.entries(perTableActions);
  if (entries.length === 0) return null;

  return (
    <div className="mt-1.5 flex flex-col gap-2">
      <strong className="text-[0.78rem] text-gray-900">Per-table actions:</strong>
      {entries.map(([table, action]) => (
        <div key={table} className="flex flex-col gap-1">
          <span className="text-[0.72rem] font-medium text-gray-500 font-mono">{table}</span>
          <SqlBlock code={action} />
        </div>
      ))}
    </div>
  );
}

interface RecommendationCardProps {
  recommendation: Recommendation;
  variant?: "full" | "compact";
  tableName?: string;
}

export function RecommendationCard({ recommendation: r, variant = "compact", tableName }: RecommendationCardProps) {
  const [expanded, setExpanded] = useState(false);
  const tier = impactTier(r.impact);
  const tables = r.affected_tables ?? [];
  const perTableActions = r.per_table_actions ?? {};
  const hasPerTableActions = Object.keys(perTableActions).length > 0;
  const isFull = variant === "full";

  const scopedAction = tableName ? perTableActions[tableName] : null;

  return (
    <div className={`rounded-lg border-l-[3px] ${REC_CARD_STYLES[r.severity] || "bg-gray-50 border-l-gray-300"}`}>
      <button
        className="w-full flex items-center gap-1.5 p-3 bg-transparent border-none cursor-pointer text-left"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span className="text-sm">{SEVERITY_ICONS[r.severity]}</span>
        <span className="font-semibold text-sm">{r.title}</span>
        {isFull && (
          <>
            <span
              className={`text-xs font-medium px-2.5 py-0.5 rounded-full capitalize ${BADGE_STYLES[r.severity] || "bg-gray-100 text-gray-600"}`}
            >
              {CATEGORY_LABELS[r.category] ?? r.category}
            </span>
            <span
              className={`inline-flex items-center gap-1 ml-auto shrink-0 ${IMPACT_COLOR[tier]}`}
              title={`Impact: ${r.impact}/10`}
            >
              <span className="inline-flex gap-[1.5px] items-center">
                {Array.from({ length: 10 }, (_, j) => (
                  <span
                    key={j}
                    className={`inline-block w-1 h-2.5 rounded-sm ${j < r.impact ? "bg-current" : "bg-black/10"}`}
                  />
                ))}
              </span>
              <span className="text-[0.68rem] font-semibold uppercase tracking-tight">
                {IMPACT_TIER_LABELS[tier]}
              </span>
            </span>
          </>
        )}
        <svg
          className={`w-3 h-3 text-gray-400 transition-transform duration-200 shrink-0 ${!isFull ? "ml-auto" : ""} ${expanded ? "rotate-180" : ""}`}
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
        <div className="px-3 pb-3">
          <p className="text-[0.8rem] text-gray-500 mb-1 leading-relaxed">{r.description}</p>

          {isFull && tables.length > 0 && (
            <div className="mt-1.5 mb-1">
              <span className="text-[0.72rem] font-semibold text-gray-500">
                Affects {tables.length} {tables.length === 1 ? "table" : "tables"}:
              </span>
              <div className="flex flex-wrap gap-1 mt-1">
                {tables.map((t) => (
                  <span
                    key={t}
                    className="inline-block text-[0.68rem] font-mono bg-black/5 text-gray-700 px-2 py-0.5 rounded"
                  >
                    {t}
                  </span>
                ))}
              </div>
            </div>
          )}

          {r.snippet && (
            <pre className="bg-black/5 border-l-[3px] border-gray-300 px-2.5 py-1.5 my-1 rounded text-xs leading-relaxed overflow-x-auto whitespace-pre-wrap break-words">
              <code className="font-mono text-gray-900">{r.snippet}</code>
            </pre>
          )}

          {isFull && (
            <>
              {r.action && !hasPerTableActions && <ActionBlock action={r.action} />}
              {r.action && hasPerTableActions && (
                <div className="text-[0.78rem] text-gray-700 mt-1">
                  <strong>Suggested action:</strong> {r.action}
                </div>
              )}
              {hasPerTableActions && <PerTableActions perTableActions={perTableActions} />}
            </>
          )}

          {!isFull && (
            <>
              {scopedAction ? (
                <SqlBlock code={scopedAction} />
              ) : r.action ? (
                <ActionBlock action={r.action} />
              ) : null}
            </>
          )}
        </div>
      )}
    </div>
  );
}
