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

const SEVERITY_GRADIENT: Record<string, string> = {
  critical: "from-rose-500 to-red-600",
  warning: "from-amber-500 to-orange-500",
  info: "from-blue-500 to-violet-500",
};

const SEVERITY_BG: Record<string, string> = {
  critical: "bg-rose-500/5",
  warning: "bg-amber-500/5",
  info: "bg-blue-500/5",
};

const BADGE_STYLES: Record<string, string> = {
  critical: "bg-rose-500/15 text-rose-400",
  warning: "bg-amber-500/15 text-amber-400",
  info: "bg-blue-500/15 text-blue-400",
};

const IMPACT_COLOR: Record<string, string> = {
  high: "#f43f5e",
  medium: "#f59e0b",
  low: "#3b82f6",
};

function ImpactGauge({ impact }: { impact: number }) {
  const r = 10;
  const circ = 2 * Math.PI * r;
  const pct = impact / 10;
  const offset = circ - pct * circ;
  const tier = impactTier(impact);
  const color = IMPACT_COLOR[tier];
  return (
    <svg width="28" height="28" viewBox="0 0 28 28" className="shrink-0">
      <circle cx="14" cy="14" r={r} fill="none" stroke="rgba(255,255,255,0.05)" strokeWidth="2.5" />
      <circle cx="14" cy="14" r={r} fill="none" stroke={color} strokeWidth="2.5" strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round" transform="rotate(-90 14 14)" className="transition-all duration-500" />
      <text x="14" y="14" textAnchor="middle" dominantBaseline="central" className="text-[7px] font-bold" style={{ fill: color }}>{impact}</text>
    </svg>
  );
}

const SQL_KEYWORDS_RE = /^\s*(ALTER|OPTIMIZE|VACUUM|ANALYZE|CREATE|CONVERT|SELECT|DROP|INSERT|UPDATE|DELETE|MERGE|WITH|SET|USE|GRANT|REVOKE|DESCRIBE|EXPLAIN|SHOW|COMPUTE|REFRESH)\b/i;

function isSqlLine(line: string): boolean { return SQL_KEYWORDS_RE.test(line); }

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
      className="inline-flex items-center gap-1 text-[0.65rem] font-medium px-2 py-0.5 rounded border border-white/[0.1] bg-white/[0.04] text-slate-400 hover:bg-white/[0.08] hover:text-slate-200 cursor-pointer transition-colors shrink-0"
    >
      {copied ? (
        <>
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" /></svg>
          Copied
        </>
      ) : (
        <>
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><rect x="9" y="9" width="13" height="13" rx="2" ry="2" /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" /></svg>
          Copy
        </>
      )}
    </button>
  );
}

function SqlBlock({ code }: { code: string }) {
  return (
    <div className="relative mt-1 group">
      <pre className="bg-[#0a0f1e] border border-white/[0.06] rounded-lg px-3 py-2 text-xs leading-relaxed overflow-x-auto whitespace-pre-wrap break-words pr-16">
        <code className="font-mono text-slate-300">{code}</code>
      </pre>
      <div className="absolute top-1.5 right-1.5"><CopyButton text={code} /></div>
    </div>
  );
}

function ActionBlock({ action }: { action: string }) {
  const lines = action.split("\n");
  const segments: { type: "text" | "sql"; content: string }[] = [];
  let currentSql: string[] = [];
  const flushSql = () => { if (currentSql.length > 0) { segments.push({ type: "sql", content: currentSql.join("\n") }); currentSql = []; } };
  for (const line of lines) {
    if (isSqlLine(line)) currentSql.push(line);
    else { flushSql(); if (line.trim()) segments.push({ type: "text", content: line }); }
  }
  flushSql();

  return (
    <div className="text-[0.78rem] text-slate-300 flex flex-col gap-1.5">
      <strong className="text-slate-400">Suggested action:</strong>
      {segments.map((seg, i) =>
        seg.type === "sql" ? <SqlBlock key={i} code={seg.content} /> : <span key={i} className="text-slate-400">{seg.content}</span>
      )}
    </div>
  );
}

function PerTableActions({ perTableActions }: { perTableActions: Record<string, string> }) {
  const entries = Object.entries(perTableActions);
  if (entries.length === 0) return null;
  return (
    <div className="mt-1.5 flex flex-col gap-2">
      <strong className="text-[0.78rem] text-slate-400">Per-table actions:</strong>
      {entries.map(([table, action]) => (
        <div key={table} className="flex flex-col gap-1">
          <span className="text-[0.72rem] font-medium text-slate-500 font-mono">{table}</span>
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
  const gradient = SEVERITY_GRADIENT[r.severity] ?? "from-slate-500 to-slate-600";

  return (
    <div className={`relative rounded-xl overflow-hidden ${SEVERITY_BG[r.severity] ?? "bg-white/[0.02]"} border border-white/[0.06]`}>
      <div className={`absolute left-0 top-0 bottom-0 w-[3px] bg-gradient-to-b ${gradient}`} />
      <button
        className="w-full flex items-center gap-2 p-3 pl-4 bg-transparent border-none cursor-pointer text-left"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span className="text-sm">{SEVERITY_ICONS[r.severity]}</span>
        <span className="font-semibold text-sm text-slate-200">{r.title}</span>
        {isFull && (
          <>
            <span className={`text-[0.65rem] font-medium px-2 py-0.5 rounded-full capitalize ${BADGE_STYLES[r.severity] ?? "bg-white/[0.05] text-slate-400"}`}>
              {CATEGORY_LABELS[r.category] ?? r.category}
            </span>
            <span className="ml-auto shrink-0 flex items-center gap-1.5">
              <ImpactGauge impact={r.impact} />
              <span className="text-[0.6rem] font-semibold uppercase tracking-tight" style={{ color: IMPACT_COLOR[tier] }}>
                {IMPACT_TIER_LABELS[tier]}
              </span>
            </span>
          </>
        )}
        <svg
          className={`w-3 h-3 text-slate-500 transition-transform duration-200 shrink-0 ${!isFull ? "ml-auto" : ""} ${expanded ? "rotate-180" : ""}`}
          fill="none" viewBox="0 0 10 6" aria-hidden="true"
        >
          <path stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M1 1l4 4 4-4" />
        </svg>
      </button>

      <div className={`overflow-hidden transition-all duration-300 ${expanded ? "max-h-[1000px] opacity-100" : "max-h-0 opacity-0"}`}>
        <div className="px-4 pb-3">
          <p className="text-[0.8rem] text-slate-400 mb-1 leading-relaxed">{r.description}</p>

          {isFull && tables.length > 0 && (
            <div className="mt-1.5 mb-1">
              <span className="text-[0.72rem] font-semibold text-slate-500">
                Affects {tables.length} {tables.length === 1 ? "table" : "tables"}:
              </span>
              <div className="flex flex-wrap gap-1 mt-1">
                {tables.map((t) => (
                  <span key={t} className="inline-block text-[0.68rem] font-mono bg-white/[0.05] text-slate-400 px-2 py-0.5 rounded border border-white/[0.06]">{t}</span>
                ))}
              </div>
            </div>
          )}

          {r.snippet && (
            <pre className="bg-[#0a0f1e] border-l-[3px] border-slate-600 px-2.5 py-1.5 my-1 rounded-lg text-xs leading-relaxed overflow-x-auto whitespace-pre-wrap break-words">
              <code className="font-mono text-slate-300">{r.snippet}</code>
            </pre>
          )}

          {isFull && (
            <>
              {r.action && !hasPerTableActions && <ActionBlock action={r.action} />}
              {r.action && hasPerTableActions && (
                <div className="text-[0.78rem] text-slate-400 mt-1"><strong>Suggested action:</strong> {r.action}</div>
              )}
              {hasPerTableActions && <PerTableActions perTableActions={perTableActions} />}
            </>
          )}

          {!isFull && (
            <>
              {scopedAction ? <SqlBlock code={scopedAction} /> : r.action ? <ActionBlock action={r.action} /> : null}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
