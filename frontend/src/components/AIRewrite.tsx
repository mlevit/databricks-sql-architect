import React, { useCallback, useEffect, useMemo, useState } from "react";
import { type BenchmarkProgress, benchmarkQueriesPoll, cancelBenchmarkQuery, rewriteQuery } from "../api";
import type { AIRewriteResult, BenchmarkResult, QueryBenchmarkStats } from "../types";

function extractParameters(sql: string): string[] {
  const regex = /(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)/g;
  const params = new Set<string>();
  let match;
  while ((match = regex.exec(sql)) !== null) {
    params.add(match[1]);
  }
  return Array.from(params).sort();
}

interface Props {
  statementId: string;
  warehouseId?: string;
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState(false);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setCopyError(false);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      setCopyError(true);
      setTimeout(() => setCopyError(false), 2000);
    }
  }, [text]);

  return (
    <button
      className="inline-flex items-center gap-1 px-2 py-1 border border-white/[0.1] rounded bg-white/[0.04] text-slate-400 text-[0.68rem] font-medium cursor-pointer hover:bg-white/[0.08] hover:text-slate-200 transition-colors"
      onClick={handleCopy}
      aria-label={`Copy ${label} to clipboard`}
    >
      {copied ? (
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M3 8.5L6.5 12L13 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/></svg>
      ) : (
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><rect x="5" y="5" width="8" height="8" rx="1.5" stroke="currentColor" strokeWidth="1.5"/><path d="M3 11V3.5C3 2.67 3.67 2 4.5 2H10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/></svg>
      )}
      {copyError ? "Failed" : copied ? "Copied" : "Copy"}
    </button>
  );
}

type DiffLine = { type: "equal" | "added" | "removed"; text: string };

function computeDiff(a: string, b: string): DiffLine[] {
  const aLines = a.split("\n"); const bLines = b.split("\n");
  if (aLines.length > 500 || bLines.length > 500) return [{ type: "equal", text: "(Query too large for inline diff)" }];
  const lcs = buildLCS(aLines, bLines);
  const result: DiffLine[] = [];
  let ai = 0; let bi = 0;
  for (const [la, lb] of lcs) {
    while (ai < la) result.push({ type: "removed", text: aLines[ai++] });
    while (bi < lb) result.push({ type: "added", text: bLines[bi++] });
    result.push({ type: "equal", text: aLines[ai] }); ai++; bi++;
  }
  while (ai < aLines.length) result.push({ type: "removed", text: aLines[ai++] });
  while (bi < bLines.length) result.push({ type: "added", text: bLines[bi++] });
  return result;
}

function buildLCS(a: string[], b: string[]): [number, number][] {
  const m = a.length; const n = b.length;
  const dp: number[][] = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));
  for (let i = m - 1; i >= 0; i--) for (let j = n - 1; j >= 0; j--) { if (a[i].trim() === b[j].trim()) dp[i][j] = dp[i + 1][j + 1] + 1; else dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1]); }
  const pairs: [number, number][] = []; let i = 0; let j = 0;
  while (i < m && j < n) { if (a[i].trim() === b[j].trim()) { pairs.push([i, j]); i++; j++; } else if (dp[i + 1][j] >= dp[i][j + 1]) i++; else j++; }
  return pairs;
}

const DIFF_LINE_STYLES: Record<string, string> = {
  added: "bg-emerald-500/10 border-l-2 border-emerald-500",
  removed: "bg-rose-500/10 border-l-2 border-rose-500 line-through decoration-rose-500/30",
  equal: "bg-transparent border-l-2 border-transparent",
};

const DIFF_MARKER_STYLES: Record<string, string> = {
  added: "text-emerald-400",
  removed: "text-rose-400",
  equal: "text-slate-600",
};

function DiffView({ original, suggested }: { original: string; suggested: string }) {
  const lines = computeDiff(original, suggested);
  if (lines.every((l) => l.type === "equal")) {
    return (
      <div className="glass-card overflow-hidden"><p className="p-3.5 text-slate-500 italic text-sm">No differences — the suggested query is identical.</p></div>
    );
  }
  return (
    <div className="glass-card overflow-hidden">
      <pre className="m-0 p-0 font-mono text-[0.78rem] leading-relaxed overflow-x-auto">
        {lines.map((line, i) => (
          <div key={i} className={`flex min-h-[1.5em] px-2.5 ${DIFF_LINE_STYLES[line.type]}`}>
            <span className={`shrink-0 w-4 text-center select-none font-semibold ${DIFF_MARKER_STYLES[line.type]}`}>
              {line.type === "added" ? "+" : line.type === "removed" ? "\u2212" : " "}
            </span>
            <span className="whitespace-pre text-slate-300">{line.text || " "}</span>
          </div>
        ))}
      </pre>
    </div>
  );
}

function formatExplanation(text: string) {
  const lines = text.split("\n").filter((l) => l.trim());
  return lines.map((line, i) => {
    const rendered = inlineBold(line.trim());
    const numberedMatch = line.match(/^\s*(\d+)\.\s+(.*)/);
    if (numberedMatch) return (<div key={i} className="flex gap-1.5 py-1 items-baseline"><span className="shrink-0 font-semibold text-blue-400 min-w-[1.1rem]">{numberedMatch[1]}.</span><span>{inlineBold(numberedMatch[2])}</span></div>);
    const bulletMatch = line.match(/^\s*[-\u2022]\s+(.*)/);
    if (bulletMatch) return (<div key={i} className="flex gap-1.5 py-1 items-baseline"><span className="shrink-0 font-semibold text-blue-400 min-w-[1.1rem]">&bull;</span><span>{inlineBold(bulletMatch[1])}</span></div>);
    return <p key={i}>{rendered}</p>;
  });
}

function inlineBold(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**")) return <strong key={i}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("`") && part.endsWith("`")) return <code key={i} className="bg-white/[0.06] px-1 py-0.5 rounded text-[0.78rem] text-slate-200">{part.slice(1, -1)}</code>;
    return part;
  });
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

type MetricUnit = "ms" | "bytes" | "count" | "pct" | "bool";

function DeltaBadge({ original, suggested, unit, lowerIsBetter = true }: { original: number; suggested: number; unit: MetricUnit; lowerIsBetter?: boolean }) {
  if (original === 0) return null;
  const pct = ((suggested - original) / original) * 100;
  const improved = lowerIsBetter ? pct < 0 : pct > 0;
  const absPct = Math.abs(pct);
  if (absPct < 0.5) return <span className="text-xs text-slate-500 ml-1">(no change)</span>;
  const diff = Math.abs(suggested - original);
  const label = unit === "ms" ? formatMs(diff) : unit === "bytes" ? formatBytes(diff) : `${diff.toLocaleString()}`;
  return <span className={`text-xs font-medium ml-1.5 ${improved ? "text-cyan-400" : "text-rose-400"}`}>{improved ? "\u25BC" : "\u25B2"} {label} ({absPct.toFixed(1)}%)</span>;
}

function fmtValue(v: number | boolean | null | undefined, unit: MetricUnit): string {
  if (v == null) return "—";
  if (unit === "bool") return v ? "Yes" : "No";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (unit === "ms") return formatMs(v);
  if (unit === "bytes") return formatBytes(v);
  if (unit === "pct") return `${v}%`;
  return v.toLocaleString();
}

interface MetricRowDef { label: string; oVal: number | boolean | null | undefined; sVal: number | boolean | null | undefined; unit: MetricUnit; lowerIsBetter?: boolean; }

function MetricRow({ label, oVal, sVal, unit, lowerIsBetter = true }: MetricRowDef) {
  if (oVal == null && sVal == null) return null;
  const showDelta = unit !== "bool" && typeof oVal === "number" && typeof sVal === "number";
  return (
    <tr className="border-b border-white/[0.04] last:border-0 hover:bg-white/[0.02] transition-colors">
      <td className="py-2 px-4 text-sm font-medium text-slate-400 w-1/3">{label}</td>
      <td className="py-2 px-4 text-sm text-slate-200 font-mono text-right w-1/3">{fmtValue(oVal, unit)}</td>
      <td className="py-2 px-4 text-sm text-slate-200 font-mono text-right w-1/3">
        {fmtValue(sVal, unit)}
        {showDelta && <DeltaBadge original={oVal} suggested={sVal} unit={unit} lowerIsBetter={lowerIsBetter} />}
      </td>
    </tr>
  );
}

function SectionHeader({ title }: { title: string }) {
  return (
    <tr className="bg-white/[0.02]">
      <td colSpan={3} className="py-1.5 px-4 text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider bg-gradient-to-r from-blue-500/5 to-transparent">{title}</td>
    </tr>
  );
}

function buildMetricRows(original: QueryBenchmarkStats, suggested: QueryBenchmarkStats): { section: string; rows: MetricRowDef[] }[] {
  const om = original.metrics; const sm = suggested.metrics;
  const sections: { section: string; rows: MetricRowDef[] }[] = [
    { section: "Timing", rows: [
      { label: "Wall Clock Time", oVal: original.elapsed_ms, sVal: suggested.elapsed_ms, unit: "ms" },
      ...(om || sm ? [
        { label: "Total Duration", oVal: om?.total_duration_ms, sVal: sm?.total_duration_ms, unit: "ms" as MetricUnit },
        { label: "Compilation", oVal: om?.compilation_duration_ms, sVal: sm?.compilation_duration_ms, unit: "ms" as MetricUnit },
        { label: "Execution", oVal: om?.execution_duration_ms, sVal: sm?.execution_duration_ms, unit: "ms" as MetricUnit },
        { label: "Result Fetch", oVal: om?.result_fetch_duration_ms, sVal: sm?.result_fetch_duration_ms, unit: "ms" as MetricUnit },
        { label: "Total Task Time", oVal: om?.total_task_duration_ms, sVal: sm?.total_task_duration_ms, unit: "ms" as MetricUnit },
      ] : []),
    ] },
    { section: "I/O", rows: [
      ...(om || sm ? [
        { label: "Bytes Read", oVal: om?.read_bytes, sVal: sm?.read_bytes, unit: "bytes" as MetricUnit },
        { label: "Rows Read", oVal: om?.read_rows, sVal: sm?.read_rows, unit: "count" as MetricUnit },
        { label: "Files Read", oVal: om?.read_files, sVal: sm?.read_files, unit: "count" as MetricUnit },
        { label: "Partitions Read", oVal: om?.read_partitions, sVal: sm?.read_partitions, unit: "count" as MetricUnit },
        { label: "Files Pruned", oVal: om?.pruned_files, sVal: sm?.pruned_files, unit: "count" as MetricUnit, lowerIsBetter: false },
        { label: "Rows Produced", oVal: om?.produced_rows, sVal: sm?.produced_rows, unit: "count" as MetricUnit },
      ] : [
        { label: "Rows Returned", oVal: original.row_count, sVal: suggested.row_count, unit: "count" as MetricUnit },
        { label: "Data Scanned", oVal: original.byte_count, sVal: suggested.byte_count, unit: "bytes" as MetricUnit },
      ]),
    ] },
    ...(om || sm ? [{ section: "Resources", rows: [
      { label: "Spilled to Disk", oVal: om?.spilled_local_bytes, sVal: sm?.spilled_local_bytes, unit: "bytes" as MetricUnit },
      { label: "Shuffle Read", oVal: om?.shuffle_read_bytes, sVal: sm?.shuffle_read_bytes, unit: "bytes" as MetricUnit },
      { label: "Result Cached", oVal: om?.from_result_cache, sVal: sm?.from_result_cache, unit: "bool" as MetricUnit },
    ] }] : []),
  ];
  return sections.map((s) => ({ ...s, rows: s.rows.filter((r) => r.oVal != null || r.sVal != null) })).filter((s) => s.rows.length > 0);
}

function BenchmarkProgressPanel({ progress, onCancel }: { progress: Record<string, BenchmarkProgress>; onCancel: (phase: "original" | "suggested") => void }) {
  const phases: { key: "original" | "suggested"; label: string }[] = [
    { key: "original", label: "Original query" },
    { key: "suggested", label: "Suggested query" },
  ];
  return (
    <div className="mt-4 glass-card overflow-hidden">
      <div className="px-4 py-2.5 border-b border-white/[0.06] bg-white/[0.02]">
        <h3 className="text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider">Benchmark Progress</h3>
      </div>
      <div className="divide-y divide-white/[0.04]">
        {phases.map(({ key, label }) => {
          const p = progress[key];
          const isDone = p?.state === "DONE"; const isCanceled = p?.state === "CANCELED";
          const isTerminal = isDone || isCanceled; const isActive = p && !isTerminal; const isWaiting = !p;
          const canCancel = isActive && p?.statement_id;
          return (
            <div key={key} className="flex items-center gap-3 px-4 py-3">
              <div className="shrink-0">
                {isDone ? (
                  <svg className="h-5 w-5 text-cyan-400" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" /></svg>
                ) : isCanceled ? (
                  <svg className="h-5 w-5 text-rose-400" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" /></svg>
                ) : isActive ? (
                  <svg className="animate-spin h-5 w-5 text-blue-400" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
                ) : (
                  <div className="h-5 w-5 rounded-full border-2 border-slate-600" />
                )}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className={`text-sm font-medium ${isWaiting ? "text-slate-500" : "text-slate-200"}`}>{label}</span>
                  {p?.state && !isWaiting && (
                    <span className={`text-xs px-1.5 py-0.5 rounded font-mono ${isDone ? "bg-cyan-500/15 text-cyan-400" : isCanceled ? "bg-rose-500/15 text-rose-400" : p.state === "FETCHING_METRICS" ? "bg-blue-500/15 text-blue-400" : "bg-amber-500/15 text-amber-400"}`}>
                      {p.state === "FETCHING_METRICS" ? "FETCHING METRICS" : p.state}
                    </span>
                  )}
                  {isWaiting && <span className="text-xs text-slate-600 italic">waiting</span>}
                </div>
                {p?.statement_id && <p className="text-xs text-slate-500 font-mono mt-0.5 truncate">{p.statement_id}</p>}
              </div>
              {p?.elapsed_ms != null && !isWaiting && <span className="text-sm font-mono text-slate-400 shrink-0 tabular-nums">{formatMs(p.elapsed_ms)}</span>}
              {canCancel && (
                <button className="shrink-0 inline-flex items-center gap-1 px-2 py-1 text-xs font-medium text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded hover:bg-rose-500/20 transition-colors cursor-pointer" onClick={() => onCancel(key)} title={`Cancel ${label.toLowerCase()}`}>
                  <svg className="h-3.5 w-3.5" viewBox="0 0 16 16" fill="currentColor"><rect x="3" y="3" width="10" height="10" rx="1.5" /></svg>
                  Stop
                </button>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function BenchmarkDisplay({ benchmark }: { benchmark: BenchmarkResult }) {
  const { original, suggested } = benchmark;
  const origFailed = original.status === "FAILED"; const sugFailed = suggested.status === "FAILED";

  if (origFailed || sugFailed) {
    return (
      <div className="mt-4 p-4 glass-card border-rose-500/20">
        <h3 className="text-sm font-semibold mb-2 text-rose-400">Benchmark Errors</h3>
        {origFailed && <p className="text-sm text-rose-300"><strong>Original query:</strong> {original.error || "Execution failed"}</p>}
        {sugFailed && <p className="text-sm text-rose-300 mt-1"><strong>Suggested query:</strong> {suggested.error || "Execution failed"}</p>}
      </div>
    );
  }

  const timeDiff = original.elapsed_ms > 0 ? ((original.elapsed_ms - suggested.elapsed_ms) / original.elapsed_ms) * 100 : 0;
  const improved = timeDiff > 0;
  const sections = buildMetricRows(original, suggested);

  return (
    <div className="mt-4">
      <h3 className="text-sm font-semibold mb-3 text-slate-200">Benchmark Results</h3>

      {Math.abs(timeDiff) >= 0.5 && (
        <div className={`flex items-center gap-2 p-3 mb-3 rounded-xl border text-sm ${improved ? "bg-cyan-500/10 border-cyan-500/20 text-cyan-300" : "bg-amber-500/10 border-amber-500/20 text-amber-300"}`}>
          <span className="text-lg">{improved ? "\u26A1" : "\u26A0\uFE0F"}</span>
          <span>{improved ? `Suggested query was ${timeDiff.toFixed(1)}% faster (${formatMs(original.elapsed_ms - suggested.elapsed_ms)} saved)` : `Suggested query was ${Math.abs(timeDiff).toFixed(1)}% slower (${formatMs(suggested.elapsed_ms - original.elapsed_ms)} slower)`}</span>
        </div>
      )}

      <div className="glass-card overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="border-b border-white/[0.06]">
              <th className="py-2 px-4 text-left text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider w-1/3">Metric</th>
              <th className="py-2 px-4 text-right text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider w-1/3">Original</th>
              <th className="py-2 px-4 text-right text-[0.6rem] font-semibold text-slate-500 uppercase tracking-wider w-1/3">Suggested</th>
            </tr>
          </thead>
          <tbody>
            {sections.map((section) => (
              <React.Fragment key={section.section}>
                <SectionHeader title={section.section} />
                {section.rows.map((row) => <MetricRow key={row.label} {...row} />)}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-xs text-slate-600 mt-2">Wall clock time is measured client-side. All other metrics are from the Databricks Query History API.</p>
    </div>
  );
}

export default function AIRewrite({ statementId, warehouseId }: Props) {
  const [result, setResult] = useState<AIRewriteResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [benchmark, setBenchmark] = useState<BenchmarkResult | null>(null);
  const [benchLoading, setBenchLoading] = useState(false);
  const [benchError, setBenchError] = useState<string | null>(null);
  const [benchProgress, setBenchProgress] = useState<Record<string, BenchmarkProgress>>({});
  const [benchmarkId, setBenchmarkId] = useState<string | null>(null);
  const [customInstruction, setCustomInstruction] = useState("");
  const [paramValues, setParamValues] = useState<Record<string, string>>({});

  const detectedParams = useMemo(() => {
    if (!result) return [];
    const fromOriginal = extractParameters(result.original_sql);
    const fromSuggested = extractParameters(result.suggested_sql);
    const merged = new Set([...fromOriginal, ...fromSuggested]);
    return Array.from(merged).sort();
  }, [result]);

  useEffect(() => {
    setParamValues((prev) => {
      const next: Record<string, string> = {};
      for (const p of detectedParams) {
        next[p] = prev[p] ?? "";
      }
      return next;
    });
  }, [detectedParams]);

  const handleRewrite = async () => {
    setLoading(true); setError(null); setBenchmark(null); setBenchError(null);
    try { const instruction = customInstruction.trim() || undefined; const data = await rewriteQuery(statementId, instruction); setResult(data); }
    catch (err: unknown) { setError(err instanceof Error ? err.message : "Rewrite failed"); }
    finally { setLoading(false); }
  };

  const hasUnfilledParams = detectedParams.length > 0 && detectedParams.some((p) => !paramValues[p]?.trim());

  const handleBenchmark = async () => {
    if (!result) return;
    const params = detectedParams.length > 0 ? paramValues : undefined;
    setBenchLoading(true); setBenchError(null); setBenchProgress({}); setBenchmarkId(null);
    await benchmarkQueriesPoll(result.original_sql, result.suggested_sql, warehouseId, params, {
      onStarted: (id) => setBenchmarkId(id),
      onProgress: (progress) => setBenchProgress(progress),
      onResult: (data) => setBenchmark(data),
      onError: (message) => setBenchError(message),
    });
    setBenchLoading(false);
  };

  const handleCancelQuery = async (phase: "original" | "suggested") => {
    if (!benchmarkId) return;
    try { await cancelBenchmarkQuery(benchmarkId, phase); } catch { /* best-effort */ }
  };

  return (
    <div className="glass-card p-6">
      <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-3">AI Query Rewrite</h2>
      <p className="text-slate-500 mb-3 text-sm">Use Claude to analyze the query and suggest an optimized version.</p>

      {!result && (
        <div className="flex flex-col gap-3">
          <div>
            <label htmlFor="custom-instruction" className="block text-sm font-medium text-slate-400 mb-1">Custom instruction <span className="text-slate-600 font-normal">(optional)</span></label>
            <textarea id="custom-instruction" className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:border-violet-500/40 focus:bg-white/[0.05] focus:outline-none transition-colors resize-y" rows={2} placeholder="e.g. Avoid using subqueries, prefer CTEs instead" value={customInstruction} onChange={(e) => setCustomInstruction(e.target.value)} disabled={loading} />
          </div>
          <div>
            <button className="text-white font-medium rounded-xl text-sm px-5 py-2.5 transition-all disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer bg-gradient-to-r from-violet-600 to-purple-600 hover:from-violet-500 hover:to-purple-500 hover:shadow-[0_0_25px_rgba(139,92,246,0.3)]" onClick={handleRewrite} disabled={loading}>
              {loading ? "Generating..." : "Generate AI Rewrite"}
            </button>
          </div>
        </div>
      )}

      {error && <p className="text-rose-400 mt-3 text-sm" role="alert">{error}</p>}

      {result && (
        <div>
          {!result.syntax_valid && (
            <div className="flex flex-col gap-1 p-3 mb-3 text-sm text-amber-300 border border-amber-500/20 rounded-xl bg-amber-500/10 leading-relaxed" role="alert">
              <span><strong>Syntax warning:</strong> The suggested SQL may contain syntax errors and should be reviewed before use.</span>
              {result.syntax_errors.length > 0 && (
                <ul className="mt-1 ml-5 p-0 font-mono text-[0.8rem]">{result.syntax_errors.map((e, i) => <li key={i}>{e}</li>)}</ul>
              )}
            </div>
          )}

          <div className="mb-3">
            <h3 className="text-sm font-semibold mt-3 mb-1.5 text-slate-300">Explanation</h3>
            <div className="text-slate-400 text-sm leading-relaxed [&_p]:my-1">{formatExplanation(result.explanation)}</div>
          </div>

          <div className="mt-3 mb-1">
            <h3 className="text-sm font-semibold mb-1.5 text-slate-300">Diff</h3>
            <DiffView original={result.original_sql} suggested={result.suggested_sql} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3 mb-3">
            <div>
              <div className="flex items-center gap-2 mb-1.5">
                <span className="text-[0.6rem] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-full text-slate-400 bg-white/[0.05]">Original</span>
              </div>
              <div className="relative">
                <div className="absolute top-2 right-2 z-10"><CopyButton text={result.original_sql} label="original query" /></div>
                <pre className="bg-[#0a0f1e] border border-white/[0.06] text-slate-300 rounded-xl p-4 pt-10 text-[0.78rem] leading-relaxed overflow-x-auto whitespace-pre-wrap break-words"><code>{result.original_sql}</code></pre>
              </div>
            </div>
            <div>
              <div className="flex items-center gap-2 mb-1.5">
                <span className="text-[0.6rem] font-semibold uppercase tracking-wider px-2 py-0.5 rounded-full text-violet-300 bg-violet-500/15">Suggested</span>
              </div>
              <div className="relative">
                <div className="absolute top-2 right-2 z-10"><CopyButton text={result.suggested_sql} label="suggested query" /></div>
                <pre className="bg-[#0a0f1e] border border-white/[0.06] text-slate-300 rounded-xl p-4 pt-10 text-[0.78rem] leading-relaxed overflow-x-auto whitespace-pre-wrap break-words"><code>{result.suggested_sql}</code></pre>
              </div>
            </div>
          </div>

          {detectedParams.length > 0 && (
            <div className="mb-3 glass-card p-4">
              <h3 className="text-[0.6rem] font-semibold uppercase tracking-wider text-slate-500 mb-2">Query Parameters</h3>
              <p className="text-xs text-slate-500 mb-3">The query contains parameters that need values before benchmarking.</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {detectedParams.map((param) => (
                  <div key={param}>
                    <label htmlFor={`param-${param}`} className="block text-sm font-medium text-slate-400 mb-1 font-mono">
                      :{param}
                    </label>
                    <input
                      id={`param-${param}`}
                      type="text"
                      className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:border-cyan-500/40 focus:bg-white/[0.05] focus:outline-none transition-colors font-mono"
                      placeholder={`Value for :${param}`}
                      value={paramValues[param] ?? ""}
                      onChange={(e) => setParamValues((prev) => ({ ...prev, [param]: e.target.value }))}
                      disabled={benchLoading}
                    />
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="mb-3">
            <label htmlFor="custom-instruction-regen" className="block text-sm font-medium text-slate-400 mb-1">Custom instruction <span className="text-slate-600 font-normal">(optional)</span></label>
            <textarea id="custom-instruction-regen" className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-sm text-slate-200 placeholder-slate-600 focus:border-violet-500/40 focus:bg-white/[0.05] focus:outline-none transition-colors resize-y" rows={2} placeholder="e.g. Avoid using subqueries, prefer CTEs instead" value={customInstruction} onChange={(e) => setCustomInstruction(e.target.value)} disabled={loading || benchLoading} />
          </div>

          <div className="flex gap-3 flex-wrap">
            <button className="text-white font-medium rounded-xl text-sm px-5 py-2.5 transition-all disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer bg-gradient-to-r from-violet-600 to-purple-600 hover:from-violet-500 hover:to-purple-500 hover:shadow-[0_0_25px_rgba(139,92,246,0.3)]" onClick={handleRewrite} disabled={loading || benchLoading}>
              {loading ? "Regenerating..." : "Regenerate"}
            </button>
            <button className="text-white font-medium rounded-xl text-sm px-5 py-2.5 transition-all disabled:opacity-30 disabled:cursor-not-allowed inline-flex items-center gap-2 cursor-pointer bg-gradient-to-r from-cyan-600 to-teal-600 hover:from-cyan-500 hover:to-teal-500 hover:shadow-[0_0_25px_rgba(34,211,238,0.3)]" onClick={handleBenchmark} disabled={benchLoading || loading || hasUnfilledParams} title={hasUnfilledParams ? "Fill in all query parameters before running" : undefined}>
              {benchLoading ? (
                <>
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
                  Running Benchmark...
                </>
              ) : (
                <>
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M4 2L13 8L4 14V2Z" fill="currentColor" /></svg>
                  Run Benchmark
                </>
              )}
            </button>
          </div>

          {benchLoading && Object.keys(benchProgress).length > 0 && <BenchmarkProgressPanel progress={benchProgress} onCancel={handleCancelQuery} />}
          {benchError && <p className="text-rose-400 mt-3 text-sm" role="alert">{benchError}</p>}
          {benchmark && <BenchmarkDisplay benchmark={benchmark} />}
        </div>
      )}
    </div>
  );
}
