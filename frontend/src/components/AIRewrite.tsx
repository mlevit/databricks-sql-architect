import React, { useCallback, useState } from "react";
import { benchmarkQueries, rewriteQuery } from "../api";
import type { AIRewriteResult, BenchmarkResult, QueryBenchmarkStats } from "../types";

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
      className="inline-flex items-center gap-1 px-2 py-1 border border-white/20 rounded bg-white/10 text-white/70 text-[0.68rem] font-medium cursor-pointer hover:bg-white/20 hover:text-white/90 transition-colors"
      onClick={handleCopy}
      aria-label={`Copy ${label} to clipboard`}
    >
      {copied ? (
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true">
          <path d="M3 8.5L6.5 12L13 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      ) : (
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true">
          <rect x="5" y="5" width="8" height="8" rx="1.5" stroke="currentColor" strokeWidth="1.5"/>
          <path d="M3 11V3.5C3 2.67 3.67 2 4.5 2H10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
        </svg>
      )}
      {copyError ? "Failed" : copied ? "Copied" : "Copy"}
    </button>
  );
}

type DiffLine = { type: "equal" | "added" | "removed"; text: string };

function computeDiff(a: string, b: string): DiffLine[] {
  const aLines = a.split("\n");
  const bLines = b.split("\n");

  if (aLines.length > 500 || bLines.length > 500) {
    return [{ type: "equal", text: "(Query too large for inline diff)" }];
  }

  const lcs = buildLCS(aLines, bLines);
  const result: DiffLine[] = [];
  let ai = 0;
  let bi = 0;

  for (const [la, lb] of lcs) {
    while (ai < la) result.push({ type: "removed", text: aLines[ai++] });
    while (bi < lb) result.push({ type: "added", text: bLines[bi++] });
    result.push({ type: "equal", text: aLines[ai] });
    ai++;
    bi++;
  }
  while (ai < aLines.length) result.push({ type: "removed", text: aLines[ai++] });
  while (bi < bLines.length) result.push({ type: "added", text: bLines[bi++] });

  return result;
}

function buildLCS(a: string[], b: string[]): [number, number][] {
  const m = a.length;
  const n = b.length;
  const dp: number[][] = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));

  for (let i = m - 1; i >= 0; i--) {
    for (let j = n - 1; j >= 0; j--) {
      if (a[i].trim() === b[j].trim()) {
        dp[i][j] = dp[i + 1][j + 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
  }

  const pairs: [number, number][] = [];
  let i = 0;
  let j = 0;
  while (i < m && j < n) {
    if (a[i].trim() === b[j].trim()) {
      pairs.push([i, j]);
      i++;
      j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      i++;
    } else {
      j++;
    }
  }
  return pairs;
}

const DIFF_LINE_STYLES: Record<string, string> = {
  added: "bg-green-50",
  removed: "bg-red-50 line-through decoration-red-300/50",
  equal: "bg-transparent",
};

const DIFF_MARKER_STYLES: Record<string, string> = {
  added: "text-green-700",
  removed: "text-red-700",
  equal: "text-gray-400",
};

function DiffView({ original, suggested }: { original: string; suggested: string }) {
  const lines = computeDiff(original, suggested);

  if (lines.every((l) => l.type === "equal")) {
    return (
      <div className="border border-gray-200 rounded-lg overflow-hidden bg-white">
        <p className="p-3.5 text-gray-400 italic text-sm">
          No differences — the suggested query is identical.
        </p>
      </div>
    );
  }

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden bg-white">
      <pre className="m-0 p-0 font-mono text-[0.78rem] leading-relaxed overflow-x-auto">
        {lines.map((line, i) => (
          <div key={i} className={`flex min-h-[1.5em] px-2.5 ${DIFF_LINE_STYLES[line.type]}`}>
            <span className={`shrink-0 w-4 text-center select-none font-semibold ${DIFF_MARKER_STYLES[line.type]}`}>
              {line.type === "added" ? "+" : line.type === "removed" ? "\u2212" : " "}
            </span>
            <span className="whitespace-pre">{line.text || " "}</span>
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
    if (numberedMatch) {
      return (
        <div key={i} className="flex gap-1.5 py-1 items-baseline">
          <span className="shrink-0 font-semibold text-blue-600 min-w-[1.1rem]">
            {numberedMatch[1]}.
          </span>
          <span>{inlineBold(numberedMatch[2])}</span>
        </div>
      );
    }

    const bulletMatch = line.match(/^\s*[-\u2022]\s+(.*)/);
    if (bulletMatch) {
      return (
        <div key={i} className="flex gap-1.5 py-1 items-baseline">
          <span className="shrink-0 font-semibold text-blue-600 min-w-[1.1rem]">
            &bull;
          </span>
          <span>{inlineBold(bulletMatch[1])}</span>
        </div>
      );
    }

    return <p key={i}>{rendered}</p>;
  });
}

function inlineBold(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={i}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code key={i} className="bg-gray-100 px-1 py-0.5 rounded text-[0.78rem] text-gray-900">
          {part.slice(1, -1)}
        </code>
      );
    }
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

function DeltaBadge({ original, suggested, unit, lowerIsBetter = true }: {
  original: number;
  suggested: number;
  unit: MetricUnit;
  lowerIsBetter?: boolean;
}) {
  if (original === 0) return null;
  const pct = ((suggested - original) / original) * 100;
  const improved = lowerIsBetter ? pct < 0 : pct > 0;
  const absPct = Math.abs(pct);

  if (absPct < 0.5) {
    return <span className="text-xs text-gray-400 ml-1">(no change)</span>;
  }

  const diff = Math.abs(suggested - original);
  const label = unit === "ms" ? formatMs(diff)
    : unit === "bytes" ? formatBytes(diff)
    : `${diff.toLocaleString()}`;

  return (
    <span className={`text-xs font-medium ml-1.5 ${improved ? "text-green-600" : "text-red-600"}`}>
      {improved ? "\u25BC" : "\u25B2"} {label} ({absPct.toFixed(1)}%)
    </span>
  );
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

interface MetricRowDef {
  label: string;
  oVal: number | boolean | null | undefined;
  sVal: number | boolean | null | undefined;
  unit: MetricUnit;
  lowerIsBetter?: boolean;
}

function MetricRow({ label, oVal, sVal, unit, lowerIsBetter = true }: MetricRowDef) {
  if (oVal == null && sVal == null) return null;

  const showDelta = unit !== "bool" && typeof oVal === "number" && typeof sVal === "number";

  return (
    <tr className="border-b border-gray-100 last:border-0">
      <td className="py-2 px-4 text-sm font-medium text-gray-600 w-1/3">{label}</td>
      <td className="py-2 px-4 text-sm text-gray-900 font-mono text-right w-1/3">{fmtValue(oVal, unit)}</td>
      <td className="py-2 px-4 text-sm text-gray-900 font-mono text-right w-1/3">
        {fmtValue(sVal, unit)}
        {showDelta && (
          <DeltaBadge original={oVal} suggested={sVal} unit={unit} lowerIsBetter={lowerIsBetter} />
        )}
      </td>
    </tr>
  );
}

function SectionHeader({ title }: { title: string }) {
  return (
    <tr className="bg-gray-50/60">
      <td colSpan={3} className="py-1.5 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
        {title}
      </td>
    </tr>
  );
}

function buildMetricRows(
  original: QueryBenchmarkStats,
  suggested: QueryBenchmarkStats,
): { section: string; rows: MetricRowDef[] }[] {
  const om = original.metrics;
  const sm = suggested.metrics;

  const sections: { section: string; rows: MetricRowDef[] }[] = [
    {
      section: "Timing",
      rows: [
        { label: "Wall Clock Time", oVal: original.elapsed_ms, sVal: suggested.elapsed_ms, unit: "ms" },
        ...(om || sm ? [
          { label: "Total Duration", oVal: om?.total_duration_ms, sVal: sm?.total_duration_ms, unit: "ms" as MetricUnit },
          { label: "Compilation", oVal: om?.compilation_duration_ms, sVal: sm?.compilation_duration_ms, unit: "ms" as MetricUnit },
          { label: "Execution", oVal: om?.execution_duration_ms, sVal: sm?.execution_duration_ms, unit: "ms" as MetricUnit },
          { label: "Result Fetch", oVal: om?.result_fetch_duration_ms, sVal: sm?.result_fetch_duration_ms, unit: "ms" as MetricUnit },
          { label: "Total Task Time", oVal: om?.total_task_duration_ms, sVal: sm?.total_task_duration_ms, unit: "ms" as MetricUnit },
        ] : []),
      ],
    },
    {
      section: "I/O",
      rows: [
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
      ],
    },
    ...(om || sm ? [{
      section: "Resources",
      rows: [
        { label: "Spilled to Disk", oVal: om?.spilled_local_bytes, sVal: sm?.spilled_local_bytes, unit: "bytes" as MetricUnit },
        { label: "Shuffle Read", oVal: om?.shuffle_read_bytes, sVal: sm?.shuffle_read_bytes, unit: "bytes" as MetricUnit },
        { label: "Result Cached", oVal: om?.from_result_cache, sVal: sm?.from_result_cache, unit: "bool" as MetricUnit },
      ],
    }] : []),
  ];

  return sections.map((s) => ({
    ...s,
    rows: s.rows.filter((r) => r.oVal != null || r.sVal != null),
  })).filter((s) => s.rows.length > 0);
}

function BenchmarkDisplay({ benchmark }: { benchmark: BenchmarkResult }) {
  const { original, suggested } = benchmark;

  const origFailed = original.status === "FAILED";
  const sugFailed = suggested.status === "FAILED";

  if (origFailed || sugFailed) {
    return (
      <div className="mt-4 p-4 border border-red-200 rounded-lg bg-red-50">
        <h3 className="text-sm font-semibold mb-2 text-red-800">Benchmark Errors</h3>
        {origFailed && (
          <p className="text-sm text-red-700">
            <strong>Original query:</strong> {original.error || "Execution failed"}
          </p>
        )}
        {sugFailed && (
          <p className="text-sm text-red-700 mt-1">
            <strong>Suggested query:</strong> {suggested.error || "Execution failed"}
          </p>
        )}
      </div>
    );
  }

  const timeDiff = original.elapsed_ms > 0
    ? ((original.elapsed_ms - suggested.elapsed_ms) / original.elapsed_ms) * 100
    : 0;
  const improved = timeDiff > 0;
  const sections = buildMetricRows(original, suggested);

  return (
    <div className="mt-4">
      <h3 className="text-sm font-semibold mb-3">Benchmark Results</h3>

      {Math.abs(timeDiff) >= 0.5 && (
        <div className={`flex items-center gap-2 p-3 mb-3 rounded-lg border text-sm ${
          improved
            ? "bg-green-50 border-green-200 text-green-800"
            : "bg-amber-50 border-amber-200 text-amber-800"
        }`}>
          <span className="text-lg">{improved ? "\u26A1" : "\u26A0\uFE0F"}</span>
          <span>
            {improved
              ? `Suggested query was ${timeDiff.toFixed(1)}% faster (${formatMs(original.elapsed_ms - suggested.elapsed_ms)} saved)`
              : `Suggested query was ${Math.abs(timeDiff).toFixed(1)}% slower (${formatMs(suggested.elapsed_ms - original.elapsed_ms)} slower)`
            }
          </span>
        </div>
      )}

      <div className="border border-gray-200 rounded-lg overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              <th className="py-2 px-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider w-1/3">Metric</th>
              <th className="py-2 px-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider w-1/3">Original</th>
              <th className="py-2 px-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider w-1/3">Suggested</th>
            </tr>
          </thead>
          <tbody>
            {sections.map((section) => (
              <React.Fragment key={section.section}>
                <SectionHeader title={section.section} />
                {section.rows.map((row) => (
                  <MetricRow key={row.label} {...row} />
                ))}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-xs text-gray-400 mt-2">
        Wall clock time is measured client-side. All other metrics are from the Databricks Query History API. Results may vary with warehouse load.
      </p>
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
  const [customInstruction, setCustomInstruction] = useState("");

  const handleRewrite = async () => {
    setLoading(true);
    setError(null);
    setBenchmark(null);
    setBenchError(null);
    try {
      const instruction = customInstruction.trim() || undefined;
      const data = await rewriteQuery(statementId, instruction);
      setResult(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Rewrite failed");
    } finally {
      setLoading(false);
    }
  };

  const handleBenchmark = async () => {
    if (!result) return;
    setBenchLoading(true);
    setBenchError(null);
    try {
      const data = await benchmarkQueries(result.original_sql, result.suggested_sql, warehouseId);
      setBenchmark(data);
    } catch (err: unknown) {
      setBenchError(err instanceof Error ? err.message : "Benchmark failed");
    } finally {
      setBenchLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">AI Query Rewrite</h2>
      <p className="text-gray-400 mb-3 text-sm">
        Use Claude to analyze the query and suggest an optimized version based on
        the identified issues.
      </p>

      {!result && (
        <div className="flex flex-col gap-3">
          <div>
            <label htmlFor="custom-instruction" className="block text-sm font-medium text-gray-600 mb-1">
              Custom instruction <span className="text-gray-400 font-normal">(optional)</span>
            </label>
            <textarea
              id="custom-instruction"
              className="w-full rounded-lg border border-gray-300 bg-gray-50 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-purple-500 focus:ring-2 focus:ring-purple-200 focus:outline-none transition-colors resize-y"
              rows={2}
              placeholder="e.g. Avoid using subqueries, prefer CTEs instead"
              value={customInstruction}
              onChange={(e) => setCustomInstruction(e.target.value)}
              disabled={loading}
            />
          </div>
          <div>
            <button
              className="text-white bg-purple-700 hover:bg-purple-800 focus:ring-4 focus:ring-purple-300 font-medium rounded-lg text-sm px-5 py-2 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              onClick={handleRewrite}
              disabled={loading}
            >
              {loading ? "Generating..." : "Generate AI Rewrite"}
            </button>
          </div>
        </div>
      )}

      {error && (
        <p className="text-red-700 mt-3 text-sm" role="alert">{error}</p>
      )}

      {result && (
        <div>
          {!result.syntax_valid && (
            <div
              className="flex flex-col gap-1 p-3 mb-3 text-sm text-yellow-800 border border-yellow-300 rounded-lg bg-yellow-50 leading-relaxed"
              role="alert"
            >
              <span>
                <strong>Syntax warning:</strong> The suggested SQL may contain syntax
                errors and should be reviewed before use.
              </span>
              {result.syntax_errors.length > 0 && (
                <ul className="mt-1 ml-5 p-0 font-mono text-[0.8rem]">
                  {result.syntax_errors.map((e, i) => (
                    <li key={i}>{e}</li>
                  ))}
                </ul>
              )}
            </div>
          )}

          <div className="mb-3">
            <h3 className="text-sm font-semibold mt-3 mb-1.5">Explanation</h3>
            <div className="text-gray-500 text-sm leading-relaxed [&_p]:my-1">
              {formatExplanation(result.explanation)}
            </div>
          </div>

          <div className="mt-3 mb-1">
            <h3 className="text-sm font-semibold mb-1.5">Diff</h3>
            <DiffView original={result.original_sql} suggested={result.suggested_sql} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3 mb-3">
            <div>
              <h3 className="text-sm font-semibold mb-1.5">Original</h3>
              <div className="relative">
                <div className="absolute top-2 right-2 z-10">
                  <CopyButton text={result.original_sql} label="original query" />
                </div>
                <pre className="bg-gray-900 text-gray-300 rounded-lg p-4 pt-10 text-[0.78rem] leading-relaxed overflow-x-auto whitespace-pre-wrap break-words">
                  <code>{result.original_sql}</code>
                </pre>
              </div>
            </div>
            <div>
              <h3 className="text-sm font-semibold mb-1.5">Suggested</h3>
              <div className="relative">
                <div className="absolute top-2 right-2 z-10">
                  <CopyButton text={result.suggested_sql} label="suggested query" />
                </div>
                <pre className="bg-gray-900 text-gray-300 rounded-lg p-4 pt-10 text-[0.78rem] leading-relaxed overflow-x-auto whitespace-pre-wrap break-words">
                  <code>{result.suggested_sql}</code>
                </pre>
              </div>
            </div>
          </div>

          <div className="mb-3">
            <label htmlFor="custom-instruction-regen" className="block text-sm font-medium text-gray-600 mb-1">
              Custom instruction <span className="text-gray-400 font-normal">(optional)</span>
            </label>
            <textarea
              id="custom-instruction-regen"
              className="w-full rounded-lg border border-gray-300 bg-gray-50 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-purple-500 focus:ring-2 focus:ring-purple-200 focus:outline-none transition-colors resize-y"
              rows={2}
              placeholder="e.g. Avoid using subqueries, prefer CTEs instead"
              value={customInstruction}
              onChange={(e) => setCustomInstruction(e.target.value)}
              disabled={loading || benchLoading}
            />
          </div>

          <div className="flex gap-3 flex-wrap">
            <button
              className="text-white bg-purple-700 hover:bg-purple-800 focus:ring-4 focus:ring-purple-300 font-medium rounded-lg text-sm px-5 py-2 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              onClick={handleRewrite}
              disabled={loading || benchLoading}
            >
              {loading ? "Regenerating..." : "Regenerate"}
            </button>
            <button
              className="text-white bg-green-700 hover:bg-green-800 focus:ring-4 focus:ring-green-300 font-medium rounded-lg text-sm px-5 py-2 transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-2"
              onClick={handleBenchmark}
              disabled={benchLoading || loading}
            >
              {benchLoading ? (
                <>
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                  Running Benchmark...
                </>
              ) : (
                <>
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true">
                    <path d="M4 2L13 8L4 14V2Z" fill="currentColor" />
                  </svg>
                  Run Benchmark
                </>
              )}
            </button>
          </div>

          {benchError && (
            <p className="text-red-700 mt-3 text-sm" role="alert">{benchError}</p>
          )}

          {benchmark && <BenchmarkDisplay benchmark={benchmark} />}
        </div>
      )}
    </div>
  );
}
