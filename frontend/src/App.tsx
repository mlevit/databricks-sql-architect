import { useCallback, useEffect, useRef, useState } from "react";
import { analyzeQueryStream } from "./api";
import type { StepProgress } from "./api";
import AIRewrite from "./components/AIRewrite";
import MetricsCards from "./components/MetricsCards";
import PlanSummary from "./components/PlanSummary";
import ProgressStepper from "./components/ProgressStepper";
import QueryInput from "./components/QueryInput";
import QueryOverview from "./components/QueryOverview";
import Recommendations from "./components/Recommendations";
import TableAnalysis from "./components/TableAnalysis";
import WarehouseInfo from "./components/WarehouseInfo";
import type { AnalysisResult } from "./types";

const TABS = [
  "Overview",
  "Metrics",
  "Tables",
  "Plan",
  "Warehouse",
  "Recommendations",
  "AI Rewrite",
] as const;

type Tab = (typeof TABS)[number];

function getInitialStatementId(): string {
  const params = new URLSearchParams(window.location.search);
  return params.get("statement_id") || "";
}

export default function App() {
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("Overview");
  const [statementId, setStatementId] = useState(getInitialStatementId);
  const [progress, setProgress] = useState<StepProgress | null>(null);
  const autoTriggered = useRef(false);

  const handleAnalyze = useCallback(async (id: string) => {
    setLoading(true);
    setError(null);
    setResult(null);
    setProgress(null);
    setStatementId(id);

    const url = new URL(window.location.href);
    url.searchParams.set("statement_id", id);
    window.history.replaceState(null, "", url.toString());

    await analyzeQueryStream(id, {
      onProgress: (p) => setProgress(p),
      onResult: (data) => {
        setResult(data);
        setTab("Overview");
        setLoading(false);
        setProgress(null);
      },
      onError: (msg) => {
        setError(msg);
        setLoading(false);
        setProgress(null);
      },
    });
  }, []);

  useEffect(() => {
    if (autoTriggered.current) return;
    const initial = getInitialStatementId();
    if (initial) {
      autoTriggered.current = true;
      handleAnalyze(initial);
    }
  }, [handleAnalyze]);

  const recCount = result?.recommendations.length ?? 0;
  const tabPanelId = `tabpanel-${tab.replace(/\s+/g, "-").toLowerCase()}`;

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <header className="relative bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-center">
        <h1 className="absolute left-6 text-base font-semibold whitespace-nowrap text-gray-900">
          Databricks Query Analyzer
        </h1>
        <QueryInput
          onSubmit={handleAnalyze}
          loading={loading}
          initialValue={statementId}
        />
      </header>

      {error && (
        <div
          className="flex items-center justify-between gap-3 max-w-5xl mx-auto mt-3 px-4 py-3 text-sm text-red-800 border border-red-300 rounded-lg bg-red-50"
          role="alert"
        >
          <span>{error}</span>
          <button
            className="bg-transparent border-none text-red-800 text-lg cursor-pointer px-1 leading-none opacity-60 hover:opacity-100 transition-opacity"
            onClick={() => setError(null)}
            aria-label="Dismiss error"
          >
            &times;
          </button>
        </div>
      )}

      {loading && (
        <div className="max-w-5xl mx-auto px-6 py-5" aria-busy="true">
          <ProgressStepper current={progress} />
        </div>
      )}

      {result && (
        <div
          className={`mx-auto px-6 py-5 transition-all ${tab === "AI Rewrite" ? "max-w-[1600px]" : "max-w-5xl"}`}
        >
          <nav
            className="flex border-b border-gray-200 mb-4 overflow-x-auto scrollbar-hide"
            role="tablist"
            aria-label="Analysis sections"
          >
            {TABS.map((t) => (
              <button
                key={t}
                role="tab"
                aria-selected={tab === t}
                aria-controls={`tabpanel-${t.replace(/\s+/g, "-").toLowerCase()}`}
                className={`inline-flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium whitespace-nowrap border-b-2 -mb-px cursor-pointer transition-colors ${
                  tab === t
                    ? "text-blue-600 border-blue-600 font-semibold"
                    : "text-gray-500 border-transparent hover:text-gray-700 hover:border-gray-300"
                }`}
                onClick={() => setTab(t)}
              >
                {t}
                {t === "Recommendations" && recCount > 0 && (
                  <span className="bg-red-600 text-white text-xs font-semibold px-2 py-0.5 rounded-full leading-tight">
                    {recCount}
                  </span>
                )}
              </button>
            ))}
          </nav>

          <main role="tabpanel" id={tabPanelId} aria-label={tab}>
            {tab === "Overview" && (
              <QueryOverview metrics={result.query_metrics} />
            )}
            {tab === "Metrics" && (
              <MetricsCards metrics={result.query_metrics} />
            )}
            {tab === "Tables" && <TableAnalysis tables={result.tables} />}
            {tab === "Plan" &&
              (result.plan_summary ? (
                <PlanSummary plan={result.plan_summary} />
              ) : (
                <div className="bg-white rounded-lg border border-gray-200 p-5">
                  <h2 className="text-base font-semibold mb-3">Execution Plan</h2>
                  <p className="text-gray-500">
                    No execution plan available. EXPLAIN is only supported for
                    SELECT statements.
                  </p>
                </div>
              ))}
            {tab === "Warehouse" &&
              (result.warehouse ? (
                <WarehouseInfo warehouse={result.warehouse} />
              ) : (
                <div className="bg-white rounded-lg border border-gray-200 p-5">
                  <h2 className="text-base font-semibold mb-3">Warehouse</h2>
                  <p className="text-gray-500">No warehouse information available.</p>
                </div>
              ))}
            {tab === "Recommendations" && (
              <Recommendations recommendations={result.recommendations} />
            )}
            {tab === "AI Rewrite" && (
              <AIRewrite
                statementId={statementId}
                warehouseId={result.query_metrics.warehouse_id ?? undefined}
              />
            )}
          </main>
        </div>
      )}

      {!result && !loading && !error && (
        <div className="text-center py-24 px-8 text-gray-400">
          <p>Enter a statement ID above to begin analysis.</p>
        </div>
      )}
    </div>
  );
}
