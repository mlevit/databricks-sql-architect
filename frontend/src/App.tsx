import { useCallback, useEffect, useRef, useState } from "react";
import { analyzeQueryStream } from "./api";
import type { StepProgress } from "./api";
import LandingPage from "./components/LandingPage";
import ProgressStepper from "./components/ProgressStepper";
import QueryInput from "./components/QueryInput";
import PerformanceTab from "./components/PerformanceTab";
import SystemTab from "./components/SystemTab";
import OptimizationTab from "./components/OptimizationTab";
import type { AnalysisResult } from "./types";

const TABS = ["Performance", "System", "Optimization"] as const;
type Tab = (typeof TABS)[number];

const TAB_ICONS: Record<Tab, JSX.Element> = {
  Performance: (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M8 2v4l3 2" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/><circle cx="8" cy="8" r="6" stroke="currentColor" strokeWidth="1.5"/></svg>
  ),
  System: (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><rect x="3" y="2" width="10" height="4" rx="1" stroke="currentColor" strokeWidth="1.5"/><rect x="3" y="10" width="10" height="4" rx="1" stroke="currentColor" strokeWidth="1.5"/><path d="M8 6v4" stroke="currentColor" strokeWidth="1.5"/></svg>
  ),
  Optimization: (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M8 2l1.5 4H14l-3.5 2.5L12 13 8 10.5 4 13l1.5-4.5L2 6h4.5L8 2z" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round"/></svg>
  ),
};

function getInitialStatementId(): string {
  const params = new URLSearchParams(window.location.search);
  return params.get("statement_id") || "";
}

export default function App() {
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("Performance");
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
        setTab("Performance");
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
  const showLanding = !result && !loading && !error;

  if (showLanding) {
    return (
      <LandingPage
        onSubmit={handleAnalyze}
        loading={loading}
        initialValue={statementId}
      />
    );
  }

  return (
    <div className="min-h-screen bg-[#060918] text-slate-200">
      {/* Glass header */}
      <header className="backdrop-blur-xl bg-white/[0.03] border-b border-white/[0.06] px-6 py-2.5 flex items-center gap-6 sticky top-0 z-40">
        <h1
          className="text-base font-bold whitespace-nowrap cursor-pointer shrink-0 gradient-text"
          onClick={() => {
            setResult(null);
            setError(null);
            setStatementId("");
            const url = new URL(window.location.href);
            url.searchParams.delete("statement_id");
            window.history.replaceState(null, "", url.toString());
          }}
        >
          SQL Architect
        </h1>
        <QueryInput
          onSubmit={handleAnalyze}
          loading={loading}
          initialValue={statementId}
          variant="compact"
        />
      </header>

      {error && (
        <div
          className="flex items-center justify-between gap-3 max-w-5xl mx-auto mt-3 px-4 py-3 text-sm text-red-300 glass-card border-red-500/20"
          role="alert"
        >
          <span>{error}</span>
          <button
            className="bg-transparent border-none text-red-300 text-lg cursor-pointer px-1 leading-none opacity-60 hover:opacity-100 transition-opacity"
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
        <div className={`mx-auto px-6 py-5 transition-all ${tab === "Optimization" ? "max-w-[1600px]" : "max-w-5xl"}`}>
          {/* Pill tab navigation */}
          <nav
            className="flex items-center justify-center mb-6"
            role="tablist"
            aria-label="Analysis sections"
          >
            <div className="inline-flex bg-white/[0.04] rounded-full p-1 gap-1">
              {TABS.map((t) => (
                <button
                  key={t}
                  role="tab"
                  aria-selected={tab === t}
                  aria-controls={`tabpanel-${t.toLowerCase()}`}
                  className={`inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium whitespace-nowrap rounded-full cursor-pointer transition-all duration-300 ${
                    tab === t
                      ? "bg-white/[0.08] text-white shadow-[0_0_20px_rgba(59,130,246,0.15)]"
                      : "text-slate-500 hover:text-slate-300 hover:bg-white/[0.03]"
                  }`}
                  onClick={() => setTab(t)}
                >
                  {TAB_ICONS[t]}
                  {t}
                  {t === "Optimization" && recCount > 0 && (
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
                    </span>
                  )}
                </button>
              ))}
            </div>
          </nav>

          <main role="tabpanel" id={`tabpanel-${tab.toLowerCase()}`} aria-label={tab}>
            {tab === "Performance" && (
              <PerformanceTab metrics={result.query_metrics} />
            )}
            {tab === "System" && (
              <SystemTab
                warehouse={result.warehouse}
                tables={result.tables}
                planSummary={result.plan_summary}
              />
            )}
            {tab === "Optimization" && (
              <OptimizationTab
                recommendations={result.recommendations}
                statementId={statementId}
                warehouseId={result.query_metrics.warehouse_id ?? undefined}
              />
            )}
          </main>
        </div>
      )}
    </div>
  );
}
