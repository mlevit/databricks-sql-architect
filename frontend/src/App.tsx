import { useState } from "react";
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
import "./App.css";

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

export default function App() {
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("Overview");
  const [statementId, setStatementId] = useState("");
  const [progress, setProgress] = useState<StepProgress | null>(null);

  const handleAnalyze = async (id: string) => {
    setLoading(true);
    setError(null);
    setResult(null);
    setProgress(null);
    setStatementId(id);

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
  };

  const recCount = result?.recommendations.length ?? 0;

  return (
    <div className="app">
      <header className="app-header">
        <h1>Databricks Query Analyzer</h1>
        <QueryInput onSubmit={handleAnalyze} loading={loading} />
      </header>

      {error && <div className="error-banner">{error}</div>}

      {loading && (
        <div className="app-body">
          <ProgressStepper current={progress} />
        </div>
      )}

      {result && (
        <div className="app-body">
          <nav className="tabs">
            {TABS.map((t) => (
              <button
                key={t}
                className={`tabs__btn ${tab === t ? "tabs__btn--active" : ""}`}
                onClick={() => setTab(t)}
              >
                {t}
                {t === "Recommendations" && recCount > 0 && (
                  <span className="tabs__badge">{recCount}</span>
                )}
              </button>
            ))}
          </nav>

          <main className="tab-content">
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
                <div className="panel">
                  <h2>Execution Plan</h2>
                  <p>
                    No execution plan available. EXPLAIN is only supported for
                    SELECT statements.
                  </p>
                </div>
              ))}
            {tab === "Warehouse" &&
              (result.warehouse ? (
                <WarehouseInfo warehouse={result.warehouse} />
              ) : (
                <div className="panel">
                  <h2>Warehouse</h2>
                  <p>No warehouse information available.</p>
                </div>
              ))}
            {tab === "Recommendations" && (
              <Recommendations recommendations={result.recommendations} />
            )}
            {tab === "AI Rewrite" && <AIRewrite statementId={statementId} />}
          </main>
        </div>
      )}

      {!result && !loading && !error && (
        <div className="app-empty">
          <p>Enter a statement ID above to begin analysis.</p>
        </div>
      )}
    </div>
  );
}
