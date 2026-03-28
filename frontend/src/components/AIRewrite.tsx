import { useState } from "react";
import { rewriteQuery } from "../api";
import type { AIRewriteResult } from "../types";

interface Props {
  statementId: string;
}

export default function AIRewrite({ statementId }: Props) {
  const [result, setResult] = useState<AIRewriteResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleRewrite = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await rewriteQuery(statementId);
      setResult(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Rewrite failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="panel ai-rewrite">
      <h2>AI Query Rewrite</h2>
      <p className="ai-rewrite__desc">
        Use Claude to analyze the query and suggest an optimized version based on
        the identified issues.
      </p>

      {!result && (
        <button
          className="ai-rewrite__btn"
          onClick={handleRewrite}
          disabled={loading}
        >
          {loading ? "Generating..." : "Generate AI Rewrite"}
        </button>
      )}

      {error && <p className="ai-rewrite__error">{error}</p>}

      {result && (
        <div className="ai-rewrite__result">
          <div className="ai-rewrite__columns">
            <div className="ai-rewrite__col">
              <h3>Original</h3>
              <pre><code>{result.original_sql}</code></pre>
            </div>
            <div className="ai-rewrite__col">
              <h3>Suggested</h3>
              <pre><code>{result.suggested_sql}</code></pre>
            </div>
          </div>
          <div className="ai-rewrite__explanation">
            <h3>Explanation</h3>
            <p>{result.explanation}</p>
          </div>
          <button className="ai-rewrite__btn" onClick={handleRewrite} disabled={loading}>
            {loading ? "Regenerating..." : "Regenerate"}
          </button>
        </div>
      )}
    </div>
  );
}
