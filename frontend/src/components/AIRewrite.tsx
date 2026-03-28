import { useCallback, useState } from "react";
import { rewriteQuery } from "../api";
import type { AIRewriteResult } from "../types";

interface Props {
  statementId: string;
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [text]);

  return (
    <button className="copy-btn" onClick={handleCopy} title="Copy to clipboard">
      {copied ? (
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
          <path d="M3 8.5L6.5 12L13 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      ) : (
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
          <rect x="5" y="5" width="8" height="8" rx="1.5" stroke="currentColor" strokeWidth="1.5"/>
          <path d="M3 11V3.5C3 2.67 3.67 2 4.5 2H10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
        </svg>
      )}
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

function formatExplanation(text: string) {
  const lines = text.split("\n").filter((l) => l.trim());

  return lines.map((line, i) => {
    const rendered = inlineBold(line.trim());

    const numberedMatch = line.match(/^\s*(\d+)\.\s+(.*)/);
    if (numberedMatch) {
      return (
        <div key={i} className="ai-rewrite__point">
          <span className="ai-rewrite__point-num">{numberedMatch[1]}.</span>
          <span>{inlineBold(numberedMatch[2])}</span>
        </div>
      );
    }

    const bulletMatch = line.match(/^\s*[-•]\s+(.*)/);
    if (bulletMatch) {
      return (
        <div key={i} className="ai-rewrite__point">
          <span className="ai-rewrite__point-num">&bull;</span>
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
      return <code key={i}>{part.slice(1, -1)}</code>;
    }
    return part;
  });
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
          <div className="ai-rewrite__explanation">
            <h3>Explanation</h3>
            <div className="ai-rewrite__explanation-body">
              {formatExplanation(result.explanation)}
            </div>
          </div>
          <div className="ai-rewrite__columns">
            <div className="ai-rewrite__col">
              <h3>Original</h3>
              <div className="ai-rewrite__code-wrap">
                <CopyButton text={result.original_sql} />
                <pre><code>{result.original_sql}</code></pre>
              </div>
            </div>
            <div className="ai-rewrite__col">
              <h3>Suggested</h3>
              <div className="ai-rewrite__code-wrap">
                <CopyButton text={result.suggested_sql} />
                <pre><code>{result.suggested_sql}</code></pre>
              </div>
            </div>
          </div>
          <button className="ai-rewrite__btn" onClick={handleRewrite} disabled={loading}>
            {loading ? "Regenerating..." : "Regenerate"}
          </button>
        </div>
      )}
    </div>
  );
}
