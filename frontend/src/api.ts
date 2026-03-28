import type { AIRewriteResult, AnalysisResult, BenchmarkResult } from "./types";

const BASE = "/api";

export interface StepProgress {
  step: number;
  total: number;
  label: string;
  status: "running" | "done";
}

export interface AnalyzeStreamCallbacks {
  onProgress: (progress: StepProgress) => void;
  onResult: (result: AnalysisResult) => void;
  onError: (message: string) => void;
}

export async function analyzeQueryStream(
  statementId: string,
  callbacks: AnalyzeStreamCallbacks,
): Promise<void> {
  const url = `${BASE}/analyze/${encodeURIComponent(statementId)}/stream`;
  const res = await fetch(url);

  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    callbacks.onError(body.detail || `Request failed: ${res.status}`);
    return;
  }

  const reader = res.body?.getReader();
  if (!reader) {
    callbacks.onError("Streaming not supported");
    return;
  }

  const decoder = new TextDecoder();
  let buffer = "";
  let completed = false;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n\n");
      buffer = lines.pop() || "";

      for (const chunk of lines) {
        const dataLine = chunk.trim();
        if (!dataLine.startsWith("data: ")) continue;
        const jsonStr = dataLine.slice(6);

        try {
          const msg = JSON.parse(jsonStr);

          if (msg.event === "error") {
            callbacks.onError(msg.detail || "Analysis failed");
            completed = true;
            return;
          }

          if (msg.event === "result" && isAnalysisResult(msg.data)) {
            callbacks.onResult(msg.data);
            completed = true;
            return;
          }

          if (typeof msg.step === "number" && typeof msg.label === "string") {
            callbacks.onProgress(msg as StepProgress);
          }
        } catch {
          // ignore malformed chunks
        }
      }
    }
  } finally {
    if (!completed) {
      callbacks.onError("Analysis ended unexpectedly");
    }
  }
}

export function rewriteQuery(statementId: string): Promise<AIRewriteResult> {
  return request<AIRewriteResult>(`${BASE}/rewrite/${encodeURIComponent(statementId)}`, {
    method: "POST",
  });
}

export function benchmarkQueries(
  originalSql: string,
  suggestedSql: string,
  warehouseId?: string,
): Promise<BenchmarkResult> {
  return request<BenchmarkResult>(`${BASE}/benchmark`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      original_sql: originalSql,
      suggested_sql: suggestedSql,
      warehouse_id: warehouseId ?? null,
    }),
  });
}

function isAnalysisResult(data: unknown): data is AnalysisResult {
  return (
    typeof data === "object" &&
    data !== null &&
    "query_metrics" in data &&
    typeof (data as Record<string, unknown>).query_metrics === "object"
  );
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || `Request failed: ${res.status}`);
  }
  return res.json();
}
