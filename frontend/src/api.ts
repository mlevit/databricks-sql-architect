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

export function rewriteQuery(statementId: string, customInstruction?: string): Promise<AIRewriteResult> {
  const body = customInstruction ? { custom_instruction: customInstruction } : undefined;
  return request<AIRewriteResult>(`${BASE}/rewrite/${encodeURIComponent(statementId)}`, {
    method: "POST",
    ...(body && {
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
}

export interface BenchmarkProgress {
  phase: "original" | "suggested";
  state: string;
  statement_id?: string;
  elapsed_ms?: number;
}

export interface BenchmarkPollCallbacks {
  onProgress: (progress: Record<string, BenchmarkProgress>) => void;
  onResult: (result: BenchmarkResult) => void;
  onError: (message: string) => void;
  onStarted?: (benchmarkId: string) => void;
}

const POLL_INTERVAL_MS = 2000;

export async function benchmarkQueriesPoll(
  originalSql: string,
  suggestedSql: string,
  warehouseId: string | undefined,
  parameters: Record<string, string> | undefined,
  callbacks: BenchmarkPollCallbacks,
): Promise<void> {
  let benchmarkId: string;
  try {
    const payload: Record<string, unknown> = {
      original_sql: originalSql,
      suggested_sql: suggestedSql,
      warehouse_id: warehouseId ?? null,
    };
    if (parameters && Object.keys(parameters).length > 0) {
      payload.parameters = parameters;
    }
    const startRes = await request<{ benchmark_id: string }>(`${BASE}/benchmark/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    benchmarkId = startRes.benchmark_id;
    callbacks.onStarted?.(benchmarkId);
  } catch (err) {
    callbacks.onError(err instanceof Error ? err.message : "Failed to start benchmark");
    return;
  }

  while (true) {
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));

    let poll: { status: string; progress: Record<string, BenchmarkProgress>; result: BenchmarkResult | null; error: string | null };
    try {
      poll = await request(`${BASE}/benchmark/${encodeURIComponent(benchmarkId)}/status`);
    } catch (err) {
      callbacks.onError(err instanceof Error ? err.message : "Failed to poll benchmark status");
      return;
    }

    if (poll.progress && Object.keys(poll.progress).length > 0) {
      callbacks.onProgress(poll.progress);
    }

    if (poll.status === "done" && poll.result) {
      callbacks.onResult(poll.result);
      return;
    }

    if (poll.status === "error") {
      callbacks.onError(poll.error || "Benchmark failed");
      return;
    }
  }
}

export function cancelBenchmarkQuery(
  benchmarkId: string,
  phase: "original" | "suggested",
): Promise<void> {
  return request(`${BASE}/benchmark/${encodeURIComponent(benchmarkId)}/cancel/${phase}`, {
    method: "POST",
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
