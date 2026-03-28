import type { AIRewriteResult, AnalysisResult } from "./types";

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

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n\n");
    buffer = lines.pop() || "";

    for (const chunk of lines) {
      const dataLine = chunk.trim();
      if (!dataLine.startsWith("data: ")) continue;
      const json_str = dataLine.slice(6);

      try {
        const msg = JSON.parse(json_str);

        if (msg.event === "error") {
          callbacks.onError(msg.detail || "Analysis failed");
          return;
        }

        if (msg.event === "result") {
          callbacks.onResult(msg.data as AnalysisResult);
          return;
        }

        // Progress event
        if (msg.step !== undefined) {
          callbacks.onProgress(msg as StepProgress);
        }
      } catch {
        // ignore malformed chunks
      }
    }
  }
}

export function rewriteQuery(statementId: string): Promise<AIRewriteResult> {
  return request<AIRewriteResult>(`${BASE}/rewrite/${encodeURIComponent(statementId)}`, {
    method: "POST",
  });
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || `Request failed: ${res.status}`);
  }
  return res.json();
}
