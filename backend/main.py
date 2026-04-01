import json
import logging
import os
import queue
import re
import threading
import time
import uuid
from collections import OrderedDict

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.analyzer import STEPS, run_analysis
from backend.analyzers.ai_advisor import rewrite_query
from backend.db import cancel_statement, execute_sql_with_metrics
from backend.models import AIRewriteResult, AnalysisResult, BenchmarkResult, QueryBenchmarkStats, QueryExecutionMetrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Databricks SQL Architect")

# ---------------------------------------------------------------------------
# Bounded TTL cache for analysis results
# ---------------------------------------------------------------------------
_CACHE_MAX_SIZE = 200
_CACHE_TTL_SECONDS = 30 * 60  # 30 minutes

_analysis_cache: OrderedDict[str, tuple[float, AnalysisResult]] = OrderedDict()


def _cache_get(key: str) -> AnalysisResult | None:
    entry = _analysis_cache.get(key)
    if entry is None:
        return None
    ts, result = entry
    if time.time() - ts > _CACHE_TTL_SECONDS:
        _analysis_cache.pop(key, None)
        return None
    _analysis_cache.move_to_end(key)
    return result


def _cache_put(key: str, result: AnalysisResult) -> None:
    _analysis_cache[key] = (time.time(), result)
    _analysis_cache.move_to_end(key)
    while len(_analysis_cache) > _CACHE_MAX_SIZE:
        _analysis_cache.popitem(last=False)


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------
_STATEMENT_ID_RE = re.compile(r"^[0-9a-fA-F\-]{1,128}$")


def _validate_statement_id(statement_id: str) -> None:
    if not _STATEMENT_ID_RE.match(statement_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid statement_id format. Expected a UUID-like identifier.",
        )


# ---------------------------------------------------------------------------
# SSE streaming analysis (sends progress events, then final result)
# ---------------------------------------------------------------------------
@app.get("/api/analyze/{statement_id}/stream")
async def analyze_stream(statement_id: str):
    _validate_statement_id(statement_id)

    q: queue.Queue[dict | None] = queue.Queue()

    def on_progress(step: int, label: str, status: str) -> None:
        q.put({"step": step, "total": len(STEPS), "label": label, "status": status})

    def run() -> None:
        try:
            result = run_analysis(statement_id, on_progress=on_progress)
            _cache_put(statement_id, result)
            q.put({"event": "result", "data": result.model_dump(mode="json")})
        except ValueError as exc:
            q.put({"event": "error", "detail": str(exc), "code": 404})
        except Exception:
            logger.exception("Analysis failed for %s", statement_id)
            q.put({"event": "error", "detail": "Internal analysis error", "code": 500})
        finally:
            q.put(None)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def event_generator():
        while True:
            msg = q.get()
            if msg is None:
                break
            yield f"data: {json.dumps(msg)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Non-streaming fallback (kept for simplicity / direct API calls)
# ---------------------------------------------------------------------------
@app.get("/api/analyze/{statement_id}", response_model=AnalysisResult)
async def analyze(statement_id: str):
    _validate_statement_id(statement_id)
    try:
        result = run_analysis(statement_id)
        _cache_put(statement_id, result)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception:
        logger.exception("Analysis failed for %s", statement_id)
        raise HTTPException(status_code=500, detail="Internal analysis error") from None


class RewriteRequest(BaseModel):
    custom_instruction: str | None = None


@app.post("/api/rewrite/{statement_id}", response_model=AIRewriteResult)
async def rewrite(statement_id: str, req: RewriteRequest | None = None):
    _validate_statement_id(statement_id)

    analysis = _cache_get(statement_id)
    if analysis is None:
        try:
            analysis = run_analysis(statement_id)
            _cache_put(statement_id, analysis)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception:
            logger.exception("Analysis failed for %s", statement_id)
            raise HTTPException(status_code=500, detail="Internal analysis error") from None

    custom_instruction = req.custom_instruction if req else None

    try:
        return rewrite_query(analysis, custom_instruction=custom_instruction)
    except Exception:
        logger.exception("AI rewrite failed for %s", statement_id)
        raise HTTPException(status_code=500, detail="AI rewrite failed") from None


class BenchmarkRequest(BaseModel):
    original_sql: str
    suggested_sql: str
    warehouse_id: str | None = None
    parameters: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# Async benchmark via submit + poll (gateway-timeout-safe)
# ---------------------------------------------------------------------------
_BENCHMARK_JOBS_MAX = 50
_BENCHMARK_JOB_TTL = 30 * 60  # 30 minutes

_benchmark_jobs: OrderedDict[str, dict] = OrderedDict()
_benchmark_jobs_lock = threading.Lock()


def _prune_benchmark_jobs() -> None:
    """Remove expired or excess jobs. Caller must hold the lock."""
    now = time.time()
    expired = [k for k, v in _benchmark_jobs.items() if now - v["created_at"] > _BENCHMARK_JOB_TTL]
    for k in expired:
        _benchmark_jobs.pop(k, None)
    while len(_benchmark_jobs) > _BENCHMARK_JOBS_MAX:
        _benchmark_jobs.popitem(last=False)


@app.post("/api/benchmark/start")
async def benchmark_start(req: BenchmarkRequest):
    """Submit a benchmark job that runs in the background. Returns a job ID for polling."""
    wid = req.warehouse_id or None
    job_id = uuid.uuid4().hex

    job: dict = {
        "created_at": time.time(),
        "status": "running",
        "progress": {},
        "statement_ids": {},
        "result": None,
        "error": None,
    }

    with _benchmark_jobs_lock:
        _prune_benchmark_jobs()
        _benchmark_jobs[job_id] = job

    def _to_benchmark_stats(raw: dict) -> QueryBenchmarkStats:
        metrics_data = raw.pop("metrics", None)
        stats = QueryBenchmarkStats(**raw)
        if metrics_data:
            stats.metrics = QueryExecutionMetrics(**metrics_data)
        return stats

    def _poll_cb(phase: str):
        def cb(info: dict):
            job["progress"][phase] = {"phase": phase, **info}
            if "statement_id" in info and info["statement_id"]:
                job["statement_ids"][phase] = info["statement_id"]
        return cb

    results: dict[str, dict | None] = {"original": None, "suggested": None}
    errors: dict[str, str] = {}

    def run_one(phase: str, sql: str) -> None:
        try:
            job["progress"][phase] = {"phase": phase, "state": "STARTING", "elapsed_ms": 0}
            raw = execute_sql_with_metrics(sql, warehouse_id=wid, parameters=req.parameters, on_poll=_poll_cb(phase))
            job["progress"][phase] = {
                "phase": phase, "state": "DONE",
                "statement_id": raw.get("statement_id"), "elapsed_ms": raw["elapsed_ms"],
            }
            results[phase] = raw
        except Exception as exc:
            logger.exception("Benchmark %s query failed", phase)
            errors[phase] = str(exc)

    def run() -> None:
        try:
            t_original = threading.Thread(target=run_one, args=("original", req.original_sql))
            t_suggested = threading.Thread(target=run_one, args=("suggested", req.suggested_sql))
            t_original.start()
            t_suggested.start()
            t_original.join()
            t_suggested.join()

            if errors:
                parts = [f"{phase}: {msg}" for phase, msg in errors.items()]
                job["status"] = "error"
                job["error"] = "; ".join(parts)
            else:
                result = BenchmarkResult(
                    original=_to_benchmark_stats(results["original"]),
                    suggested=_to_benchmark_stats(results["suggested"]),
                )
                job["result"] = result.model_dump(mode="json")
                job["status"] = "done"
        except Exception as exc:
            logger.exception("Benchmark job failed")
            job["status"] = "error"
            job["error"] = str(exc)

    threading.Thread(target=run, daemon=True).start()

    return {"benchmark_id": job_id}


@app.get("/api/benchmark/{benchmark_id}/status")
async def benchmark_status(benchmark_id: str):
    """Poll for benchmark job progress and results."""
    with _benchmark_jobs_lock:
        job = _benchmark_jobs.get(benchmark_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Benchmark job not found or expired")

    return {
        "status": job["status"],
        "progress": job["progress"],
        "result": job["result"],
        "error": job["error"],
    }


@app.post("/api/benchmark/{benchmark_id}/cancel/{phase}")
async def benchmark_cancel(benchmark_id: str, phase: str):
    """Cancel a running benchmark query (original or suggested)."""
    if phase not in ("original", "suggested"):
        raise HTTPException(status_code=400, detail="Phase must be 'original' or 'suggested'")

    with _benchmark_jobs_lock:
        job = _benchmark_jobs.get(benchmark_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Benchmark job not found or expired")

    stmt_id = job["statement_ids"].get(phase)
    if not stmt_id:
        raise HTTPException(status_code=400, detail=f"No statement ID found for {phase} query")

    cancel_statement(stmt_id)
    return {"cancelled": True, "phase": phase, "statement_id": stmt_id}


@app.get("/api/health")
async def health_check():
    return {"status": "healthy"}


# ---------------------------------------------------------------------------
# Static files (built React frontend)
# ---------------------------------------------------------------------------
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(static_dir, exist_ok=True)

assets_dir = os.path.join(static_dir, "assets")
if os.path.isdir(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


@app.get("/{full_path:path}")
async def serve_react(full_path: str):  # noqa: ARG001
    index_html = os.path.join(static_dir, "index.html")
    if os.path.exists(index_html):
        return FileResponse(index_html)
    raise HTTPException(
        status_code=404,
        detail="Frontend not built. Please run 'npm run build' first.",
    )
