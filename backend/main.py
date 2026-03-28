import json
import logging
import os
import queue
import re
import threading
import time
from collections import OrderedDict

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.analyzer import STEPS, run_analysis
from backend.analyzers.ai_advisor import rewrite_query
from backend.db import execute_sql_with_metrics
from backend.models import AIRewriteResult, AnalysisResult, BenchmarkResult, QueryBenchmarkStats, QueryExecutionMetrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Databricks Query Performance Analyzer")

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


@app.post("/api/benchmark", response_model=BenchmarkResult)
async def benchmark(req: BenchmarkRequest):
    """Run both the original and suggested queries and return execution stats."""
    wid = req.warehouse_id or None

    try:
        original_stats = execute_sql_with_metrics(req.original_sql, warehouse_id=wid)
    except Exception:
        logger.exception("Benchmark: original query execution failed")
        raise HTTPException(status_code=500, detail="Failed to execute original query") from None

    try:
        suggested_stats = execute_sql_with_metrics(req.suggested_sql, warehouse_id=wid)
    except Exception:
        logger.exception("Benchmark: suggested query execution failed")
        raise HTTPException(status_code=500, detail="Failed to execute suggested query") from None

    def _to_benchmark_stats(raw: dict) -> QueryBenchmarkStats:
        metrics_data = raw.pop("metrics", None)
        stats = QueryBenchmarkStats(**raw)
        if metrics_data:
            stats.metrics = QueryExecutionMetrics(**metrics_data)
        return stats

    return BenchmarkResult(
        original=_to_benchmark_stats(original_stats),
        suggested=_to_benchmark_stats(suggested_stats),
    )


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
