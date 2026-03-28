import json
import logging
import os
import queue
import threading

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.analyzer import STEPS, run_analysis
from backend.analyzers.ai_advisor import rewrite_query
from backend.models import AIRewriteResult, AnalysisResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Databricks Query Performance Analyzer")

_analysis_cache: dict[str, AnalysisResult] = {}


# ---------------------------------------------------------------------------
# SSE streaming analysis (sends progress events, then final result)
# ---------------------------------------------------------------------------
@app.get("/api/analyze/{statement_id}/stream")
async def analyze_stream(statement_id: str):
    q: queue.Queue[dict | None] = queue.Queue()

    def on_progress(step: int, label: str, status: str) -> None:
        q.put({"step": step, "total": len(STEPS), "label": label, "status": status})

    def run() -> None:
        try:
            result = run_analysis(statement_id, on_progress=on_progress)
            _analysis_cache[statement_id] = result
            q.put({"event": "result", "data": result.model_dump(mode="json")})
        except ValueError as exc:
            q.put({"event": "error", "detail": str(exc), "code": 404})
        except Exception as exc:
            logger.exception("Analysis failed for %s", statement_id)
            q.put({"event": "error", "detail": str(exc), "code": 500})
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
    try:
        result = run_analysis(statement_id)
        _analysis_cache[statement_id] = result
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Analysis failed for %s", statement_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/rewrite/{statement_id}", response_model=AIRewriteResult)
async def rewrite(statement_id: str):
    analysis = _analysis_cache.get(statement_id)
    if analysis is None:
        try:
            analysis = run_analysis(statement_id)
            _analysis_cache[statement_id] = analysis
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Analysis failed for %s", statement_id)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        return rewrite_query(analysis)
    except Exception as exc:
        logger.exception("AI rewrite failed for %s", statement_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
