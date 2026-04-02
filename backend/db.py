from __future__ import annotations

import logging
import os
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

logger = logging.getLogger(__name__)

_client: WorkspaceClient | None = None

_user_token_var: ContextVar[str | None] = ContextVar("user_token", default=None)


def set_user_token(token: str | None) -> None:
    """Store the current user's access token for on-behalf-of-user auth."""
    _user_token_var.set(token)


def _get_app_client() -> WorkspaceClient:
    """Return the singleton app-identity (service principal) WorkspaceClient."""
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def get_client() -> WorkspaceClient:
    """Return a WorkspaceClient scoped to the current user when a token is
    available, otherwise fall back to the app service principal."""
    token = _user_token_var.get()
    if token:
        host = _get_app_client().config.host
        return WorkspaceClient(host=host, token=token, auth_type="pat")
    return _get_app_client()


def get_warehouse_id() -> str:
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not wid:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID environment variable is not set. "
            "Configure it in app.yaml or your environment."
        )
    return wid


def cancel_statement(statement_id: str) -> None:
    """Cancel a running Databricks SQL statement."""
    w = get_client()
    try:
        w.statement_execution.cancel_execution(statement_id)
        logger.info("Cancelled statement %s", statement_id)
    except Exception:
        logger.warning("Failed to cancel statement %s", statement_id, exc_info=True)


def execute_sql(
    statement: str, *, warehouse_id: str | None = None
) -> list[dict[str, Any]]:
    """Execute a SQL statement and return rows as list of dicts."""
    w = get_client()
    wid = warehouse_id or get_warehouse_id()

    logger.info("Executing SQL on warehouse %s: %s", wid, statement[:200])
    response = w.statement_execution.execute_statement(
        warehouse_id=wid,
        statement=statement,
        wait_timeout="50s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        err = response.status.error
        raise RuntimeError(f"SQL execution failed: {err}")

    rows: list[dict[str, Any]] = []
    if response.result and response.result.data_array and response.manifest:
        columns = [col.name for col in response.manifest.schema.columns]
        for row_data in response.result.data_array:
            rows.append(dict(zip(columns, row_data)))

    return rows


def execute_sql_with_metrics(
    statement: str,
    *,
    warehouse_id: str | None = None,
    parameters: dict[str, str] | None = None,
    on_poll: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Execute a SQL statement and return wall-clock time plus manifest stats.

    Returns a dict with keys: elapsed_ms, row_count, byte_count, status,
    statement_id, and error (if failed).

    *parameters* is an optional dict mapping parameter names to string values.
    These are passed to Databricks as native statement parameters (``:`-prefixed
    markers in the SQL text).

    *on_poll* is an optional callback invoked during the polling loop and
    metric-fetch phase so callers (e.g. SSE endpoints) can relay progress.
    It receives a dict with ``statement_id``, ``state``, and ``elapsed_ms``.
    """
    import time as _time

    w = get_client()
    wid = warehouse_id or get_warehouse_id()

    logger.info("Benchmark executing on warehouse %s: %s", wid, statement[:200])

    sdk_params: list[StatementParameterListItem] | None = None
    if parameters:
        sdk_params = [
            StatementParameterListItem(name=k, value=v)
            for k, v in parameters.items()
        ]

    w.statement_execution.execute_statement(
        warehouse_id=wid,
        statement="SET use_cached_result = false",
        wait_timeout="10s",
    )

    t0 = _time.perf_counter()
    response = w.statement_execution.execute_statement(
        warehouse_id=wid,
        statement=statement,
        parameters=sdk_params,
        wait_timeout="0s",
    )

    stmt_id = getattr(response, "statement_id", None)

    def _elapsed() -> int:
        return round((_time.perf_counter() - t0) * 1000)

    if on_poll and stmt_id:
        on_poll({"statement_id": stmt_id, "state": "SUBMITTED", "elapsed_ms": _elapsed()})

    if stmt_id:
        while True:
            poll = w.statement_execution.get_statement(stmt_id)
            state = poll.status.state if poll.status else None
            if state in (
                StatementState.SUCCEEDED,
                StatementState.FAILED,
                StatementState.CANCELED,
                StatementState.CLOSED,
            ):
                response = poll
                break
            if on_poll:
                on_poll({
                    "statement_id": stmt_id,
                    "state": state.value if state else "PENDING",
                    "elapsed_ms": _elapsed(),
                })
            _time.sleep(1)

    elapsed_ms = _elapsed()

    result: dict[str, Any] = {
        "elapsed_ms": elapsed_ms,
        "row_count": None,
        "byte_count": None,
        "statement_id": stmt_id,
        "status": "SUCCEEDED",
        "error": None,
    }

    if response.status and response.status.state == StatementState.FAILED:
        result["status"] = "FAILED"
        result["error"] = (
            str(response.status.error) if response.status.error else "Unknown error"
        )
        return result

    if response.manifest:
        result["row_count"] = response.manifest.total_row_count
        result["byte_count"] = response.manifest.total_byte_count

    if stmt_id:
        if on_poll:
            on_poll({"statement_id": stmt_id, "state": "FETCHING_METRICS", "elapsed_ms": _elapsed()})
        metrics = _fetch_benchmark_metrics(stmt_id)
        if metrics:
            result["metrics"] = metrics

    return result


def _fetch_benchmark_metrics(
    statement_id: str, retries: int = 3
) -> dict[str, Any] | None:
    """Fetch detailed execution metrics from Query History for a completed statement."""
    import time as _time

    w = get_client()
    for attempt in range(retries):
        if attempt > 0:
            _time.sleep(2)
        try:
            resp = w.query_history.list(
                filter_by=sql_service.QueryFilter(statement_ids=[statement_id]),
                include_metrics=True,
                max_results=1,
            )
            queries = resp.res if resp and resp.res else []
            if not queries:
                continue

            m = queries[0].metrics
            if not m:
                continue

            return {
                "total_duration_ms": m.total_time_ms,
                "compilation_duration_ms": m.compilation_time_ms,
                "execution_duration_ms": m.execution_time_ms,
                "result_fetch_duration_ms": m.result_fetch_time_ms,
                "total_task_duration_ms": m.task_total_time_ms,
                "read_bytes": m.read_bytes,
                "read_rows": m.rows_read_count,
                "read_files": m.read_files_count,
                "read_partitions": m.read_partitions_count,
                "pruned_files": m.pruned_files_count,
                "produced_rows": m.rows_produced_count,
                "spilled_local_bytes": m.spill_to_disk_bytes,
                "shuffle_read_bytes": getattr(m, "shuffle_read_bytes", None),
                "from_result_cache": m.result_from_cache,
            }
        except Exception:
            logger.warning(
                "Failed to fetch benchmark metrics for %s (attempt %d)",
                statement_id,
                attempt + 1,
            )

    return None


def fetch_query_history_via_api(statement_id: str) -> dict[str, Any] | None:
    """Fetch query details via the Query History REST API and normalise
    the result into the same dict shape that system.query.history returns."""
    w = get_client()

    logger.info("Falling back to Query History API for %s", statement_id)
    resp = w.query_history.list(
        filter_by=sql_service.QueryFilter(
            statement_ids=[statement_id],
        ),
        include_metrics=True,
        max_results=1,
    )

    queries = resp.res if resp and resp.res else []
    if not queries:
        return None

    q = queries[0]
    m = q.metrics

    return {
        "statement_id": q.query_id,
        "statement_text": q.query_text,
        "execution_status": q.status.value if q.status else "UNKNOWN",
        "total_duration_ms": m.total_time_ms if m else q.duration,
        "compilation_duration_ms": m.compilation_time_ms if m else None,
        "execution_duration_ms": m.execution_time_ms if m else None,
        "waiting_for_compute_duration_ms": None,
        "waiting_at_capacity_duration_ms": None,
        "result_fetch_duration_ms": m.result_fetch_time_ms if m else None,
        "total_task_duration_ms": m.task_total_time_ms if m else None,
        "read_bytes": m.read_bytes if m else None,
        "read_rows": m.rows_read_count if m else None,
        "read_files": m.read_files_count if m else None,
        "read_partitions": m.read_partitions_count if m else None,
        "pruned_files": m.pruned_files_count if m else None,
        "produced_rows": m.rows_produced_count if m else None,
        "spilled_local_bytes": m.spill_to_disk_bytes if m else None,
        "read_io_cache_percent": None,
        "from_result_cache": m.result_from_cache if m else None,
        "shuffle_read_bytes": None,
        "written_bytes": m.write_remote_bytes if m else None,
        "compute": {"warehouse_id": q.warehouse_id or q.endpoint_id},
        "start_time": (
            datetime.fromtimestamp(q.query_start_time_ms / 1000, tz=timezone.utc).isoformat()
            if getattr(q, "query_start_time_ms", None)
            else None
        ),
        "end_time": (
            datetime.fromtimestamp(q.query_end_time_ms / 1000, tz=timezone.utc).isoformat()
            if getattr(q, "query_end_time_ms", None)
            else None
        ),
    }


def get_warehouse_config(warehouse_id: str) -> dict[str, Any]:
    """Fetch warehouse configuration via the SDK."""
    w = get_client()
    wh = w.warehouses.get(warehouse_id)
    return {
        "warehouse_id": wh.id,
        "name": wh.name,
        "warehouse_type": wh.warehouse_type.value if wh.warehouse_type else None,
        "cluster_size": wh.cluster_size,
        "min_num_clusters": wh.min_num_clusters,
        "max_num_clusters": wh.max_num_clusters,
        "num_clusters": wh.num_clusters,
        "auto_stop_mins": wh.auto_stop_mins,
        "enable_photon": wh.enable_photon,
        "enable_serverless_compute": wh.enable_serverless_compute,
        "spot_instance_policy": (
            wh.spot_instance_policy.value if wh.spot_instance_policy else None
        ),
        "channel": wh.channel.name.value if wh.channel and wh.channel.name else None,
    }


def fetch_concurrent_queries(
    warehouse_id: str,
    statement_id: str,
    start_time: str,
    end_time: str,
) -> dict[str, int]:
    """Count queries on the same warehouse that overlapped with the given time window."""
    safe_wid = warehouse_id.replace("'", "''")
    safe_sid = statement_id.replace("'", "''")
    safe_start = start_time.replace("'", "''")
    safe_end = end_time.replace("'", "''")

    sql = (
        "SELECT "
        "  COUNT(*) AS total_queries, "
        "  COUNT(CASE WHEN waiting_at_capacity_duration_ms > 0 THEN 1 END) AS queued_queries "
        "FROM system.query.history "
        f"WHERE compute.warehouse_id = '{safe_wid}' "
        f"  AND start_time < '{safe_end}' "
        f"  AND end_time > '{safe_start}' "
        f"  AND statement_id != '{safe_sid}'"
    )
    rows = execute_sql(sql)
    if rows:
        return {
            "total_queries": int(rows[0].get("total_queries", 0)),
            "queued_queries": int(rows[0].get("queued_queries", 0)),
        }
    return {"total_queries": 0, "queued_queries": 0}


def fetch_query_load_timeline(
    warehouse_id: str,
    start_time: str,
    end_time: str,
    *,
    buffer_minutes: int = 5,
) -> list[dict[str, Any]]:
    """Return per-minute count of running queries on the warehouse during the window.

    Includes ``buffer_minutes`` of context before and after the query so the
    chart shows surrounding warehouse activity.
    """
    safe_wid = warehouse_id.replace("'", "''")
    safe_start = start_time.replace("'", "''")
    safe_end = end_time.replace("'", "''")

    sql = (
        "WITH buckets AS ("
        "  SELECT EXPLODE(SEQUENCE("
        f"    DATE_TRUNC('minute', TIMESTAMPADD(MINUTE, -{buffer_minutes}, TIMESTAMP '{safe_start}')),"
        f"    TIMESTAMPADD(MINUTE, {buffer_minutes}, TIMESTAMP '{safe_end}'),"
        "    INTERVAL 1 MINUTE"
        "  )) AS bucket_start"
        "), "
        "queries AS ("
        "  SELECT start_time AS q_start, end_time AS q_end, "
        "    COALESCE(waiting_at_capacity_duration_ms, 0) AS wait_ms "
        "  FROM system.query.history "
        f"  WHERE compute.warehouse_id = '{safe_wid}' "
        f"    AND start_time < TIMESTAMPADD(MINUTE, {buffer_minutes + 1}, TIMESTAMP '{safe_end}') "
        f"    AND end_time > TIMESTAMPADD(MINUTE, -{buffer_minutes}, TIMESTAMP '{safe_start}')"
        ") "
        "SELECT b.bucket_start AS bucket_time, "
        "  COUNT(q.q_start) AS running_count, "
        "  COUNT(CASE WHEN q.wait_ms > 0 THEN 1 END) AS queued_count "
        "FROM buckets b "
        "LEFT JOIN queries q "
        "  ON q.q_start < TIMESTAMPADD(MINUTE, 1, b.bucket_start) "
        "  AND q.q_end > b.bucket_start "
        "GROUP BY b.bucket_start "
        "ORDER BY b.bucket_start"
    )
    return execute_sql(sql)


def fetch_scaling_events(
    warehouse_id: str,
    start_time: str,
    end_time: str,
) -> list[dict[str, Any]]:
    """Fetch warehouse scaling events around the given time window (+-5 min buffer)."""
    safe_wid = warehouse_id.replace("'", "''")
    safe_start = start_time.replace("'", "''")
    safe_end = end_time.replace("'", "''")

    sql = (
        "SELECT event_time, event_type, cluster_count "
        "FROM system.compute.warehouse_events "
        f"WHERE warehouse_id = '{safe_wid}' "
        f"  AND event_time BETWEEN TIMESTAMPADD(MINUTE, -5, TIMESTAMP '{safe_start}') "
        f"      AND TIMESTAMPADD(MINUTE, 5, TIMESTAMP '{safe_end}') "
        "ORDER BY event_time"
    )
    return execute_sql(sql)
