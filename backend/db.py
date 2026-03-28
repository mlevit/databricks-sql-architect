from __future__ import annotations

import logging
import os
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from databricks.sdk.service.sql import StatementState

logger = logging.getLogger(__name__)

_client: WorkspaceClient | None = None


def get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def get_warehouse_id() -> str:
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not wid:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID environment variable is not set. "
            "Configure it in app.yaml or your environment."
        )
    return wid


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
    statement: str, *, warehouse_id: str | None = None
) -> dict[str, Any]:
    """Execute a SQL statement and return wall-clock time plus manifest stats.

    Returns a dict with keys: elapsed_ms, row_count, byte_count, status,
    statement_id, and error (if failed).
    """
    import time as _time

    w = get_client()
    wid = warehouse_id or get_warehouse_id()

    logger.info("Benchmark executing on warehouse %s: %s", wid, statement[:200])

    w.statement_execution.execute_statement(
        warehouse_id=wid,
        statement="SET use_cached_result = false",
        wait_timeout="10s",
    )

    t0 = _time.perf_counter()
    response = w.statement_execution.execute_statement(
        warehouse_id=wid,
        statement=statement,
        wait_timeout="0s",
    )

    stmt_id = getattr(response, "statement_id", None)

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
            _time.sleep(1)

    elapsed_ms = round((_time.perf_counter() - t0) * 1000)

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
        "num_clusters": wh.num_clusters,
        "enable_photon": wh.enable_photon,
        "enable_serverless_compute": wh.enable_serverless_compute,
        "spot_instance_policy": (
            wh.spot_instance_policy.value if wh.spot_instance_policy else None
        ),
        "channel": wh.channel.name.value if wh.channel and wh.channel.name else None,
    }
