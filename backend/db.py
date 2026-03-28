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


def execute_sql(statement: str, *, warehouse_id: str | None = None) -> list[dict[str, Any]]:
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
        "spot_instance_policy": wh.spot_instance_policy.value if wh.spot_instance_policy else None,
        "channel": wh.channel.name.value if wh.channel and wh.channel.name else None,
    }
