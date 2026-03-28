from __future__ import annotations

import json
import logging
from typing import Any

from backend.models import Category, QueryMetrics, Recommendation, Severity

logger = logging.getLogger(__name__)

SPILL_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB
LOW_CACHE_PERCENT = 20
HIGH_SHUFFLE_RATIO = 0.5
CAPACITY_WAIT_RATIO = 0.25
HIGH_COMPILATION_RATIO = 0.3
READ_TO_PRODUCED_RATIO = 100
READ_TO_PRODUCED_MIN_ROWS = 1_000_000
HIGH_FETCH_RATIO = 0.3
LOW_PARALLELISM = 2.0


def build_query_metrics(row: dict[str, Any]) -> QueryMetrics:
    """Convert a system.query.history row into a QueryMetrics model."""

    def _int(key: str) -> int | None:
        val = row.get(key)
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    compute = row.get("compute")
    warehouse_id = None
    if isinstance(compute, str):
        try:
            compute = json.loads(compute)
        except (json.JSONDecodeError, TypeError):
            pass
    if isinstance(compute, dict):
        warehouse_id = compute.get("warehouse_id")
    if not warehouse_id:
        warehouse_id = row.get("warehouse_id")

    return QueryMetrics(
        statement_id=row.get("statement_id", ""),
        statement_text=row.get("statement_text", ""),
        execution_status=row.get("execution_status", "UNKNOWN"),
        total_duration_ms=_int("total_duration_ms"),
        compilation_duration_ms=_int("compilation_duration_ms"),
        execution_duration_ms=_int("execution_duration_ms"),
        waiting_for_compute_duration_ms=_int("waiting_for_compute_duration_ms"),
        waiting_at_capacity_duration_ms=_int("waiting_at_capacity_duration_ms"),
        result_fetch_duration_ms=_int("result_fetch_duration_ms"),
        total_task_duration_ms=_int("total_task_duration_ms"),
        read_bytes=_int("read_bytes"),
        read_rows=_int("read_rows"),
        read_files=_int("read_files"),
        read_partitions=_int("read_partitions"),
        pruned_files=_int("pruned_files"),
        produced_rows=_int("produced_rows"),
        spilled_local_bytes=_int("spilled_local_bytes"),
        read_io_cache_percent=_int("read_io_cache_percent"),
        from_result_cache=row.get("from_result_cache") in (True, "true", "TRUE", "1"),
        shuffle_read_bytes=_int("shuffle_read_bytes"),
        written_bytes=_int("written_bytes"),
        warehouse_id=warehouse_id,
    )


def analyze_query_metrics(
    metrics: QueryMetrics,
    tables: list[str] | None = None,
    filter_columns: list[str] | None = None,
) -> list[Recommendation]:
    recs: list[Recommendation] = []

    # Spill to disk
    if metrics.spilled_local_bytes and metrics.spilled_local_bytes > SPILL_THRESHOLD_BYTES:
        size_mb = metrics.spilled_local_bytes / (1024 * 1024)
        recs.append(Recommendation(
            severity=Severity.CRITICAL,
            category=Category.EXECUTION,
            title="Data spilling to disk",
            description=(
                f"Query spilled {size_mb:,.0f} MB to local disk. "
                "This dramatically slows execution and indicates the data processed "
                "exceeds available memory."
            ),
            action=(
                "Consider using a larger warehouse, reducing data volume with filters, "
                "or breaking the query into smaller stages."
            ),
        ))

    # Poor data-skipping / file pruning
    if (
        metrics.read_files is not None
        and metrics.pruned_files is not None
        and metrics.read_files > 0
    ):
        total_files = metrics.read_files + metrics.pruned_files
        if total_files > 0:
            prune_ratio = metrics.pruned_files / total_files
            if prune_ratio < 0.3 and total_files > 10:
                action = _build_clustering_action(tables, filter_columns)
                recs.append(Recommendation(
                    severity=Severity.WARNING,
                    category=Category.EXECUTION,
                    title="Poor data skipping",
                    description=(
                        f"Only {prune_ratio:.0%} of files were pruned "
                        f"({metrics.pruned_files} pruned out of {total_files} total). "
                        "The query is scanning far more files than necessary."
                    ),
                    action=action,
                ))

    # Low IO cache hit rate
    if (
        metrics.read_io_cache_percent is not None
        and metrics.read_io_cache_percent < LOW_CACHE_PERCENT
        and metrics.read_bytes
        and metrics.read_bytes > 100 * 1024 * 1024
    ):
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.EXECUTION,
            title="Low IO cache utilization",
            description=(
                f"IO cache hit rate is only {metrics.read_io_cache_percent}%. "
                "Repeated runs of this query are not benefiting from caching."
            ),
            action=(
                "If this query runs frequently, consider using a warehouse with "
                "local SSD caching or check if the data is changing too often for caching."
            ),
        ))

    # High shuffle
    if (
        metrics.shuffle_read_bytes
        and metrics.read_bytes
        and metrics.read_bytes > 0
    ):
        ratio = metrics.shuffle_read_bytes / metrics.read_bytes
        if ratio > HIGH_SHUFFLE_RATIO:
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.EXECUTION,
                title="High shuffle volume",
                description=(
                    f"Shuffle data ({metrics.shuffle_read_bytes / (1024**2):,.0f} MB) "
                    f"is {ratio:.0%} of data read. Heavy shuffling indicates large "
                    "data redistribution across nodes."
                ),
                action=(
                    "Check join keys and GROUP BY columns. Consider pre-aggregating data, "
                    "using broadcast joins for small tables, or clustering on join keys."
                ),
            ))

    # Waiting at capacity
    if (
        metrics.waiting_at_capacity_duration_ms
        and metrics.total_duration_ms
        and metrics.total_duration_ms > 0
    ):
        ratio = metrics.waiting_at_capacity_duration_ms / metrics.total_duration_ms
        if ratio > CAPACITY_WAIT_RATIO:
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.EXECUTION,
                title="Significant time waiting for capacity",
                description=(
                    f"Query spent {metrics.waiting_at_capacity_duration_ms:,} ms "
                    f"({ratio:.0%} of total time) waiting for available compute capacity."
                ),
                action=(
                    "Scale up the warehouse (more clusters or larger cluster size) "
                    "or schedule heavy queries during off-peak hours."
                ),
            ))

    # High compilation time
    if (
        metrics.compilation_duration_ms
        and metrics.total_duration_ms
        and metrics.total_duration_ms > 0
        and metrics.compilation_duration_ms > 5000
    ):
        ratio = metrics.compilation_duration_ms / metrics.total_duration_ms
        if ratio > HIGH_COMPILATION_RATIO:
            recs.append(Recommendation(
                severity=Severity.INFO,
                category=Category.EXECUTION,
                title="High compilation time",
                description=(
                    f"Compilation took {metrics.compilation_duration_ms:,} ms "
                    f"({ratio:.0%} of total). Complex queries with many joins or "
                    "UDFs can cause slow optimization."
                ),
                action=(
                    "Simplify the query, break it into CTEs or temporary views, "
                    "or ensure table statistics are up to date (ANALYZE TABLE)."
                ),
            ))

    # Extreme rows-read-to-rows-produced ratio
    if (
        metrics.read_rows
        and metrics.produced_rows
        and metrics.produced_rows > 0
        and metrics.read_rows > READ_TO_PRODUCED_MIN_ROWS
    ):
        ratio = metrics.read_rows / metrics.produced_rows
        if ratio > READ_TO_PRODUCED_RATIO:
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.EXECUTION,
                title="Excessive rows scanned vs produced",
                description=(
                    f"Query scanned {metrics.read_rows:,} rows but only produced "
                    f"{metrics.produced_rows:,} ({ratio:,.0f}x ratio). "
                    "Filters are not effectively reducing the data early in the pipeline."
                ),
                action=(
                    "Ensure tables are clustered on the filter columns so data skipping "
                    "can eliminate files before scanning. Check that predicate pushdown "
                    "is working (avoid wrapping filter columns in functions)."
                ),
            ))

    # High result fetch time
    if (
        metrics.result_fetch_duration_ms
        and metrics.total_duration_ms
        and metrics.total_duration_ms > 0
        and metrics.result_fetch_duration_ms > 2000
    ):
        fetch_ratio = metrics.result_fetch_duration_ms / metrics.total_duration_ms
        if fetch_ratio > HIGH_FETCH_RATIO:
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.EXECUTION,
                title="High result fetch time",
                description=(
                    f"Result fetching took {metrics.result_fetch_duration_ms:,} ms "
                    f"({fetch_ratio:.0%} of total). The query is returning a large "
                    "result set to the client."
                ),
                action=(
                    "Add a LIMIT clause, project fewer columns, or aggregate results "
                    "server-side to reduce the volume of data transferred."
                ),
            ))

    # Low parallelism efficiency
    if (
        metrics.total_task_duration_ms
        and metrics.execution_duration_ms
        and metrics.execution_duration_ms > 5000
    ):
        parallelism = metrics.total_task_duration_ms / metrics.execution_duration_ms
        if parallelism < LOW_PARALLELISM:
            recs.append(Recommendation(
                severity=Severity.INFO,
                category=Category.EXECUTION,
                title="Low parallelism efficiency",
                description=(
                    f"Effective parallelism is only {parallelism:.1f}x "
                    f"(total task time {metrics.total_task_duration_ms:,} ms vs "
                    f"wall clock {metrics.execution_duration_ms:,} ms). "
                    "The query is not fully utilizing the warehouse's compute capacity."
                ),
                action=(
                    "This may indicate data skew (one partition much larger than others), "
                    "a dataset too small for the warehouse size, or a single-threaded "
                    "bottleneck. Check data distribution and consider a smaller warehouse "
                    "for cost savings, or repartition the data."
                ),
            ))

    return recs


def _build_clustering_action(
    tables: list[str] | None,
    filter_columns: list[str] | None,
) -> str:
    if not tables or not filter_columns:
        return (
            "Add or adjust liquid clustering (CLUSTER BY) on the columns "
            "used in WHERE predicates. Run OPTIMIZE on the table."
        )

    # De-duplicate filter columns while preserving order
    seen: set[str] = set()
    unique_cols: list[str] = []
    for c in filter_columns:
        if c.lower() not in seen:
            seen.add(c.lower())
            unique_cols.append(c)

    # Databricks supports up to 4 clustering columns
    cluster_cols = unique_cols[:4]
    col_list = ", ".join(cluster_cols)

    user_tables = [t for t in tables if not t.lower().startswith("system.")]
    if not user_tables:
        return (
            "Add or adjust liquid clustering (CLUSTER BY) on the columns "
            "used in WHERE predicates. Run OPTIMIZE on the table."
        )

    lines: list[str] = []
    for table in user_tables:
        lines.append(f"ALTER TABLE {table} CLUSTER BY ({col_list});")
        lines.append(f"OPTIMIZE {table};")

    return "\n".join(lines)
