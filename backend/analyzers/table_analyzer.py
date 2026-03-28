from __future__ import annotations

import json
import logging
from typing import Any

from backend.analyzers.sql_parser import ParsedQuery
from backend.db import execute_sql
from backend.models import Category, Recommendation, Severity, TableInfo

logger = logging.getLogger(__name__)

SMALL_FILE_THRESHOLD = 32 * 1024 * 1024  # 32 MB average per file
MANY_FILES_THRESHOLD = 1000
LARGE_TABLE_THRESHOLD = 10 * 1024 * 1024 * 1024  # 10 GB
OVER_PARTITIONED_FILE_RATIO = 5  # files per MB on average


def fetch_table_detail(table_name: str) -> dict[str, Any] | None:
    try:
        rows = execute_sql(f"DESCRIBE DETAIL {table_name}")
        if rows:
            return rows[0]
    except Exception as exc:
        logger.warning("Failed to DESCRIBE DETAIL %s: %s", table_name, exc)
    return None


def analyze_tables(
    table_names: list[str],
    parsed_query: ParsedQuery,
) -> list[TableInfo]:
    results: list[TableInfo] = []

    for name in table_names:
        if name.lower().startswith("system."):
            results.append(TableInfo(full_name=name))
            continue

        detail = fetch_table_detail(name)
        if detail is None:
            results.append(TableInfo(full_name=name))
            continue

        clustering = _parse_list(detail.get("clusteringColumns"))
        partitions = _parse_list(detail.get("partitionColumns"))
        num_files = _safe_int(detail.get("numFiles"))
        size_bytes = _safe_int(detail.get("sizeInBytes"))
        props = detail.get("properties", {})
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except (json.JSONDecodeError, TypeError):
                props = {}

        recs = _analyze_single_table(
            name, clustering, partitions, num_files, size_bytes,
            props if isinstance(props, dict) else {},
            parsed_query,
        )

        results.append(TableInfo(
            full_name=name,
            format=detail.get("format"),
            clustering_columns=clustering,
            partition_columns=partitions,
            num_files=num_files,
            size_in_bytes=size_bytes,
            properties=props if isinstance(props, dict) else {},
            recommendations=recs,
        ))

    return results


def _strip_table_prefix(col: str) -> str:
    """Remove table alias prefix from a column name (e.g. 't.col' -> 'col')."""
    return col.rsplit(".", 1)[-1]


def _analyze_single_table(
    table_name: str,
    clustering: list[str],
    partitions: list[str],
    num_files: int | None,
    size_bytes: int | None,
    properties: dict[str, str],
    parsed: ParsedQuery,
) -> list[Recommendation]:
    recs: list[Recommendation] = []

    table_filters = parsed.table_filter_columns.get(table_name, [])
    if not table_filters:
        table_filters = parsed.filter_columns

    filter_cols_lower = {_strip_table_prefix(c).lower() for c in table_filters}
    join_cols_lower = {_strip_table_prefix(c).lower() for c in parsed.join_columns}
    clustering_lower = {c.lower() for c in clustering}
    partition_lower = {c.lower() for c in partitions}

    # No clustering on columns used in WHERE
    unclustered_filter_cols = filter_cols_lower - clustering_lower - partition_lower
    if unclustered_filter_cols and not clustering:
        clean_cols = sorted(unclustered_filter_cols)
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.TABLE,
            title=f"No clustering on {table_name}",
            description=(
                f"Table {table_name} has no liquid clustering configured, "
                f"but query filters on columns: {', '.join(clean_cols)}. "
                "Without clustering, data skipping is ineffective."
            ),
            action=f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(clean_cols)})",
        ))

    # Small file problem
    if num_files and size_bytes and num_files > MANY_FILES_THRESHOLD:
        avg_file_size = size_bytes / num_files
        if avg_file_size < SMALL_FILE_THRESHOLD:
            avg_mb = avg_file_size / (1024 * 1024)
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.TABLE,
                title=f"Small file problem on {table_name}",
                description=(
                    f"Table has {num_files:,} files with average size {avg_mb:.1f} MB. "
                    "Many small files cause excessive metadata overhead and slow scans."
                ),
                action=f"OPTIMIZE {table_name}",
            ))

    # Partitioned but filter columns don't align
    if partitions and not partition_lower.intersection(filter_cols_lower):
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.TABLE,
            title=f"Partition columns not used in filters on {table_name}",
            description=(
                f"Table is partitioned by [{', '.join(partitions)}] "
                "but none of these columns appear in the query's WHERE clause. "
                "Partition pruning cannot help this query."
            ),
            action=(
                "Add a filter on the partition column if possible, "
                "or consider re-partitioning the table based on your most common query patterns."
            ),
        ))

    # D1: Table statistics staleness
    _check_stats_staleness(table_name, properties, recs)

    # D2: Over-partitioned tables
    _check_over_partitioned(table_name, partitions, num_files, size_bytes, recs)

    # D3: Large unorganized tables
    _check_large_unorganized(table_name, clustering, partitions, size_bytes, filter_cols_lower, recs)

    # D4: Join columns not clustered
    _check_join_columns_not_clustered(table_name, clustering_lower, join_cols_lower, recs)

    return recs


def _check_stats_staleness(
    table_name: str,
    properties: dict[str, str],
    recs: list[Recommendation],
) -> None:
    """D1: Recommend ANALYZE TABLE if statistics appear missing."""
    stats_keys = {
        "spark.sql.statistics.totalSize",
        "spark.sql.statistics.numRows",
        "delta.stats.numRecords",
    }
    has_stats = any(k in properties for k in stats_keys)

    if not has_stats:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.TABLE,
            title=f"Table statistics may be stale on {table_name}",
            description=(
                f"No row/size statistics found in table properties for {table_name}. "
                "The query optimizer relies on accurate statistics for cost-based "
                "decisions like join ordering and broadcast thresholds."
            ),
            action=f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS",
        ))


def _check_over_partitioned(
    table_name: str,
    partitions: list[str],
    num_files: int | None,
    size_bytes: int | None,
    recs: list[Recommendation],
) -> None:
    """D2: Detect tables with too many small partitions."""
    if not partitions or not num_files or not size_bytes or size_bytes == 0:
        return

    size_mb = size_bytes / (1024 * 1024)
    if size_mb > 0 and num_files / size_mb > OVER_PARTITIONED_FILE_RATIO:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.TABLE,
            title=f"Potentially over-partitioned: {table_name}",
            description=(
                f"Table has {num_files:,} files across {', '.join(partitions)} partitions "
                f"for {size_mb:,.0f} MB of data ({num_files / size_mb:.1f} files per MB). "
                "High-cardinality partition columns create many tiny partitions."
            ),
            action=(
                f"Consider migrating from partitioning to liquid clustering:\n"
                f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(partitions[:4])});\n"
                f"OPTIMIZE {table_name};"
            ),
        ))


def _check_large_unorganized(
    table_name: str,
    clustering: list[str],
    partitions: list[str],
    size_bytes: int | None,
    filter_cols: set[str],
    recs: list[Recommendation],
) -> None:
    """D3: Flag large tables with no clustering or partitioning."""
    if clustering or partitions:
        return
    if not size_bytes or size_bytes < LARGE_TABLE_THRESHOLD:
        return

    size_gb = size_bytes / (1024 ** 3)
    cols_hint = ", ".join(sorted(filter_cols)[:4]) if filter_cols else "<frequently filtered columns>"
    recs.append(Recommendation(
        severity=Severity.WARNING,
        category=Category.TABLE,
        title=f"Large unorganized table: {table_name}",
        description=(
            f"Table is {size_gb:,.1f} GB with no clustering or partitioning. "
            "Every query must scan through unordered data, making data skipping impossible."
        ),
        action=(
            f"ALTER TABLE {table_name} CLUSTER BY ({cols_hint});\n"
            f"OPTIMIZE {table_name};"
        ),
    ))


def _check_join_columns_not_clustered(
    table_name: str,
    clustering_lower: set[str],
    join_cols_lower: set[str],
    recs: list[Recommendation],
) -> None:
    """D4: Check if join columns are not covered by clustering."""
    if not join_cols_lower or not clustering_lower:
        return

    unclustered_join_cols = join_cols_lower - clustering_lower
    if unclustered_join_cols and len(unclustered_join_cols) == len(join_cols_lower):
        cols = ", ".join(sorted(unclustered_join_cols))
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.TABLE,
            title=f"Join columns not in clustering on {table_name}",
            description=(
                f"Query joins on columns [{cols}] but {table_name} is clustered "
                f"on different columns. Clustering on join keys enables co-located "
                "joins and reduces shuffle."
            ),
            action=(
                f"If this join pattern is common, consider including the join columns "
                f"in the clustering key: ALTER TABLE {table_name} CLUSTER BY ({cols})"
            ),
        ))


def _parse_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("["):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass
        if value:
            return [v.strip() for v in value.split(",") if v.strip()]
    return []


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
