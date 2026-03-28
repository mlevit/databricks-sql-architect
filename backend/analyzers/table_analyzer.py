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
            name, clustering, partitions, num_files, size_bytes, parsed_query
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


def _analyze_single_table(
    table_name: str,
    clustering: list[str],
    partitions: list[str],
    num_files: int | None,
    size_bytes: int | None,
    parsed: ParsedQuery,
) -> list[Recommendation]:
    recs: list[Recommendation] = []

    filter_cols_lower = {c.lower() for c in parsed.filter_columns}
    clustering_lower = {c.lower() for c in clustering}
    partition_lower = {c.lower() for c in partitions}

    # No clustering on columns used in WHERE
    unclustered_filter_cols = filter_cols_lower - clustering_lower - partition_lower
    if unclustered_filter_cols and not clustering:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.TABLE,
            title=f"No clustering on {table_name}",
            description=(
                f"Table {table_name} has no liquid clustering configured, "
                f"but query filters on columns: {', '.join(sorted(unclustered_filter_cols))}. "
                "Without clustering, data skipping is ineffective."
            ),
            action=f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(sorted(unclustered_filter_cols))})",
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

    return recs


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
