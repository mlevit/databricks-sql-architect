from __future__ import annotations

import contextvars
import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from backend.analyzers.sql_parser import ParsedQuery
from backend.db import execute_sql, get_client
from backend.models import Category, ColumnInfo, Recommendation, Severity, TableInfo

logger = logging.getLogger(__name__)

SMALL_FILE_THRESHOLD = 32 * 1024 * 1024  # 32 MB average per file
MANY_FILES_THRESHOLD = 1000
LARGE_TABLE_THRESHOLD = 10 * 1024 * 1024 * 1024  # 10 GB
OVER_PARTITIONED_FILE_RATIO = 5  # files per MB on average
WIDE_TABLE_COLUMN_THRESHOLD = 100
UNDER_PARTITIONED_SIZE_THRESHOLD = 1024 * 1024 * 1024 * 1024  # 1 TB per partition

_HIGH_CARDINALITY_KEY_PATTERNS = re.compile(
    r"(uuid|guid|request_id|trace_id|session_id|transaction_id|correlation_id)$",
    re.IGNORECASE,
)

_DATE_COLUMN_PATTERNS = re.compile(
    r"(date|_dt$|_ts$|timestamp|created_at|updated_at|event_time|event_date|load_date|"
    r"ingestion_date|partition_date|day$|month$)",
    re.IGNORECASE,
)

_NUMERIC_COLUMN_PATTERNS = re.compile(
    r"(amount|price|cost|revenue|quantity|qty|total|balance|rate|fee|tax|salary|"
    r"budget|discount|weight|height|width|length|score|count|num_)",
    re.IGNORECASE,
)

_ENUM_COLUMN_PATTERNS = re.compile(
    r"(status|state|type|kind|category|code|flag|level|tier|role|mode|"
    r"priority|channel|region|country|currency|gender|class)",
    re.IGNORECASE,
)

_JSON_COLUMN_PATTERNS = re.compile(
    r"(json|payload|raw|event_data|body|response|request|metadata|attributes|"
    r"properties|context|config|settings|extra|tags|labels|params|details)",
    re.IGNORECASE,
)

_SKEW_PRONE_PARTITION_PATTERNS = re.compile(
    r"(country|region|state|city|status|type|category|channel|source|"
    r"tenant_id|tenant|customer_id|customer|user_id|user|account_id|account|"
    r"seller|vendor|merchant|partner|platform|device|browser|os$)",
    re.IGNORECASE,
)

_SAFE_TABLE_NAME_RE = re.compile(r"^(`[\w]+`|\w[\w]*)(\.(`[\w]+`|\w[\w]*))*$")

_SDK_MESSAGE_RE = re.compile(r'message="([^"]+)"')


def _extract_error_message(exc: Exception) -> str:
    """Extract a human-readable message from SDK exceptions."""
    text = str(exc)
    m = _SDK_MESSAGE_RE.search(text)
    return m.group(1) if m else text


def _unquote_table_name(name: str) -> str:
    """Strip backtick quoting from a fully-qualified table name for API calls."""
    return ".".join(part.strip("`") for part in name.split("."))


def is_poor_clustering_candidate(col_name: str) -> bool:
    """Return True if the column name suggests it would be a poor clustering key.

    Continuous numeric measures (amount, price, revenue, …) and high-cardinality
    unique identifiers (uuid, trace_id, …) make ineffective clustering keys
    because their min/max ranges per file span nearly the entire value space,
    rendering data-skipping useless.
    """
    return bool(
        _NUMERIC_COLUMN_PATTERNS.search(col_name)
        or _HIGH_CARDINALITY_KEY_PATTERNS.search(col_name)
    )


def _is_safe_table_name(name: str) -> bool:
    return bool(_SAFE_TABLE_NAME_RE.match(name)) and len(name) <= 256


def fetch_table_detail(
    table_name: str,
) -> tuple[dict[str, Any] | None, str | None]:
    if not _is_safe_table_name(table_name):
        logger.warning(
            "Skipping DESCRIBE DETAIL for unsafe table name: %s", table_name[:100]
        )
        return None, None
    try:
        rows = execute_sql(f"DESCRIBE DETAIL {table_name}")
        if rows:
            return rows[0], None
    except Exception as exc:
        logger.warning("Failed to DESCRIBE DETAIL %s: %s", table_name, exc)
        msg = _extract_error_message(exc)
        return None, f"Could not fetch details for `{table_name}`: {msg}"
    return None, None


def fetch_table_columns(
    table_name: str,
) -> tuple[list[ColumnInfo], str | None]:
    """Fetch column names, types, and comments via DESCRIBE TABLE."""
    if not _is_safe_table_name(table_name):
        return [], None
    try:
        rows = execute_sql(f"DESCRIBE TABLE {table_name}")
        columns: list[ColumnInfo] = []
        for row in rows:
            col_name = row.get("col_name", "")
            if not col_name or col_name.startswith("#") or col_name == "":
                continue
            data_type = row.get("data_type", "")
            if not data_type:
                continue
            comment = row.get("comment")
            columns.append(
                ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    comment=comment if comment else None,
                )
            )
        return columns, None
    except Exception as exc:
        logger.warning("Failed to DESCRIBE TABLE %s: %s", table_name, exc)
        msg = _extract_error_message(exc)
        return [], f"Could not fetch columns for `{table_name}`: {msg}"


def fetch_table_cbo_stats(
    table_name: str,
) -> tuple[dict[str, Any], str | None]:
    """Fetch statistics via the Unity Catalog Tables API.

    The Tables API ``properties`` map contains ``spark.sql.statistics.*``
    keys when ANALYZE TABLE has been run (manually or via AUTO_STATS).

    Returns a tuple of (stats_dict, warning_or_none).
    """
    empty: dict[str, Any] = {
        "has_cbo_stats": False,
        "num_rows": None,
        "total_size": None,
    }
    if not _is_safe_table_name(table_name):
        return empty, None
    try:
        w = get_client()
        api_name = _unquote_table_name(table_name)
        table_info = w.tables.get(api_name)
        props = table_info.properties or {}
        num_rows_str = props.get("spark.sql.statistics.numRows")
        total_size_str = props.get("spark.sql.statistics.totalSize")
        has_stats = num_rows_str is not None or total_size_str is not None
        return {
            "has_cbo_stats": has_stats,
            "num_rows": int(num_rows_str) if num_rows_str else None,
            "total_size": int(total_size_str) if total_size_str else None,
        }, None
    except Exception as exc:
        logger.warning("Failed to fetch catalog properties for %s: %s", table_name, exc)
        msg = _extract_error_message(exc)
        return empty, f"Could not fetch catalog properties for `{table_name}`: {msg}"


def analyze_tables(
    table_names: list[str],
    parsed_query: ParsedQuery,
) -> tuple[list[TableInfo], list[str]]:
    fetchable = [n for n in table_names if not n.lower().startswith("system.")]
    system_tables = [n for n in table_names if n.lower().startswith("system.")]

    details_map: dict[str, dict[str, Any] | None] = {}
    columns_map: dict[str, list[ColumnInfo]] = {}
    cbo_stats_map: dict[str, dict[str, Any]] = {}
    _empty_cbo: dict[str, Any] = {
        "has_cbo_stats": False,
        "num_rows": None,
        "total_size": None,
    }

    warnings: list[str] = []
    warnings_lock = threading.Lock()

    def _collect_warning(warning: str | None) -> None:
        if warning:
            with warnings_lock:
                warnings.append(warning)

    if fetchable:

        def _submit(pool, fn, *args):
            ctx = contextvars.copy_context()
            return pool.submit(ctx.run, fn, *args)

        with ThreadPoolExecutor(max_workers=min(len(fetchable) * 3, 16)) as pool:
            detail_futures = {
                _submit(pool, fetch_table_detail, name): ("detail", name)
                for name in fetchable
            }
            column_futures = {
                _submit(pool, fetch_table_columns, name): ("columns", name)
                for name in fetchable
            }
            cbo_futures = {
                _submit(pool, fetch_table_cbo_stats, name): ("cbo", name)
                for name in fetchable
            }
            all_futures = {**detail_futures, **column_futures, **cbo_futures}
            for future in as_completed(all_futures):
                kind, name = all_futures[future]
                try:
                    result, warning = future.result()
                    _collect_warning(warning)
                    if kind == "detail":
                        details_map[name] = result
                    elif kind == "columns":
                        columns_map[name] = result
                    else:
                        cbo_stats_map[name] = result
                except Exception:
                    if kind == "detail":
                        details_map[name] = None
                    elif kind == "columns":
                        columns_map[name] = []
                    else:
                        cbo_stats_map[name] = _empty_cbo

    results: list[TableInfo] = []
    for name in table_names:
        if name in system_tables:
            results.append(TableInfo(full_name=name))
            continue

        detail = details_map.get(name)
        if detail is None:
            results.append(TableInfo(full_name=name))
            continue

        clustering = _parse_list(detail.get("clusteringColumns"))
        partitions = _parse_list(detail.get("partitionColumns"))
        num_files = _safe_int(detail.get("numFiles"))
        size_bytes = _safe_int(detail.get("sizeInBytes"))
        table_format = detail.get("format")
        props = detail.get("properties", {})
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except (json.JSONDecodeError, TypeError):
                props = {}

        columns = columns_map.get(name, [])
        cbo = cbo_stats_map.get(name, _empty_cbo)

        recs = _analyze_single_table(
            name,
            clustering,
            partitions,
            num_files,
            size_bytes,
            props if isinstance(props, dict) else {},
            parsed_query,
            table_format=table_format,
            columns=columns,
            has_cbo_stats=cbo["has_cbo_stats"],
        )

        results.append(
            TableInfo(
                full_name=name,
                format=table_format,
                clustering_columns=clustering,
                partition_columns=partitions,
                num_files=num_files,
                size_in_bytes=size_bytes,
                column_count=len(columns) if columns else None,
                columns=columns,
                properties=props if isinstance(props, dict) else {},
                has_cbo_stats=cbo["has_cbo_stats"],
                stats_num_rows=cbo["num_rows"],
                stats_total_size=cbo["total_size"],
                recommendations=recs,
            )
        )

    seen = set()
    deduped_warnings = []
    for w in warnings:
        if w not in seen:
            seen.add(w)
            deduped_warnings.append(w)

    return results, deduped_warnings


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
    *,
    table_format: str | None = None,
    columns: list[ColumnInfo] | None = None,
    has_cbo_stats: bool = False,
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
        table_action = f"ALTER TABLE {table_name} CLUSTER BY AUTO;"

        recs.append(
            Recommendation(
                severity=Severity.WARNING,
                category=Category.TABLE,
                title="No clustering configured",
                description=(
                    "Table has no liquid clustering configured, but the query filters on "
                    "columns that would benefit from it. Without clustering, data skipping "
                    "is ineffective. A single query is not enough context to pick optimal "
                    "clustering columns — let Databricks decide based on overall workload patterns."
                ),
                affected_tables=[table_name],
                per_table_actions={table_name: table_action},
                impact=7,
            )
        )

    # Small file problem
    if num_files and size_bytes and num_files > MANY_FILES_THRESHOLD:
        avg_file_size = size_bytes / num_files
        if avg_file_size < SMALL_FILE_THRESHOLD:
            recs.append(
                Recommendation(
                    severity=Severity.WARNING,
                    category=Category.TABLE,
                    title="Small file problem",
                    description=(
                        "Table has many small files causing excessive metadata overhead "
                        "and slow scans. Run OPTIMIZE to compact them."
                    ),
                    affected_tables=[table_name],
                    per_table_actions={table_name: f"OPTIMIZE {table_name};"},
                    impact=6,
                )
            )

    # Partitioned but filter columns don't align
    if partitions and not partition_lower.intersection(filter_cols_lower):
        recs.append(
            Recommendation(
                severity=Severity.INFO,
                category=Category.TABLE,
                title="Partition columns not used in filters",
                description=(
                    "Table is partitioned but none of the partition columns appear in the "
                    "query's WHERE clause. Partition pruning cannot help this query."
                ),
                action=(
                    "Add a filter on the partition column if possible, "
                    "or consider re-partitioning the table based on your most common query patterns."
                ),
                affected_tables=[table_name],
                impact=5,
            )
        )

    # D1: Table statistics staleness
    _check_stats_staleness(table_name, has_cbo_stats, recs)

    # D2: Over-partitioned tables
    _check_over_partitioned(table_name, partitions, num_files, size_bytes, recs)

    # D3: Large unorganized tables
    _check_large_unorganized(
        table_name, clustering, partitions, size_bytes, filter_cols_lower, recs
    )

    # D4: Join columns not clustered
    _check_join_columns_not_clustered(
        table_name, clustering_lower, join_cols_lower, recs
    )

    # D5: No clustering at all — suggest CLUSTER BY AUTO as baseline
    if not clustering and not table_name.lower().startswith("system."):
        already_has_clustering_rec = any(
            "No clustering" in r.title or "Large unorganized" in r.title for r in recs
        )
        if not already_has_clustering_rec:
            recs.append(
                Recommendation(
                    severity=Severity.INFO,
                    category=Category.TABLE,
                    title="No clustering configured",
                    description=(
                        "Table has no liquid clustering. Enabling clustering "
                        "improves data skipping, reduces scan volume, and speeds up queries — "
                        "even when you are unsure which columns to cluster on."
                    ),
                    affected_tables=[table_name],
                    per_table_actions={
                        table_name: f"ALTER TABLE {table_name} CLUSTER BY AUTO;"
                    },
                    impact=5,
                )
            )

    # D6: Under-partitioning
    _check_under_partitioned(table_name, partitions, num_files, size_bytes, recs)

    # D7: High-cardinality clustering key
    _check_high_cardinality_clustering_key(table_name, clustering, size_bytes, recs)

    # D8: Wide table
    _check_wide_table(table_name, columns or [], recs)

    # D9: Non-Delta format
    _check_non_delta_format(table_name, table_format, recs)

    # D10: VACUUM / compaction needed
    _check_vacuum_needed(table_name, properties, size_bytes, recs)

    # D11: Inappropriate data types
    _check_inappropriate_data_types(table_name, columns or [], recs)

    # D12: VARCHAR for likely enum columns
    _check_string_enum_columns(table_name, columns or [], recs)

    # D13: Large table without date-based clustering
    _check_large_table_no_date_clustering(
        table_name,
        clustering,
        partitions,
        size_bytes,
        columns or [],
        recs,
    )

    # D14: Partition columns prone to data skew
    _check_partition_skew_risk(table_name, partitions, size_bytes, recs)

    # D15: STRING columns likely storing JSON
    _check_json_string_columns(table_name, columns or [], recs)

    # D16: Hive-style partitioning — migrate to liquid clustering
    _check_hive_partitioning(table_name, partitions, clustering, recs)

    return recs


def _check_stats_staleness(
    table_name: str,
    has_cbo_stats: bool,
    recs: list[Recommendation],
) -> None:
    """D1: Recommend ANALYZE TABLE if CBO statistics are missing.

    Uses the Unity Catalog Tables API to check whether ``spark.sql.statistics.*``
    properties exist.  When row-level stats are absent the cost-based optimizer
    cannot make informed join-ordering or broadcast decisions.
    """
    if not has_cbo_stats:
        recs.append(
            Recommendation(
                severity=Severity.INFO,
                category=Category.TABLE,
                title="Table statistics may be stale",
                description=(
                    "No row-level CBO statistics found for this table. "
                    "The query optimizer relies on accurate statistics for cost-based "
                    "decisions like join ordering and broadcast thresholds."
                ),
                affected_tables=[table_name],
                per_table_actions={
                    table_name: f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS;"
                },
                impact=4,
            )
        )


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
        recs.append(
            Recommendation(
                severity=Severity.WARNING,
                category=Category.TABLE,
                title="Potentially over-partitioned",
                description=(
                    "Table has a high file-to-data ratio suggesting over-partitioning. "
                    "High-cardinality partition columns create many tiny partitions. "
                    "Consider migrating to liquid clustering."
                ),
                affected_tables=[table_name],
                per_table_actions={
                    table_name: (
                        f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(partitions[:4])});\n"
                        f"OPTIMIZE {table_name};"
                    ),
                },
                impact=5,
            )
        )


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

    table_action = (
        f"ALTER TABLE {table_name} CLUSTER BY AUTO;\n" f"OPTIMIZE {table_name};"
    )
    recs.append(
        Recommendation(
            severity=Severity.WARNING,
            category=Category.TABLE,
            title="Large unorganized table",
            description=(
                "Table is large with no clustering or partitioning. "
                "Every query must scan through unordered data, making data skipping impossible."
            ),
            affected_tables=[table_name],
            per_table_actions={table_name: table_action},
            impact=7,
        )
    )


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
        recs.append(
            Recommendation(
                severity=Severity.INFO,
                category=Category.TABLE,
                title="Join columns not in clustering",
                description=(
                    "Query joins on columns that are not covered by the table's clustering key. "
                    "Clustering on join keys enables co-located joins and reduces shuffle."
                ),
                action="If this join pattern is common, consider including the join columns in the clustering key.",
                affected_tables=[table_name],
                per_table_actions={
                    table_name: f"ALTER TABLE {table_name} CLUSTER BY ({cols});"
                },
                impact=6,
            )
        )


# ---------------------------------------------------------------------------
# D6: Under-partitioning
# ---------------------------------------------------------------------------
def _check_under_partitioned(
    table_name: str,
    partitions: list[str],
    num_files: int | None,
    size_bytes: int | None,
    recs: list[Recommendation],
) -> None:
    if not partitions or not size_bytes or not num_files:
        return
    if num_files < 2:
        return
    avg_partition_size = size_bytes / max(num_files, 1)
    if avg_partition_size > UNDER_PARTITIONED_SIZE_THRESHOLD:
        recs.append(
            Recommendation(
                severity=Severity.INFO,
                category=Category.TABLE,
                title="Under-partitioned table",
                description=(
                    "Average partition size is very large, limiting the benefit of partition "
                    "pruning. Consider migrating to liquid clustering."
                ),
                affected_tables=[table_name],
                per_table_actions={
                    table_name: (
                        f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(partitions[:4])});\n"
                        f"OPTIMIZE {table_name};"
                    ),
                },
                impact=5,
            )
        )


# ---------------------------------------------------------------------------
# D7: High-cardinality clustering key
# ---------------------------------------------------------------------------
def _check_high_cardinality_clustering_key(
    table_name: str,
    clustering: list[str],
    size_bytes: int | None,
    recs: list[Recommendation],
) -> None:
    if not clustering:
        return
    if not size_bytes or size_bytes < LARGE_TABLE_THRESHOLD:
        return
    suspect = [c for c in clustering if _HIGH_CARDINALITY_KEY_PATTERNS.search(c)]
    if suspect:
        recs.append(
            Recommendation(
                severity=Severity.WARNING,
                category=Category.TABLE,
                title="High-cardinality clustering key",
                description=(
                    "Clustering column(s) appear to be unique identifiers. "
                    "Clustering on high-cardinality keys provides almost no scan reduction "
                    "because each file's min/max range spans the entire value space."
                ),
                action="Re-cluster on columns commonly used in filters (dates, status, region).",
                affected_tables=[table_name],
                per_table_actions={
                    table_name: f"ALTER TABLE {table_name} CLUSTER BY AUTO;\nOPTIMIZE {table_name};",
                },
                impact=5,
            )
        )


# ---------------------------------------------------------------------------
# D8: Wide table
# ---------------------------------------------------------------------------
def _check_wide_table(
    table_name: str,
    columns: list[ColumnInfo],
    recs: list[Recommendation],
) -> None:
    if not columns or len(columns) < WIDE_TABLE_COLUMN_THRESHOLD:
        return
    recs.append(
        Recommendation(
            severity=Severity.INFO,
            category=Category.DATA_MODELING,
            title="Wide table (many columns)",
            description=(
                "Table has a very large number of columns. Even in columnar stores "
                "like Delta Lake, wide tables incur metadata overhead per column and "
                "increase schema evolution complexity. Queries touching only a few columns "
                "still pay a cost for schema parsing."
            ),
            action=(
                "Consider splitting rarely-queried columns into a separate table "
                "joined by a shared key, or use SELECT with explicit column lists."
            ),
            affected_tables=[table_name],
            impact=3,
        )
    )


# ---------------------------------------------------------------------------
# D9: Non-Delta format
# ---------------------------------------------------------------------------
def _check_non_delta_format(
    table_name: str,
    table_format: str | None,
    recs: list[Recommendation],
) -> None:
    if not table_format:
        return
    if table_format.lower() == "delta":
        return
    recs.append(
        Recommendation(
            severity=Severity.WARNING,
            category=Category.STORAGE,
            title="Non-Delta format detected",
            description=(
                "Table uses a non-Delta format. "
                "Non-Delta tables miss out on ACID transactions, time travel, liquid "
                "clustering, Z-order, OPTIMIZE, VACUUM, and Photon-optimized reads."
            ),
            affected_tables=[table_name],
            per_table_actions={
                table_name: (
                    f"CREATE TABLE {table_name}_delta USING DELTA AS SELECT * FROM {table_name};\n"
                    "Or in-place: CONVERT TO DELTA (for Parquet tables)."
                ),
            },
            impact=6,
        )
    )


# ---------------------------------------------------------------------------
# D10: VACUUM / compaction needed
# ---------------------------------------------------------------------------
def _check_vacuum_needed(
    table_name: str,
    properties: dict[str, str],
    size_bytes: int | None,
    recs: list[Recommendation],
) -> None:
    if not size_bytes or size_bytes < 100 * 1024 * 1024:
        return
    vacuum_keys = {
        "delta.lastVacuumTimestamp",
        "delta.vacuum.lastVacuumTimestamp",
    }
    has_vacuum_history = any(k in properties for k in vacuum_keys)
    if not has_vacuum_history:
        recs.append(
            Recommendation(
                severity=Severity.INFO,
                category=Category.STORAGE,
                title="No evidence of VACUUM",
                description=(
                    "Table properties contain no vacuum timestamp. "
                    "Without periodic VACUUM, deleted file markers and old data versions "
                    "accumulate, increasing metadata overhead and slowing reads."
                ),
                action=(
                    "Set up a recurring job to vacuum tables regularly. "
                    "Default retention is 7 days (168 hours)."
                ),
                affected_tables=[table_name],
                per_table_actions={table_name: f"VACUUM {table_name};"},
                impact=4,
            )
        )


# ---------------------------------------------------------------------------
# D11: Inappropriate data types (dates/numbers stored as STRING)
# ---------------------------------------------------------------------------
def _check_inappropriate_data_types(
    table_name: str,
    columns: list[ColumnInfo],
    recs: list[Recommendation],
) -> None:
    bad_dates: list[str] = []
    bad_numbers: list[str] = []
    for col in columns:
        dtype = col.data_type.upper()
        if dtype not in ("STRING", "VARCHAR", "TEXT"):
            continue
        if _DATE_COLUMN_PATTERNS.search(col.name):
            bad_dates.append(col.name)
        elif _NUMERIC_COLUMN_PATTERNS.search(col.name):
            bad_numbers.append(col.name)

    if bad_dates:
        recs.append(
            Recommendation(
                severity=Severity.WARNING,
                category=Category.DATA_MODELING,
                title="Date columns stored as STRING",
                description=(
                    "Columns that appear to hold date/time values are typed as STRING. "
                    "This prevents partition pruning, data skipping via zone maps, "
                    "and proper sort ordering. Compression is also significantly worse."
                ),
                affected_tables=[table_name],
                per_table_actions={
                    table_name: (
                        f"ALTER TABLE {table_name} ALTER COLUMN <col> SET DATA TYPE "
                        "TIMESTAMP or DATE as appropriate."
                    ),
                },
                impact=3,
            )
        )

    if bad_numbers:
        recs.append(
            Recommendation(
                severity=Severity.WARNING,
                category=Category.DATA_MODELING,
                title="Numeric columns stored as STRING",
                description=(
                    "Columns that appear to hold numeric values are typed as STRING. "
                    "This prevents proper aggregation pushdown, zone-map-based "
                    "data skipping, and wastes storage due to poor compression."
                ),
                affected_tables=[table_name],
                per_table_actions={
                    table_name: (
                        f"ALTER TABLE {table_name} ALTER COLUMN <col> SET DATA TYPE "
                        "DECIMAL, DOUBLE, or BIGINT as appropriate."
                    ),
                },
                impact=3,
            )
        )


# ---------------------------------------------------------------------------
# D12: STRING columns that are likely enums / low-cardinality codes
# ---------------------------------------------------------------------------
def _check_string_enum_columns(
    table_name: str,
    columns: list[ColumnInfo],
    recs: list[Recommendation],
) -> None:
    suspect = [
        c.name
        for c in columns
        if c.data_type.upper() in ("STRING", "VARCHAR", "TEXT")
        and _ENUM_COLUMN_PATTERNS.search(c.name)
    ]
    if not suspect:
        return
    recs.append(
        Recommendation(
            severity=Severity.INFO,
            category=Category.DATA_MODELING,
            title="Possible low-cardinality STRING columns",
            description=(
                "Columns that appear to be categorical/enum values are stored as STRING. "
                "Low-cardinality STRING columns compress poorly compared to TINYINT or "
                "SMALLINT codes and increase shuffle/join overhead."
            ),
            action=(
                "Consider mapping to integer codes with a lookup table, or accept the "
                "trade-off if readability is more important than compression."
            ),
            affected_tables=[table_name],
            impact=2,
        )
    )


# ---------------------------------------------------------------------------
# D13: Large table without date-based clustering
# ---------------------------------------------------------------------------
def _check_large_table_no_date_clustering(
    table_name: str,
    clustering: list[str],
    partitions: list[str],
    size_bytes: int | None,
    columns: list[ColumnInfo],
    recs: list[Recommendation],
) -> None:
    if clustering or partitions:
        return
    if not size_bytes or size_bytes < LARGE_TABLE_THRESHOLD:
        return

    date_cols = [c.name for c in columns if _DATE_COLUMN_PATTERNS.search(c.name)]
    if not date_cols:
        return

    best_col = date_cols[0]
    recs.append(
        Recommendation(
            severity=Severity.INFO,
            category=Category.TABLE,
            title="Large table without date clustering",
            description=(
                "Table is large with no clustering and has date/time columns that would "
                "enable significant data skipping for time-range queries."
            ),
            affected_tables=[table_name],
            per_table_actions={
                table_name: (
                    f"ALTER TABLE {table_name} CLUSTER BY ({best_col});\n"
                    f"OPTIMIZE {table_name};"
                ),
            },
            impact=6,
        )
    )


# ---------------------------------------------------------------------------
# D14: Partition columns prone to data skew
# ---------------------------------------------------------------------------
def _check_partition_skew_risk(
    table_name: str,
    partitions: list[str],
    size_bytes: int | None,
    recs: list[Recommendation],
) -> None:
    """Warn when a large partitioned table uses columns with inherently skewed distributions."""
    if not partitions:
        return
    if not size_bytes or size_bytes < LARGE_TABLE_THRESHOLD:
        return

    skew_prone = [p for p in partitions if _SKEW_PRONE_PARTITION_PATTERNS.search(p)]
    if not skew_prone:
        return

    cols_str = ", ".join(skew_prone)
    recs.append(
        Recommendation(
            severity=Severity.WARNING,
            category=Category.TABLE,
            title="Partition columns prone to data skew",
            description=(
                f"Table is partitioned on column(s) ({cols_str}) that commonly have "
                "highly uneven data distributions. A few dominant values (e.g. one "
                "country or tenant) can create oversized partitions, leading to task "
                "skew during queries, uneven file sizes, and spill on hot partitions."
            ),
            action=(
                "Migrate to liquid clustering which handles skew more gracefully, "
                "or add a secondary partition column to spread the data more evenly."
            ),
            affected_tables=[table_name],
            per_table_actions={
                table_name: (
                    f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(partitions[:4])});\n"
                    f"OPTIMIZE {table_name};"
                ),
            },
            impact=6,
        )
    )


# ---------------------------------------------------------------------------
# D15: STRING columns likely storing JSON
# ---------------------------------------------------------------------------
def _check_json_string_columns(
    table_name: str,
    columns: list[ColumnInfo],
    recs: list[Recommendation],
) -> None:
    suspect: list[str] = []
    for col in columns:
        if col.data_type.upper() not in ("STRING", "VARCHAR", "TEXT"):
            continue
        if _JSON_COLUMN_PATTERNS.search(col.name):
            suspect.append(col.name)
        elif col.comment and any(
            kw in col.comment.lower() for kw in ("json", "variant")
        ):
            suspect.append(col.name)

    if not suspect:
        return

    recs.append(
        Recommendation(
            severity=Severity.INFO,
            category=Category.DATA_MODELING,
            title="STRING columns likely storing JSON",
            description=(
                "Columns appear to store JSON data as STRING. Querying these "
                "with get_json_object or from_json parses JSON on every row, breaks "
                "Photon vectorized execution, and prevents data skipping on nested fields."
            ),
            action="Migrate to VARIANT for native path access, then use col:path.to.field instead of get_json_object().",
            affected_tables=[table_name],
            per_table_actions={
                table_name: f"ALTER TABLE {table_name} ALTER COLUMN <col> SET DATA TYPE VARIANT;",
            },
            impact=5,
        )
    )


# ---------------------------------------------------------------------------
# D16: Hive-style partitioning — migrate to liquid clustering
# ---------------------------------------------------------------------------
def _check_hive_partitioning(
    table_name: str,
    partitions: list[str],
    clustering: list[str],
    recs: list[Recommendation],
) -> None:
    if not partitions:
        return
    if clustering:
        return

    part_cols = ", ".join(partitions[:4])
    recs.append(
        Recommendation(
            severity=Severity.INFO,
            category=Category.TABLE,
            title="Hive-style partitioning detected",
            description=(
                "Table uses Hive-style partitioning. Liquid clustering "
                "provides superior performance: it enables data skipping on any clustered "
                "column, adapts to changing query patterns, avoids small-file problems, "
                "and can be changed without rewriting the table."
            ),
            affected_tables=[table_name],
            per_table_actions={
                table_name: (
                    f"ALTER TABLE {table_name} CLUSTER BY ({part_cols});\n"
                    f"OPTIMIZE {table_name};\n"
                    "Or let Databricks choose optimal columns automatically:\n"
                    f"ALTER TABLE {table_name} CLUSTER BY AUTO;"
                ),
            },
            impact=6,
        )
    )


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
