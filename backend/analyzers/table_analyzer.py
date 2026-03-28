from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from backend.analyzers.sql_parser import ParsedQuery
from backend.db import execute_sql
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

_SAFE_TABLE_NAME_RE = re.compile(r"^[\w][\w.]*$")


def _is_safe_table_name(name: str) -> bool:
    return bool(_SAFE_TABLE_NAME_RE.match(name)) and len(name) <= 256


def fetch_table_detail(table_name: str) -> dict[str, Any] | None:
    if not _is_safe_table_name(table_name):
        logger.warning("Skipping DESCRIBE DETAIL for unsafe table name: %s", table_name[:100])
        return None
    try:
        rows = execute_sql(f"DESCRIBE DETAIL {table_name}")
        if rows:
            return rows[0]
    except Exception as exc:
        logger.warning("Failed to DESCRIBE DETAIL %s: %s", table_name, exc)
    return None


def fetch_table_columns(table_name: str) -> list[ColumnInfo]:
    """Fetch column names, types, and comments via DESCRIBE TABLE."""
    if not _is_safe_table_name(table_name):
        return []
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
            columns.append(ColumnInfo(
                name=col_name,
                data_type=data_type,
                comment=comment if comment else None,
            ))
        return columns
    except Exception as exc:
        logger.warning("Failed to DESCRIBE TABLE %s: %s", table_name, exc)
        return []


def analyze_tables(
    table_names: list[str],
    parsed_query: ParsedQuery,
) -> list[TableInfo]:
    fetchable = [n for n in table_names if not n.lower().startswith("system.")]
    system_tables = [n for n in table_names if n.lower().startswith("system.")]

    details_map: dict[str, dict[str, Any] | None] = {}
    columns_map: dict[str, list[ColumnInfo]] = {}
    if fetchable:
        with ThreadPoolExecutor(max_workers=min(len(fetchable) * 2, 16)) as pool:
            detail_futures = {
                pool.submit(fetch_table_detail, name): ("detail", name)
                for name in fetchable
            }
            column_futures = {
                pool.submit(fetch_table_columns, name): ("columns", name)
                for name in fetchable
            }
            all_futures = {**detail_futures, **column_futures}
            for future in as_completed(all_futures):
                kind, name = all_futures[future]
                try:
                    if kind == "detail":
                        details_map[name] = future.result()
                    else:
                        columns_map[name] = future.result()
                except Exception:
                    if kind == "detail":
                        details_map[name] = None
                    else:
                        columns_map[name] = []

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

        recs = _analyze_single_table(
            name, clustering, partitions, num_files, size_bytes,
            props if isinstance(props, dict) else {},
            parsed_query,
            table_format=table_format,
            columns=columns,
        )

        results.append(TableInfo(
            full_name=name,
            format=table_format,
            clustering_columns=clustering,
            partition_columns=partitions,
            num_files=num_files,
            size_in_bytes=size_bytes,
            column_count=len(columns) if columns else None,
            columns=columns,
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
    *,
    table_format: str | None = None,
    columns: list[ColumnInfo] | None = None,
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
            action=(
                f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(clean_cols)});\n"
                f"Alternatively, let Databricks choose automatically: "
                f"ALTER TABLE {table_name} CLUSTER BY AUTO;"
            ),
            impact=7,
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
                impact=6,
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
            impact=5,
        ))

    # D1: Table statistics staleness
    _check_stats_staleness(table_name, properties, recs)

    # D2: Over-partitioned tables
    _check_over_partitioned(table_name, partitions, num_files, size_bytes, recs)

    # D3: Large unorganized tables
    _check_large_unorganized(table_name, clustering, partitions, size_bytes, filter_cols_lower, recs)

    # D4: Join columns not clustered
    _check_join_columns_not_clustered(table_name, clustering_lower, join_cols_lower, recs)

    # D5: No clustering at all — suggest CLUSTER BY AUTO as baseline
    if not clustering and not table_name.lower().startswith("system."):
        already_has_clustering_rec = any(
            "No clustering" in r.title or "Large unorganized" in r.title for r in recs
        )
        if not already_has_clustering_rec:
            recs.append(Recommendation(
                severity=Severity.INFO,
                category=Category.TABLE,
                title=f"No clustering on {table_name}",
                description=(
                    f"Table {table_name} has no liquid clustering. Enabling clustering "
                    "improves data skipping, reduces scan volume, and speeds up queries — "
                    "even when you are unsure which columns to cluster on."
                ),
                action=f"ALTER TABLE {table_name} CLUSTER BY AUTO;",
                impact=5,
            ))

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
        table_name, clustering, partitions, size_bytes, columns or [], recs,
    )

    # D15: STRING columns likely storing JSON
    _check_json_string_columns(table_name, columns or [], recs)

    # D16: Hive-style partitioning — migrate to liquid clustering
    _check_hive_partitioning(table_name, partitions, clustering, recs)

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
            impact=4,
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
            impact=5,
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
    if filter_cols:
        cols_hint = ", ".join(sorted(filter_cols)[:4])
        action = (
            f"ALTER TABLE {table_name} CLUSTER BY ({cols_hint});\n"
            f"OPTIMIZE {table_name};\n"
            f"Alternatively, let Databricks choose automatically: "
            f"ALTER TABLE {table_name} CLUSTER BY AUTO;"
        )
    else:
        action = (
            f"ALTER TABLE {table_name} CLUSTER BY AUTO;\n"
            f"OPTIMIZE {table_name};"
        )
    recs.append(Recommendation(
        severity=Severity.WARNING,
        category=Category.TABLE,
        title=f"Large unorganized table: {table_name}",
        description=(
            f"Table is {size_gb:,.1f} GB with no clustering or partitioning. "
            "Every query must scan through unordered data, making data skipping impossible."
        ),
        action=action,
        impact=7,
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
            impact=6,
        ))


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
        size_tb = avg_partition_size / (1024 ** 4)
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.TABLE,
            title=f"Under-partitioned table: {table_name}",
            description=(
                f"Average partition size is ~{size_tb:.1f} TB, which is very large. "
                "Partitions this big limit the benefit of partition pruning."
            ),
            action=(
                f"Migrate from Hive-style partitioning to liquid clustering:\n"
                f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(partitions[:4])});\n"
                f"OPTIMIZE {table_name};"
            ),
            impact=5,
        ))


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
        cols = ", ".join(suspect)
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.TABLE,
            title=f"High-cardinality clustering key on {table_name}",
            description=(
                f"Clustering column(s) [{cols}] appear to be unique identifiers. "
                "Clustering on high-cardinality keys provides almost no scan reduction "
                "because each file's min/max range spans the entire value space."
            ),
            action=(
                f"Re-cluster on columns commonly used in filters (dates, status, region):\n"
                f"ALTER TABLE {table_name} CLUSTER BY AUTO;\nOPTIMIZE {table_name};"
            ),
            impact=5,
        ))


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
    recs.append(Recommendation(
        severity=Severity.INFO,
        category=Category.DATA_MODELING,
        title=f"Wide table with {len(columns)} columns: {table_name}",
        description=(
            f"Table {table_name} has {len(columns)} columns. Even in columnar stores "
            "like Delta Lake, wide tables incur metadata overhead per column and "
            "increase schema evolution complexity. Queries touching only a few columns "
            "still pay a cost for schema parsing."
        ),
        action=(
            "Consider splitting rarely-queried columns into a separate table "
            "joined by a shared key, or use SELECT with explicit column lists."
        ),
        impact=3,
    ))


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
    recs.append(Recommendation(
        severity=Severity.WARNING,
        category=Category.STORAGE,
        title=f"Non-Delta format ({table_format}) on {table_name}",
        description=(
            f"Table {table_name} uses {table_format} format instead of Delta Lake. "
            "Non-Delta tables miss out on ACID transactions, time travel, liquid "
            "clustering, Z-order, OPTIMIZE, VACUUM, and Photon-optimized reads."
        ),
        action=(
            f"Convert to Delta Lake:\n"
            f"CREATE TABLE {table_name}_delta USING DELTA AS SELECT * FROM {table_name};\n"
            "Or in-place: CONVERT TO DELTA (for Parquet tables)."
        ),
        impact=6,
    ))


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
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.STORAGE,
            title=f"No evidence of VACUUM on {table_name}",
            description=(
                f"Table properties for {table_name} contain no vacuum timestamp. "
                "Without periodic VACUUM, deleted file markers and old data versions "
                "accumulate, increasing metadata overhead and slowing reads."
            ),
            action=(
                f"VACUUM {table_name};\n"
                "Set up a recurring job to vacuum tables regularly. "
                "Default retention is 7 days (168 hours)."
            ),
            impact=4,
        ))


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
        cols = ", ".join(bad_dates[:5])
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.DATA_MODELING,
            title=f"Date columns stored as STRING on {table_name}",
            description=(
                f"Columns [{cols}] appear to hold date/time values but are typed as "
                "STRING. This prevents partition pruning, data skipping via zone maps, "
                "and proper sort ordering. Compression is also significantly worse."
            ),
            action=(
                "ALTER TABLE " + table_name + " ALTER COLUMN <col> SET DATA TYPE "
                "TIMESTAMP or DATE as appropriate."
            ),
            impact=3,
        ))

    if bad_numbers:
        cols = ", ".join(bad_numbers[:5])
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.DATA_MODELING,
            title=f"Numeric columns stored as STRING on {table_name}",
            description=(
                f"Columns [{cols}] appear to hold numeric values but are typed as "
                "STRING. This prevents proper aggregation pushdown, zone-map-based "
                "data skipping, and wastes storage due to poor compression."
            ),
            action=(
                "ALTER TABLE " + table_name + " ALTER COLUMN <col> SET DATA TYPE "
                "DECIMAL, DOUBLE, or BIGINT as appropriate."
            ),
            impact=3,
        ))


# ---------------------------------------------------------------------------
# D12: STRING columns that are likely enums / low-cardinality codes
# ---------------------------------------------------------------------------
def _check_string_enum_columns(
    table_name: str,
    columns: list[ColumnInfo],
    recs: list[Recommendation],
) -> None:
    suspect = [c.name for c in columns
               if c.data_type.upper() in ("STRING", "VARCHAR", "TEXT")
               and _ENUM_COLUMN_PATTERNS.search(c.name)]
    if not suspect:
        return
    cols = ", ".join(suspect[:5])
    recs.append(Recommendation(
        severity=Severity.INFO,
        category=Category.DATA_MODELING,
        title=f"Possible low-cardinality STRING columns on {table_name}",
        description=(
            f"Columns [{cols}] appear to be categorical/enum values stored as STRING. "
            "Low-cardinality STRING columns compress poorly compared to TINYINT or "
            "SMALLINT codes and increase shuffle/join overhead."
        ),
        action=(
            "Consider mapping to integer codes with a lookup table, or accept the "
            "trade-off if readability is more important than compression."
        ),
        impact=2,
    ))


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
    recs.append(Recommendation(
        severity=Severity.INFO,
        category=Category.TABLE,
        title=f"Large table without date clustering: {table_name}",
        description=(
            f"Table is {size_bytes / (1024 ** 3):,.1f} GB with no clustering. "
            f"Column '{best_col}' appears to be a date/time dimension — clustering "
            "on it would enable significant data skipping for time-range queries."
        ),
        action=(
            f"ALTER TABLE {table_name} CLUSTER BY ({best_col});\n"
            f"OPTIMIZE {table_name};"
        ),
        impact=6,
    ))


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
        elif col.comment and any(kw in col.comment.lower() for kw in ("json", "variant")):
            suspect.append(col.name)

    if not suspect:
        return

    cols = ", ".join(suspect[:5])
    recs.append(Recommendation(
        severity=Severity.INFO,
        category=Category.DATA_MODELING,
        title=f"STRING columns likely storing JSON on {table_name}",
        description=(
            f"Columns [{cols}] appear to store JSON data as STRING. Querying these "
            "with get_json_object or from_json parses JSON on every row, breaks "
            "Photon vectorized execution, and prevents data skipping on nested fields."
        ),
        action=(
            f"Migrate to VARIANT for native path access:\n"
            f"ALTER TABLE {table_name} ALTER COLUMN <col> SET DATA TYPE VARIANT;\n"
            "Then use col:path.to.field instead of get_json_object()."
        ),
        impact=5,
    ))


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
    recs.append(Recommendation(
        severity=Severity.INFO,
        category=Category.TABLE,
        title=f"Hive-style partitioning on {table_name} — consider liquid clustering",
        description=(
            f"Table uses Hive-style partitioning by [{part_cols}]. Liquid clustering "
            "provides superior performance: it enables data skipping on any clustered "
            "column, adapts to changing query patterns, avoids small-file problems, "
            "and can be changed without rewriting the table."
        ),
        action=(
            f"ALTER TABLE {table_name} CLUSTER BY ({part_cols});\n"
            f"OPTIMIZE {table_name};\n"
            "Or let Databricks choose optimal columns automatically:\n"
            f"ALTER TABLE {table_name} CLUSTER BY AUTO;"
        ),
        impact=6,
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
