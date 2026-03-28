from __future__ import annotations

import logging
from typing import Any, Callable

from backend.analyzers.plan_analyzer import analyze_plan
from backend.analyzers.query_metrics import analyze_query_metrics, build_query_metrics
from backend.analyzers.sql_parser import LARGE_IN_LIST_THRESHOLD, ParsedQuery, parse_query
from backend.analyzers.table_analyzer import analyze_tables
from backend.analyzers.warehouse_analyzer import analyze_warehouse
from backend.db import execute_sql, fetch_query_history_via_api
from backend.models import (
    AnalysisResult,
    Category,
    PlanSummary,
    QueryMetrics,
    Recommendation,
    Severity,
    TableInfo,
)

logger = logging.getLogger(__name__)

STEPS = [
    "Fetching query history",
    "Parsing SQL structure",
    "Analyzing execution metrics",
    "Analyzing tables",
    "Analyzing execution plan",
    "Analyzing warehouse config",
    "Generating recommendations",
]

ProgressCallback = Callable[[int, str, str], None]

SPILL_THRESHOLD_BYTES = 100 * 1024 * 1024  # matches query_metrics threshold


def _noop_progress(_step: int, _label: str, _status: str) -> None:
    pass


def run_analysis(
    statement_id: str,
    on_progress: ProgressCallback | None = None,
) -> AnalysisResult:
    """Full analysis pipeline for a given statement_id."""
    progress = on_progress or _noop_progress

    # Step 1 — Fetch query history
    progress(0, STEPS[0], "running")
    query_row = _fetch_query_history(statement_id)
    metrics = build_query_metrics(query_row)
    progress(0, STEPS[0], "done")

    all_recs: list[Recommendation] = []

    # Step 2 — Parse SQL structure (needed by metrics analysis for clustering suggestions)
    progress(1, STEPS[1], "running")
    parsed = parse_query(metrics.statement_text)
    sql_recs = _sql_pattern_recommendations(parsed)
    all_recs.extend(sql_recs)
    progress(1, STEPS[1], "done")

    # Step 3 — Analyze execution metrics
    progress(2, STEPS[2], "running")
    metric_recs = analyze_query_metrics(
        metrics, tables=parsed.tables, filter_columns=parsed.filter_columns,
    )
    all_recs.extend(metric_recs)
    progress(2, STEPS[2], "done")

    # Step 4 — Analyze tables
    progress(3, STEPS[3], "running")
    tables = analyze_tables(parsed.tables, parsed)
    for t in tables:
        all_recs.extend(t.recommendations)
    progress(3, STEPS[3], "done")

    # Step 5 — Execution plan (best-effort)
    progress(4, STEPS[4], "running")
    plan_summary = _try_explain(metrics.statement_text)
    if plan_summary:
        for w in plan_summary.warnings:
            all_recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.EXECUTION,
                title="Execution plan warning",
                description=w,
            ))
    progress(4, STEPS[4], "done")

    # Step 6 — Warehouse analysis
    progress(5, STEPS[5], "running")
    warehouse_info = None
    if metrics.warehouse_id:
        warehouse_info = analyze_warehouse(metrics.warehouse_id)
        all_recs.extend(warehouse_info.recommendations)
    progress(5, STEPS[5], "done")

    # Step 7 — Finalise recommendations (cross-correlations, then sort)
    progress(6, STEPS[6], "running")
    _cross_correlate(metrics, parsed, tables, plan_summary, all_recs)

    severity_order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
    all_recs.sort(key=lambda r: severity_order.get(r.severity, 99))
    progress(6, STEPS[6], "done")

    return AnalysisResult(
        query_metrics=metrics,
        tables=tables,
        plan_summary=plan_summary,
        warehouse=warehouse_info,
        recommendations=all_recs,
    )


# ---------------------------------------------------------------------------
# Cross-analyzer correlations (E1–E3)
# ---------------------------------------------------------------------------

def _cross_correlate(
    metrics: QueryMetrics,
    parsed: ParsedQuery,
    tables: list[TableInfo],
    plan_summary: PlanSummary | None,
    recs: list[Recommendation],
) -> None:
    """Produce correlated recommendations that span multiple analysis domains."""

    # E1: Spill + SortMergeJoin → specific broadcast hint recommendation
    has_spill = (
        metrics.spilled_local_bytes is not None
        and metrics.spilled_local_bytes > SPILL_THRESHOLD_BYTES
    )
    has_smj = plan_summary and "SortMergeJoin" in plan_summary.raw_plan
    if has_spill and has_smj:
        recs.append(Recommendation(
            severity=Severity.CRITICAL,
            category=Category.EXECUTION,
            title="Spill during SortMergeJoin",
            description=(
                "The query is spilling to disk during a SortMergeJoin. "
                "SortMergeJoin requires sorting both sides, which is memory-intensive. "
                "If one join side is significantly smaller, converting to a "
                "BroadcastHashJoin avoids the sort and reduces memory pressure."
            ),
            action=(
                "Add a broadcast hint on the smaller table: "
                "/*+ BROADCAST(small_table) */. Alternatively, pre-filter data "
                "before the join to reduce the volume, or use a larger warehouse."
            ),
        ))

    # E2: Poor pruning + unclustered table → consolidated recommendation
    has_poor_pruning = any(
        r.title == "Poor data skipping" for r in recs
    )
    unclustered_tables = [
        t.full_name for t in tables
        if not t.clustering_columns
        and not t.full_name.lower().startswith("system.")
        and t.size_in_bytes and t.size_in_bytes > 100 * 1024 * 1024
    ]
    if has_poor_pruning and unclustered_tables:
        table_list = ", ".join(unclustered_tables)
        filter_cols = sorted(set(parsed.filter_columns))[:4]
        cols_str = ", ".join(filter_cols) if filter_cols else "<filter columns>"
        recs.append(Recommendation(
            severity=Severity.CRITICAL,
            category=Category.EXECUTION,
            title="Poor data skipping on unclustered tables",
            description=(
                f"File pruning is ineffective and the following tables have no clustering: "
                f"{table_list}. This combination means every query must scan all files."
            ),
            action=(
                "Priority fix — add clustering and optimize:\n"
                + "\n".join(
                    f"ALTER TABLE {t} CLUSTER BY ({cols_str});\nOPTIMIZE {t};"
                    for t in unclustered_tables
                )
            ),
        ))

    # E3: High shuffle + join columns not clustered
    has_high_shuffle = any(
        r.title == "High shuffle volume" for r in recs
    )
    if has_high_shuffle and parsed.join_columns:
        join_cols_lower = {c.lower() for c in parsed.join_columns}
        tables_needing_clustering = []
        for t in tables:
            if t.full_name.lower().startswith("system."):
                continue
            clustering_lower = {c.lower() for c in t.clustering_columns}
            if clustering_lower and not join_cols_lower.intersection(clustering_lower):
                tables_needing_clustering.append(t.full_name)

        if tables_needing_clustering:
            join_cols_str = ", ".join(sorted(set(parsed.join_columns))[:4])
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.EXECUTION,
                title="High shuffle with misaligned clustering",
                description=(
                    f"Shuffle volume is high and tables "
                    f"[{', '.join(tables_needing_clustering)}] are not clustered on "
                    f"the join columns [{join_cols_str}]. Co-locating data on join "
                    "keys can eliminate or reduce shuffle."
                ),
                action=(
                    "Re-cluster tables on the join key columns:\n"
                    + "\n".join(
                        f"ALTER TABLE {t} CLUSTER BY ({join_cols_str});\nOPTIMIZE {t};"
                        for t in tables_needing_clustering
                    )
                ),
            ))


# ---------------------------------------------------------------------------
# Query history fetching
# ---------------------------------------------------------------------------

def _fetch_query_history(statement_id: str) -> dict[str, Any]:
    safe_id = statement_id.replace("'", "''")
    sql = (
        "SELECT * FROM system.query.history "
        f"WHERE statement_id = '{safe_id}' "
        "LIMIT 1"
    )
    try:
        rows = execute_sql(sql)
        if rows:
            return rows[0]
        logger.info("Statement %s not found in system.query.history, trying API", statement_id)
    except Exception as exc:
        logger.warning("system.query.history query failed (%s), trying API", exc)

    row = fetch_query_history_via_api(statement_id)
    if row:
        return row

    raise ValueError(f"No query found for statement_id: {statement_id}")


_EXPLAINABLE_PREFIXES = ("SELECT", "WITH", "FROM", "TABLE", "VALUES")


def _try_explain(statement_text: str) -> PlanSummary | None:
    trimmed = statement_text.strip().rstrip(";")

    check = trimmed
    while check.startswith("/*"):
        end = check.find("*/")
        if end == -1:
            break
        check = check[end + 2:].lstrip()
    while check.startswith("--"):
        newline = check.find("\n")
        if newline == -1:
            break
        check = check[newline + 1:].lstrip()

    first_word = check.split()[0].upper() if check.split() else ""
    if first_word not in _EXPLAINABLE_PREFIXES:
        logger.info("Skipping EXPLAIN for non-query statement starting with: %s", first_word)
        return None

    try:
        rows = execute_sql(f"EXPLAIN EXTENDED {trimmed}")
        plan_lines = []
        for row in rows:
            for val in row.values():
                if val:
                    plan_lines.append(str(val))
        raw_plan = "\n".join(plan_lines)
        if raw_plan:
            return analyze_plan(raw_plan)
    except Exception as exc:
        logger.warning("EXPLAIN failed: %s", exc)

    return None


# ---------------------------------------------------------------------------
# SQL pattern recommendations (wired from ParsedQuery flags)
# ---------------------------------------------------------------------------

def _sql_pattern_recommendations(parsed: ParsedQuery) -> list[Recommendation]:
    recs: list[Recommendation] = []

    if parsed.has_select_star:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="SELECT * used",
            description=(
                "Using SELECT * reads all columns from the table, including those you "
                "may not need. This increases I/O, memory usage, and network transfer."
            ),
            action="List only the columns you need explicitly.",
        ))

    if parsed.has_cross_join:
        recs.append(Recommendation(
            severity=Severity.CRITICAL,
            category=Category.QUERY,
            title="Cross join detected",
            description=(
                "A CROSS JOIN produces a cartesian product of both tables. "
                "This is extremely expensive and usually unintentional."
            ),
            action="Replace with an INNER/LEFT JOIN with an appropriate ON clause.",
        ))

    if parsed.missing_where and not parsed.has_limit:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="No WHERE clause or LIMIT",
            description=(
                "The query has no WHERE clause and no LIMIT. "
                "This will scan the entire table."
            ),
            action="Add a WHERE clause to filter data or a LIMIT to restrict rows.",
        ))

    if parsed.has_order_by_in_subquery:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="ORDER BY in subquery",
            description=(
                "An ORDER BY inside a subquery is usually unnecessary because "
                "the outer query does not guarantee row order from subqueries."
            ),
            action="Remove the ORDER BY from the subquery unless it is paired with LIMIT.",
        ))

    if parsed.has_function_on_filter_column:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Function applied to filter column",
            description=(
                "A function is applied to a column in the WHERE clause. "
                "This prevents predicate pushdown and data skipping."
            ),
            action=(
                "Rewrite the predicate to keep the column bare. "
                "E.g. replace YEAR(dt) = 2024 with dt >= '2024-01-01' AND dt < '2025-01-01'."
            ),
        ))

    if parsed.has_function_on_join_key:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="Function applied to join key",
            description=(
                "A function wraps a column used in a JOIN condition. "
                "This prevents the engine from using statistics or indexes for the join."
            ),
            action="Pre-compute the function result in a CTE or a generated column.",
        ))

    # --- New pattern recommendations ---

    if parsed.has_union_without_all:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="UNION used instead of UNION ALL",
            description=(
                "UNION implicitly adds a DISTINCT step, requiring a full sort and "
                "deduplication of the combined result set. This is significantly "
                "more expensive than UNION ALL."
            ),
            action=(
                "If duplicate rows are acceptable (or impossible given the data), "
                "replace UNION with UNION ALL to avoid the extra sort pass."
            ),
        ))

    if parsed.has_not_in_subquery:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="NOT IN with subquery",
            description=(
                "NOT IN (SELECT ...) can produce unexpected empty results when the "
                "subquery contains NULLs, and it prevents the optimizer from using "
                "efficient anti-join strategies."
            ),
            action=(
                "Rewrite using NOT EXISTS or a LEFT ANTI JOIN:\n"
                "  WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"
            ),
        ))

    if parsed.has_leading_wildcard_like:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="LIKE with leading wildcard",
            description=(
                "A LIKE pattern starting with '%' (e.g. LIKE '%value') prevents "
                "the engine from using data skipping or any index-like optimizations. "
                "Every row must be scanned and compared."
            ),
            action=(
                "If possible, restructure the filter to avoid leading wildcards. "
                "Consider using a computed column, full-text search, or reversing "
                "the string for suffix matching."
            ),
        ))

    if parsed.has_distinct:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="SELECT DISTINCT used",
            description=(
                "DISTINCT forces a full shuffle and deduplication across all output "
                "columns. This is expensive on large result sets."
            ),
            action=(
                "Consider whether GROUP BY achieves the same result more efficiently, "
                "or if an EXISTS subquery can replace the DISTINCT."
            ),
        ))

    if parsed.has_correlated_subquery:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Correlated subquery detected",
            description=(
                "A subquery references a column from the outer query, causing it to "
                "be evaluated once per outer row. This pattern can be extremely slow "
                "on large datasets."
            ),
            action=(
                "Rewrite the correlated subquery as a JOIN or use a window function. "
                "For example, replace a correlated scalar subquery with a LEFT JOIN "
                "and aggregate."
            ),
        ))

    if parsed.has_unpartitioned_window:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Window function without PARTITION BY",
            description=(
                "A window function operates over the entire dataset without a "
                "PARTITION BY clause. All rows are shuffled to a single partition, "
                "creating a severe bottleneck."
            ),
            action=(
                "Add a PARTITION BY clause to the window function to distribute "
                "the work across partitions. If a global window is truly required, "
                "consider pre-aggregating the data first."
            ),
        ))

    if parsed.large_in_list_count > 0:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title=f"Large IN list ({parsed.large_in_list_count} occurrence(s))",
            description=(
                f"The query contains {parsed.large_in_list_count} IN clause(s) with "
                f"{LARGE_IN_LIST_THRESHOLD}+ literal values. Large IN lists are slower "
                "than joins and harder to maintain."
            ),
            action=(
                "Move the values into a CTE or temporary view and JOIN against it:\n"
                "  WITH ids AS (SELECT * FROM VALUES (1), (2), ...) "
                "SELECT ... FROM main_table JOIN ids ON ..."
            ),
        ))

    if parsed.has_count_distinct:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="COUNT(DISTINCT ...) used",
            description=(
                "COUNT(DISTINCT col) requires a full shuffle and deduplication to "
                "compute the exact unique count. This is expensive on high-cardinality "
                "columns."
            ),
            action=(
                "If an approximate count is acceptable, use approx_count_distinct(col) "
                "which is significantly faster (HyperLogLog-based, ~2% error)."
            ),
        ))

    if parsed.has_complex_or_filter:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="Complex OR filter chain",
            description=(
                "The WHERE clause contains 3 or more OR branches. Complex OR "
                "conditions can prevent predicate pushdown and data skipping, as "
                "the engine may not be able to convert them into efficient scan filters."
            ),
            action=(
                "If the ORs are on the same column, rewrite as IN (...). "
                "If on different columns, consider splitting into UNION ALL of "
                "simpler queries, each with a single filter condition."
            ),
        ))

    return recs
