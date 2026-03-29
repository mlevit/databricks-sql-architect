from __future__ import annotations

import logging
from typing import Any, Callable

from backend.analyzers.plan_analyzer import analyze_plan
from backend.analyzers.query_metrics import analyze_query_metrics, build_query_metrics
from backend.analyzers.sql_parser import (
    DEEP_NESTING_THRESHOLD,
    DEEP_PAGINATION_OFFSET_THRESHOLD,
    HIGH_GROUP_BY_COLUMNS,
    LARGE_IN_LIST_THRESHOLD,
    ParsedQuery,
    parse_query,
)
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


_PLAN_WARNING_IMPACTS: list[tuple[str, int]] = [
    ("Cartesian product", 9),
    ("nested loop join", 9),
    ("Large fact-to-fact join", 8),
    ("Broadcast join with large table", 7),
    ("Full scan without filter pushdown", 7),
    ("SortMergeJoin", 6),
    ("without partition pruning", 6),
    ("exchange operations", 5),
    ("Data skew", 5),
    ("sort operations", 4),
]


def _plan_warning_impact(description: str) -> int:
    """Assign impact score to a plan warning based on known patterns."""
    desc_lower = description.lower()
    for pattern, impact in _PLAN_WARNING_IMPACTS:
        if pattern.lower() in desc_lower:
            return impact
    return 5


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
    metric_recs = analyze_query_metrics(metrics, tables=parsed.tables)
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
                impact=_plan_warning_impact(w),
            ))
    progress(4, STEPS[4], "done")

    # Step 6 — Warehouse analysis
    progress(5, STEPS[5], "running")
    warehouse_info = None
    if metrics.warehouse_id:
        warehouse_info = analyze_warehouse(metrics.warehouse_id)
        all_recs.extend(warehouse_info.recommendations)
    progress(5, STEPS[5], "done")

    # Step 7 — Finalise recommendations (cross-correlations, group, then sort)
    progress(6, STEPS[6], "running")
    _cross_correlate(metrics, parsed, tables, plan_summary, all_recs)
    all_recs = _deduplicate_clustering_recs(all_recs)
    all_recs = _group_recommendations(all_recs)

    severity_order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
    all_recs.sort(key=lambda r: (-r.impact, severity_order.get(r.severity, 99)))
    progress(6, STEPS[6], "done")

    return AnalysisResult(
        query_metrics=metrics,
        tables=tables,
        plan_summary=plan_summary,
        warehouse=warehouse_info,
        recommendations=all_recs,
    )


# ---------------------------------------------------------------------------
# Deduplicate overlapping clustering recommendations
# ---------------------------------------------------------------------------

_CLUSTERING_REC_TITLES = {"No clustering configured", "Large unorganized table"}


def _deduplicate_clustering_recs(recs: list[Recommendation]) -> list[Recommendation]:
    """Drop 'Poor data skipping' when a table-level clustering rec already exists.

    Both diagnose the same root cause (missing clustering) and suggest the same
    fix.  The table-level recommendation is more specific, so keep that one.
    """
    has_clustering_rec = any(r.title in _CLUSTERING_REC_TITLES for r in recs)
    if not has_clustering_rec:
        return recs

    return [r for r in recs if r.title != "Poor data skipping"]


# ---------------------------------------------------------------------------
# Recommendation grouping — merge per-table recs with the same title
# ---------------------------------------------------------------------------

def _group_recommendations(recs: list[Recommendation]) -> list[Recommendation]:
    """Merge recommendations that share the same title, severity, and category.

    Only recommendations with ``affected_tables`` populated are grouped; all
    others pass through unchanged.
    """
    from collections import OrderedDict

    grouped: OrderedDict[tuple, list[Recommendation]] = OrderedDict()
    result: list[Recommendation] = []

    for r in recs:
        if r.affected_tables:
            key = (r.title, r.severity, r.category)
            grouped.setdefault(key, []).append(r)
        else:
            result.append(r)

    for group in grouped.values():
        if len(group) == 1:
            result.append(group[0])
        else:
            all_tables = [t for r in group for t in r.affected_tables]
            all_per_table = {t: a for r in group for t, a in r.per_table_actions.items()}
            merged = Recommendation(
                severity=group[0].severity,
                category=group[0].category,
                title=group[0].title,
                description=group[0].description,
                action=group[0].action,
                snippet=group[0].snippet,
                impact=max(r.impact for r in group),
                affected_tables=all_tables,
                per_table_actions=all_per_table,
            )
            result.append(merged)

    return result


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
            impact=9,
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
        per_table: dict[str, str] = {}
        for t in unclustered_tables:
            if filter_cols:
                cols_str = ", ".join(filter_cols)
                per_table[t] = f"ALTER TABLE {t} CLUSTER BY ({cols_str});\nOPTIMIZE {t};"
            else:
                per_table[t] = f"ALTER TABLE {t} CLUSTER BY AUTO;\nOPTIMIZE {t};"
        recs.append(Recommendation(
            severity=Severity.CRITICAL,
            category=Category.EXECUTION,
            title="Poor data skipping on unclustered tables",
            description=(
                f"File pruning is ineffective and the following tables have no clustering: "
                f"{table_list}. This combination means every query must scan all files."
            ),
            action="Priority fix — add clustering and optimize:",
            affected_tables=unclustered_tables,
            per_table_actions=per_table,
            impact=10,
        ))
        unclustered_set = {t.lower() for t in unclustered_tables}
        recs[:] = [
            r for r in recs
            if not (
                r.title == "Poor data skipping"
                or (
                    r.title == "No clustering configured"
                    and any(t.lower() in unclustered_set for t in r.affected_tables)
                )
            )
        ]

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
                impact=8,
            ))

    # E4: Missing join filters — plan shows no pushdown + SQL has joins
    has_no_pushdown = any(
        "Full scan without filter pushdown" in r.description for r in recs
    )
    has_joins = bool(parsed.joins)
    if has_no_pushdown and has_joins and metrics.read_bytes and metrics.read_bytes > 500 * 1024 * 1024:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.EXECUTION,
            title="Joins without pre-join filter pushdown",
            description=(
                "The query performs joins but filters are not being pushed down to "
                "scan operators. This forces the engine to read and shuffle full "
                "tables before applying filters, dramatically increasing I/O and "
                "network transfer."
            ),
            action=(
                "Add WHERE filters that can be applied before the join, or "
                "restructure the query so predicates appear in the same scope as "
                "the table they filter. Ensure filter columns are not wrapped in "
                "functions that prevent pushdown."
            ),
            impact=8,
        ))

    # E5: Window PARTITION BY column not in table clustering
    if parsed.window_partition_columns:
        win_cols_lower = {c.lower() for c in parsed.window_partition_columns}
        for t in tables:
            if t.full_name.lower().startswith("system."):
                continue
            if not t.clustering_columns:
                continue
            clustering_lower = {c.lower() for c in t.clustering_columns}
            if not win_cols_lower.intersection(clustering_lower):
                win_cols_str = ", ".join(sorted(set(parsed.window_partition_columns))[:4])
                recs.append(Recommendation(
                    severity=Severity.INFO,
                    category=Category.EXECUTION,
                    title="Window PARTITION BY not aligned with clustering",
                    description=(
                        "Window functions partition by columns that are not covered by "
                        "the table's clustering key. This forces a full redistribute "
                        "and sort for the window operation."
                    ),
                    action="If this window pattern is common, include the partition column(s) in the clustering key.",
                    affected_tables=[t.full_name],
                    per_table_actions={
                        t.full_name: f"ALTER TABLE {t.full_name} CLUSTER BY ({win_cols_str});",
                    },
                    impact=4,
                ))
                break


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

def _first_snippet(parsed: ParsedQuery, key: str) -> str | None:
    """Return the first captured SQL snippet for a check, or None."""
    snippets = parsed.snippets.get(key)
    return snippets[0] if snippets else None


def _sql_pattern_recommendations(parsed: ParsedQuery) -> list[Recommendation]:
    recs: list[Recommendation] = []

    if parsed.has_select_star:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="SELECT * used",
            description=(
                "Using SELECT * reads all columns from the table, including those you "
                "may not need. In Delta Lake's columnar format, each extra column is "
                "an additional I/O operation. This also increases memory usage, shuffle "
                "volume, and network transfer."
            ),
            action="List only the columns you need explicitly.",
            impact=3,
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
            snippet=_first_snippet(parsed, "has_cross_join"),
            impact=10,
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
            impact=5,
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
            impact=2,
        ))

    if parsed.has_function_on_filter_column:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Function applied to filter column",
            description=(
                "A function is applied to a column in the WHERE clause. "
                "This prevents Delta Lake data skipping (zone map pruning), "
                "partition pruning, and Photon predicate pushdown."
            ),
            action=(
                "Rewrite the predicate to keep the column bare. "
                "E.g. replace YEAR(dt) = 2024 with dt >= '2024-01-01' AND dt < '2025-01-01'."
            ),
            snippet=_first_snippet(parsed, "has_function_on_filter_column"),
            impact=7,
        ))

    if parsed.has_function_on_join_key:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="Function applied to join key",
            description=(
                "A function wraps a column used in a JOIN condition. "
                "This prevents the optimizer from using clustering or distribution "
                "keys for co-located joins, forcing a full shuffle."
            ),
            action="Pre-compute the function result in a CTE or a generated column.",
            snippet=_first_snippet(parsed, "has_function_on_join_key"),
            impact=6,
        ))

    # --- New pattern recommendations ---

    if parsed.has_union_without_all:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="UNION used instead of UNION ALL",
            description=(
                "UNION implicitly adds a DISTINCT step, requiring a full shuffle, "
                "sort, and deduplication of the combined result set. On Databricks "
                "this is significantly more expensive than UNION ALL, especially for "
                "large analytical result sets."
            ),
            action=(
                "If duplicate rows are acceptable (or impossible given the data), "
                "replace UNION with UNION ALL to avoid the extra sort pass."
            ),
            snippet=_first_snippet(parsed, "has_union_without_all"),
            impact=5,
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
            snippet=_first_snippet(parsed, "has_not_in_subquery"),
            impact=5,
        ))

    if parsed.has_leading_wildcard_like:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="LIKE with leading wildcard",
            description=(
                "A LIKE pattern starting with '%' (e.g. LIKE '%value') prevents "
                "Delta Lake data skipping and zone map pruning. Every file and row "
                "must be scanned and compared."
            ),
            action=(
                "If possible, restructure the filter to avoid leading wildcards. "
                "Consider using a computed column, CONTAINS(), or reversing "
                "the string for suffix matching."
            ),
            snippet=_first_snippet(parsed, "has_leading_wildcard_like"),
            impact=6,
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
            impact=3,
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
            snippet=_first_snippet(parsed, "has_correlated_subquery"),
            impact=8,
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
            snippet=_first_snippet(parsed, "has_unpartitioned_window"),
            impact=7,
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
            impact=3,
        ))

    if parsed.has_count_distinct:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="COUNT(DISTINCT ...) used",
            description=(
                "COUNT(DISTINCT col) requires a full shuffle and deduplication to "
                "compute the exact unique count. This is expensive on high-cardinality "
                "columns across billions of rows."
            ),
            action=(
                "If an approximate count is acceptable, use APPROX_COUNT_DISTINCT(col) "
                "which is significantly faster on Databricks (HyperLogLog-based, ~2% error)."
            ),
            snippet=_first_snippet(parsed, "has_count_distinct"),
            impact=3,
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
            impact=4,
        ))

    if parsed.has_scalar_subquery_in_select:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Scalar subquery in SELECT (N+1 pattern)",
            description=(
                "A subquery in the SELECT list is evaluated once per outer row, "
                "similar to the N+1 query problem. This causes repeated correlated "
                "lookups instead of a single efficient JOIN."
            ),
            action=(
                "Rewrite the scalar subquery as a LEFT JOIN with aggregation. "
                "For example, replace:\n"
                "  SELECT (SELECT MAX(price) FROM orders WHERE orders.cid = c.id) FROM customers c\n"
                "with:\n"
                "  SELECT o.max_price FROM customers c "
                "LEFT JOIN (SELECT cid, MAX(price) AS max_price FROM orders GROUP BY cid) o "
                "ON c.id = o.cid"
            ),
            snippet=_first_snippet(parsed, "has_scalar_subquery_in_select"),
            impact=8,
        ))

    if parsed.has_distinct_with_joins:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="DISTINCT used to mask fan-out join",
            description=(
                "SELECT DISTINCT is used alongside a JOIN, which often indicates the "
                "join is producing duplicate rows due to a many-to-many or "
                "one-to-many relationship. The DISTINCT hides the fan-out instead of "
                "fixing the root cause."
            ),
            action=(
                "Review the join conditions to ensure they produce a true one-to-one "
                "match. If duplicates arise from a one-to-many relationship, "
                "pre-aggregate the many-side before joining, or use a semi-join "
                "(EXISTS / IN) if you only need existence checks."
            ),
            impact=6,
        ))

    if parsed.repeated_union_all_tables:
        table_list = ", ".join(parsed.repeated_union_all_tables)
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Repeated table scan in UNION ALL",
            description=(
                f"Table(s) [{table_list}] appear in multiple UNION ALL branches, "
                "causing the same data to be scanned repeatedly. Each branch triggers "
                "a full read of the table."
            ),
            action=(
                "Rewrite using one-pass conditional aggregation with CASE WHEN or "
                "the FILTER clause:\n"
                "  SELECT\n"
                "    COUNT(CASE WHEN status = 'A' THEN 1 END) AS count_a,\n"
                "    COUNT(CASE WHEN status = 'B' THEN 1 END) AS count_b\n"
                "  FROM table_name"
            ),
            impact=6,
        ))

    if parsed.max_nesting_depth >= DEEP_NESTING_THRESHOLD:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title=f"Deeply nested subqueries (depth {parsed.max_nesting_depth})",
            description=(
                f"The query has {parsed.max_nesting_depth} levels of subquery nesting. "
                "Deep nesting creates optimization barriers — the Databricks SQL "
                "optimizer may not be able to push predicates through or reorder "
                "operations across nesting boundaries. Stacked views are a common cause."
            ),
            action=(
                "Flatten nested subqueries into CTEs or JOINs. Each CTE is "
                "independently optimizable, and the planner can often merge them. "
                "Also verify that views referenced in the query aren't adding "
                "hidden layers of nesting."
            ),
            impact=4,
        ))

    if parsed.has_implicit_cast_in_predicate:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Type cast on column in predicate",
            description=(
                "A CAST or TRY_CAST wraps a column in a WHERE or JOIN condition. "
                "This defeats Delta Lake data skipping and Photon predicate pushdown "
                "because the engine must evaluate the cast for every row before filtering."
            ),
            action=(
                "Cast the literal/parameter side instead of the column. "
                "For example, replace WHERE CAST(id AS STRING) = '123' with "
                "WHERE id = 123. If types are mismatched across tables, align "
                "the schema so casts are unnecessary."
            ),
            snippet=_first_snippet(parsed, "has_implicit_cast_in_predicate"),
            impact=7,
        ))

    if parsed.has_or_different_columns:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="OR across different columns",
            description=(
                "The WHERE clause uses OR to combine conditions on different columns. "
                "This prevents Delta Lake partition pruning and zone-map-based file "
                "skipping because the engine cannot narrow down which files to read "
                "for either condition."
            ),
            action=(
                "Split the query into separate queries joined with UNION ALL, each "
                "filtering on a single column. This allows each branch to leverage "
                "data skipping independently:\n"
                "  SELECT ... WHERE col_a = 1\n"
                "  UNION ALL\n"
                "  SELECT ... WHERE col_b = 2"
            ),
            snippet=_first_snippet(parsed, "has_or_different_columns"),
            impact=5,
        ))

    if parsed.has_missing_join_predicate:
        recs.append(Recommendation(
            severity=Severity.CRITICAL,
            category=Category.QUERY,
            title="Join without predicate (implicit cross join)",
            description=(
                "A JOIN is used without an ON or USING clause and is not an explicit "
                "CROSS JOIN. This produces a silent cartesian product, likely yielding "
                "incorrect and explosively large results."
            ),
            action="Add an ON clause with the correct join key columns.",
            snippet=_first_snippet(parsed, "has_missing_join_predicate"),
            impact=10,
        ))

    if parsed.has_order_by_without_limit:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="ORDER BY without LIMIT",
            description=(
                "The query sorts the full result set without a LIMIT. Sorting is "
                "an expensive full-shuffle operation, and without a LIMIT the engine "
                "must materialize and sort every row before returning results."
            ),
            action=(
                "Add a LIMIT clause if only the top/bottom N rows are needed. "
                "If the full sorted result is required, consider whether the client "
                "or downstream process can handle ordering instead."
            ),
            snippet=_first_snippet(parsed, "has_order_by_without_limit"),
            impact=4,
        ))

    if parsed.group_by_column_count >= HIGH_GROUP_BY_COLUMNS:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title=f"GROUP BY on {parsed.group_by_column_count} columns",
            description=(
                f"The query groups by {parsed.group_by_column_count} columns, which "
                "likely produces a very high-cardinality grouping key. High cardinality "
                "means most groups contain only one row, making the aggregation "
                "expensive with little reduction in data volume."
            ),
            action=(
                "Review whether all GROUP BY columns are necessary. Consider "
                "grouping on fewer high-level dimensions and using window functions "
                "for detail-level calculations."
            ),
            impact=3,
        ))

    # --- A19–A25: New Databricks-tuned recommendations ---

    if parsed.has_having_without_agg:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="HAVING clause filters non-aggregated data",
            description=(
                "A HAVING clause contains conditions that do not reference any "
                "aggregate function. These filters could have been applied in the "
                "WHERE clause before aggregation, forcing the engine to aggregate "
                "rows it will ultimately discard."
            ),
            action=(
                "Move non-aggregate conditions from HAVING to the WHERE clause. "
                "This allows Databricks to filter rows before the aggregation step, "
                "reducing shuffle volume and memory usage."
            ),
            snippet=_first_snippet(parsed, "has_having_without_agg"),
            impact=4,
        ))

    if parsed.has_deep_pagination_offset:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title=f"Deep pagination with OFFSET >= {DEEP_PAGINATION_OFFSET_THRESHOLD}",
            description=(
                "The query uses a large OFFSET value for pagination. OLAP engines "
                "like Databricks SQL must scan, sort, and discard all rows before the "
                "offset, making deep pagination increasingly expensive."
            ),
            action=(
                "Switch to keyset (seek) pagination using a WHERE clause on the "
                "last-seen sort key value:\n"
                "  WHERE id > :last_seen_id ORDER BY id LIMIT :page_size\n"
                "Alternatively, materialize results into a temporary view and "
                "paginate over it."
            ),
            snippet=_first_snippet(parsed, "has_deep_pagination_offset"),
            impact=5,
        ))

    if parsed.has_exact_percentile:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="Exact percentile calculation used",
            description=(
                "PERCENTILE_CONT / PERCENTILE_DISC require a full sort of the data "
                "to compute exact quantiles. On large datasets this is very expensive."
            ),
            action=(
                "If an approximate result is acceptable, use "
                "APPROX_PERCENTILE(col, percentage [, accuracy]) which uses a "
                "t-digest algorithm and is significantly faster on Databricks."
            ),
            snippet=_first_snippet(parsed, "has_exact_percentile"),
            impact=3,
        ))

    if parsed.has_non_equi_join:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="Non-equality (theta) join detected",
            description=(
                "A JOIN condition uses only range operators (>, <, >=, <=, !=) "
                "without an equality predicate. Databricks cannot use a "
                "BroadcastHashJoin or ShuffledHashJoin for this pattern and must "
                "fall back to a SortMergeJoin or BroadcastNestedLoopJoin, which "
                "are significantly slower."
            ),
            action=(
                "If possible, add an equality condition to the join (e.g. a bucketed "
                "date key) and apply the range condition as a post-filter. Consider "
                "pre-bucketing one side of the join to convert the range into equality."
            ),
            snippet=_first_snippet(parsed, "has_non_equi_join"),
            impact=7,
        ))

    if parsed.has_count_star_for_existence:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="COUNT(*) may be used for existence check",
            description=(
                "The query appears to use SELECT COUNT(*) to check whether rows "
                "exist. On Databricks this can scan billions of rows when a simple "
                "existence check would short-circuit after the first match."
            ),
            action=(
                "Replace with SELECT 1 FROM table WHERE ... LIMIT 1, or use "
                "EXISTS (SELECT 1 FROM table WHERE ...) in a subquery."
            ),
            snippet=_first_snippet(parsed, "has_count_star_for_existence"),
            impact=4,
        ))

    if parsed.has_possible_udf:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.QUERY,
            title="Possible UDF detected",
            description=(
                "The query calls a function that appears to be a user-defined "
                "function (UDF). Python and Java UDFs break Photon vectorized "
                "execution and force row-by-row processing, which can be orders "
                "of magnitude slower than native SQL."
            ),
            action=(
                "Rewrite the UDF logic using built-in SQL functions or Spark "
                "higher-order functions (TRANSFORM, FILTER, AGGREGATE). If a UDF "
                "is unavoidable, consider a Scala/Java UDF over Python for better "
                "performance, or use Pandas UDFs (vectorized UDFs) for batch processing."
            ),
            snippet=_first_snippet(parsed, "has_possible_udf"),
            impact=6,
        ))

    if parsed.has_string_json_parsing:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.QUERY,
            title="String-based JSON parsing instead of VARIANT",
            description=(
                "The query uses legacy JSON string functions (get_json_object, "
                "from_json, json_tuple, etc.) to extract values from JSON strings. "
                "These functions parse JSON on every row, break Photon vectorized "
                "execution, and prevent predicate pushdown into nested fields."
            ),
            action=(
                "Migrate JSON STRING columns to the VARIANT data type and use "
                "native : path syntax for extraction:\n"
                "  -- Instead of: get_json_object(col, '$.customer.name')\n"
                "  -- Use:        col:customer.name\n"
                "  ALTER TABLE t ALTER COLUMN json_col SET DATA TYPE VARIANT;\n"
                "For ad-hoc queries, wrap with PARSE_JSON(): "
                "PARSE_JSON(json_col):customer.name"
            ),
            snippet=_first_snippet(parsed, "has_string_json_parsing"),
            impact=6,
        ))

    return recs
