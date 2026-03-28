from __future__ import annotations

import logging
from typing import Any, Callable

from backend.analyzers.plan_analyzer import analyze_plan
from backend.analyzers.query_metrics import analyze_query_metrics, build_query_metrics
from backend.analyzers.sql_parser import parse_query
from backend.analyzers.table_analyzer import analyze_tables
from backend.analyzers.warehouse_analyzer import analyze_warehouse
from backend.db import execute_sql, fetch_query_history_via_api
from backend.models import (
    AnalysisResult,
    Category,
    PlanSummary,
    Recommendation,
    Severity,
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

    # Step 7 — Finalise recommendations
    progress(6, STEPS[6], "running")
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

    # Strip leading block/line comments so we can inspect the real keyword
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


def _sql_pattern_recommendations(parsed: Any) -> list[Recommendation]:
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

    return recs
