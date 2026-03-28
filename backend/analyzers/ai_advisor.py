from __future__ import annotations

import logging

from backend.db import execute_sql
from backend.models import AIRewriteResult, AnalysisResult

logger = logging.getLogger(__name__)

AI_MODEL = "databricks-claude-sonnet-4-5"


def build_rewrite_prompt(analysis: AnalysisResult) -> str:
    parts = [
        "You are a Databricks SQL performance expert. Analyze the following query and the "
        "performance issues identified, then provide an optimized rewrite of the SQL.\n",
        "## Original SQL\n```sql",
        analysis.query_metrics.statement_text,
        "```\n",
        "## Identified Issues\n",
    ]

    for rec in analysis.recommendations:
        parts.append(f"- [{rec.severity.value.upper()}] {rec.title}: {rec.description}")

    if analysis.tables:
        parts.append("\n## Table Details\n")
        for t in analysis.tables:
            parts.append(f"- **{t.full_name}**: {t.num_files or '?'} files, "
                         f"{_human_bytes(t.size_in_bytes)}, "
                         f"clustering={t.clustering_columns or 'none'}, "
                         f"partitions={t.partition_columns or 'none'}")

    parts.append(
        "\n## Instructions\n"
        "1. Rewrite the SQL query to address the issues above.\n"
        "2. Only make changes that preserve the same result set.\n"
        "3. If no meaningful rewrite is possible, return the original query unchanged.\n"
        "4. Format your response as:\n"
        "OPTIMIZED SQL:\n```sql\n<your rewritten query>\n```\n"
        "EXPLANATION:\n<brief explanation of changes>\n"
    )

    return "\n".join(parts)


def rewrite_query(analysis: AnalysisResult) -> AIRewriteResult:
    prompt = build_rewrite_prompt(analysis)
    escaped = prompt.replace("'", "''")

    sql = f"SELECT ai_query('{AI_MODEL}', '{escaped}') AS suggestion"

    try:
        rows = execute_sql(sql)
    except Exception as exc:
        logger.error("ai_query failed: %s", exc)
        return AIRewriteResult(
            original_sql=analysis.query_metrics.statement_text,
            suggested_sql=analysis.query_metrics.statement_text,
            explanation=f"AI rewrite failed: {exc}",
        )

    raw_response = ""
    if rows and rows[0].get("suggestion"):
        raw_response = rows[0]["suggestion"]

    suggested_sql, explanation = _parse_ai_response(
        raw_response, analysis.query_metrics.statement_text
    )

    return AIRewriteResult(
        original_sql=analysis.query_metrics.statement_text,
        suggested_sql=suggested_sql,
        explanation=explanation,
    )


def _parse_ai_response(response: str, original_sql: str) -> tuple[str, str]:
    """Extract optimized SQL and explanation from the AI response."""
    suggested_sql = original_sql
    explanation = response

    sql_start = response.find("```sql")
    if sql_start != -1:
        sql_start += len("```sql")
        sql_end = response.find("```", sql_start)
        if sql_end != -1:
            suggested_sql = response[sql_start:sql_end].strip()

    expl_marker = "EXPLANATION:"
    expl_idx = response.find(expl_marker)
    if expl_idx != -1:
        explanation = response[expl_idx + len(expl_marker):].strip()
        code_block = explanation.find("```")
        if code_block != -1 and code_block < 10:
            explanation = explanation[explanation.find("```", code_block + 3) + 3:].strip()
    elif "```" in response:
        last_fence = response.rfind("```")
        tail = response[last_fence + 3:].strip()
        if tail:
            explanation = tail

    return suggested_sql, explanation


def _human_bytes(b: int | None) -> str:
    if b is None:
        return "unknown size"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024  # type: ignore[assignment]
    return f"{b:.1f} PB"
