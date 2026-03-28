from __future__ import annotations

import logging
import re

import sqlglot

from backend.db import execute_sql
from backend.models import AIRewriteResult, AnalysisResult

logger = logging.getLogger(__name__)

AI_MODEL = "databricks-claude-opus-4-6"


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
        "2. The rewritten query MUST be valid **Databricks SQL** syntax. Do not use "
        "syntax from other SQL dialects (e.g., PostgreSQL, MySQL, T-SQL).\n"
        "3. Only make changes that preserve the same result set.\n"
        "4. If no meaningful rewrite is possible, return the original query unchanged.\n"
        "5. Format your response as:\n"
        "OPTIMIZED SQL:\n```sql\n<your rewritten query>\n```\n"
        "EXPLANATION:\n<brief explanation of changes>\n"
        "\n## Databricks SQL Syntax Reminders\n"
        "- Column aliases in UNPIVOT / PIVOT must be backtick-quoted identifiers "
        "(e.g., `Total Revenue`), NOT single-quoted string literals ('Total Revenue').\n"
        "- Use backticks for identifiers containing spaces or special characters.\n"
        "- Single quotes are only for string literal values, never for aliases or identifiers.\n"
        "- LATERAL VIEW is not supported; use UNPIVOT or EXPLODE in a SELECT instead.\n"
        "- Use TIMESTAMPADD / TIMESTAMPDIFF instead of DATEADD / DATEDIFF with non-standard syntax.\n"
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

    syntax_valid, syntax_errors = _validate_sql(suggested_sql)

    return AIRewriteResult(
        original_sql=analysis.query_metrics.statement_text,
        suggested_sql=suggested_sql,
        explanation=explanation,
        syntax_valid=syntax_valid,
        syntax_errors=syntax_errors,
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


def _validate_sql(sql: str) -> tuple[bool, list[str]]:
    """Parse the SQL with sqlglot's Databricks dialect, then run Databricks-specific lints."""
    if not sql or not sql.strip():
        return False, ["Empty SQL statement"]

    errors: list[str] = []

    try:
        sqlglot.transpile(sql, read="databricks", error_level=sqlglot.ErrorLevel.RAISE)
    except sqlglot.errors.ParseError as exc:
        errors.extend(
            e.get("description", str(e)) if isinstance(e, dict) else str(e)
            for e in exc.errors
        ) if exc.errors else errors.append(str(exc))
    except Exception as exc:
        errors.append(str(exc))

    errors.extend(_lint_databricks_sql(sql))

    return (len(errors) == 0), errors


# Matches UNPIVOT/PIVOT blocks, then looks for single-quoted aliases inside them.
_PIVOT_UNPIVOT_RE = re.compile(
    r"\b(UNPIVOT|PIVOT)\s*\(", re.IGNORECASE,
)
_SINGLE_QUOTED_ALIAS_RE = re.compile(
    r"""(?:(?:AS|IN|FOR)\s*\()?\s*'([^']+)'""", re.IGNORECASE,
)


def _lint_databricks_sql(sql: str) -> list[str]:
    """Catch Databricks-specific issues that sqlglot's parser does not enforce."""
    warnings: list[str] = []

    for pivot_match in _PIVOT_UNPIVOT_RE.finditer(sql):
        start = pivot_match.start()
        depth = 0
        block_end = start
        for i in range(pivot_match.end() - 1, len(sql)):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    block_end = i + 1
                    break
        block = sql[start:block_end]
        keyword = pivot_match.group(1).upper()

        if _SINGLE_QUOTED_ALIAS_RE.search(block):
            warnings.append(
                f"Single-quoted string used as column alias in {keyword}. "
                "Databricks SQL requires backtick-quoted identifiers for aliases "
                "(e.g., `Total Revenue` instead of 'Total Revenue')."
            )

    return warnings


def _human_bytes(b: int | None) -> str:
    if b is None:
        return "unknown size"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024  # type: ignore[assignment]
    return f"{b:.1f} PB"
