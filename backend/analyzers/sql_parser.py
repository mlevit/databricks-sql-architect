from __future__ import annotations

import logging
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

LARGE_IN_LIST_THRESHOLD = 50


@dataclass
class ParsedQuery:
    tables: list[str] = field(default_factory=list)
    joins: list[JoinInfo] = field(default_factory=list)
    has_select_star: bool = False
    has_order_by_in_subquery: bool = False
    has_cross_join: bool = False
    missing_where: bool = False
    filter_columns: list[str] = field(default_factory=list)
    join_columns: list[str] = field(default_factory=list)
    has_function_on_filter_column: bool = False
    has_function_on_join_key: bool = False
    has_limit: bool = False
    # Per-table filter column mapping: table_name -> [col1, col2, ...]
    table_filter_columns: dict[str, list[str]] = field(default_factory=dict)
    # New pattern flags
    has_union_without_all: bool = False
    has_not_in_subquery: bool = False
    has_leading_wildcard_like: bool = False
    has_distinct: bool = False
    has_correlated_subquery: bool = False
    has_unpartitioned_window: bool = False
    large_in_list_count: int = 0
    has_count_distinct: bool = False
    has_complex_or_filter: bool = False


@dataclass
class JoinInfo:
    join_type: str
    left_table: str | None = None
    right_table: str | None = None
    on_columns: list[str] = field(default_factory=list)


def parse_query(sql: str) -> ParsedQuery:
    result = ParsedQuery()
    try:
        parsed = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:
        logger.warning("sqlglot failed to parse query, returning empty result")
        return result

    if not parsed:
        return result

    for statement in parsed:
        if statement is None:
            continue
        _extract_tables(statement, result)
        _check_select_star(statement, result)
        _extract_joins(statement, result)
        _check_where(statement, result)
        _extract_filters(statement, result)
        _check_order_in_subquery(statement, result)
        _check_limit(statement, result)
        _check_union_without_all(statement, result)
        _check_not_in_subquery(statement, result)
        _check_leading_wildcard_like(statement, result)
        _check_distinct(statement, result)
        _check_correlated_subquery(statement, result)
        _check_unpartitioned_window(statement, result)
        _check_large_in_list(statement, result)
        _check_count_distinct(statement, result)
        _check_complex_or_filter(statement, result)

    result.tables = list(dict.fromkeys(result.tables))
    return result


def _extract_tables(node: exp.Expression, result: ParsedQuery) -> None:
    virtual_names: set[str] = set()

    for cte in node.find_all(exp.CTE):
        alias = cte.args.get("alias")
        if alias and isinstance(alias, exp.TableAlias):
            virtual_names.add(alias.name.lower())
        elif alias:
            virtual_names.add(str(alias).lower())

    for subq in node.find_all(exp.Subquery):
        alias = subq.args.get("alias")
        if alias and isinstance(alias, exp.TableAlias):
            virtual_names.add(alias.name.lower())
        elif alias:
            virtual_names.add(str(alias).lower())

    for table in node.find_all(exp.Table):
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        if table.name:
            parts.append(table.name)
        if not parts:
            continue

        full_name = ".".join(parts)

        if full_name.lower() in virtual_names:
            continue

        result.tables.append(full_name)


def _check_select_star(node: exp.Expression, result: ParsedQuery) -> None:
    for select in node.find_all(exp.Select):
        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                result.has_select_star = True
                return


def _extract_joins(node: exp.Expression, result: ParsedQuery) -> None:
    for join in node.find_all(exp.Join):
        join_type = join.side or ""
        kind = join.kind or ""
        full_type = f"{join_type} {kind}".strip().upper() or "INNER"

        if kind and kind.upper() == "CROSS":
            result.has_cross_join = True

        right_table = None
        table_node = join.find(exp.Table)
        if table_node:
            parts = []
            if table_node.catalog:
                parts.append(table_node.catalog)
            if table_node.db:
                parts.append(table_node.db)
            if table_node.name:
                parts.append(table_node.name)
            right_table = ".".join(parts) if parts else None

        on_cols: list[str] = []
        on_clause = join.args.get("on")
        if on_clause:
            for col in on_clause.find_all(exp.Column):
                on_cols.append(col.name)
                result.join_columns.append(col.name)
                if _is_wrapped_in_function(col):
                    result.has_function_on_join_key = True

        result.joins.append(JoinInfo(
            join_type=full_type,
            right_table=right_table,
            on_columns=on_cols,
        ))


def _check_where(node: exp.Expression, result: ParsedQuery) -> None:
    """Check if the outermost query lacks a WHERE clause."""
    top_select = node.find(exp.Select)
    if not top_select:
        return
    parent_query = top_select.parent
    if parent_query and not parent_query.find(exp.Where):
        result.missing_where = True


def _extract_filters(node: exp.Expression, result: ParsedQuery) -> None:
    """Extract filter columns and build per-table filter column mapping."""
    table_alias_map = _build_table_alias_map(node)

    for where in node.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            col_name = col.name
            result.filter_columns.append(col_name)
            if _is_wrapped_in_function(col):
                result.has_function_on_filter_column = True

            table_ref = col.table
            if table_ref:
                resolved = table_alias_map.get(table_ref.lower(), table_ref)
                result.table_filter_columns.setdefault(resolved, []).append(col_name)


def _build_table_alias_map(node: exp.Expression) -> dict[str, str]:
    """Map table aliases to their full qualified names."""
    alias_map: dict[str, str] = {}
    for table in node.find_all(exp.Table):
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        if table.name:
            parts.append(table.name)
        if not parts:
            continue
        full_name = ".".join(parts)
        alias = table.alias
        if alias:
            alias_map[alias.lower()] = full_name
    return alias_map


def _check_order_in_subquery(node: exp.Expression, result: ParsedQuery) -> None:
    for subquery in node.find_all(exp.Subquery):
        if subquery.find(exp.Order):
            result.has_order_by_in_subquery = True
            return


def _check_limit(node: exp.Expression, result: ParsedQuery) -> None:
    if node.find(exp.Limit):
        result.has_limit = True


# ---------------------------------------------------------------------------
# A1: UNION without ALL
# ---------------------------------------------------------------------------
def _check_union_without_all(node: exp.Expression, result: ParsedQuery) -> None:
    for union in node.find_all(exp.Union):
        if not union.args.get("distinct") is False:
            if not isinstance(union, exp.Intersect) and not isinstance(union, exp.Except):
                result.has_union_without_all = True
                return


# ---------------------------------------------------------------------------
# A2: NOT IN subquery
# ---------------------------------------------------------------------------
def _check_not_in_subquery(node: exp.Expression, result: ParsedQuery) -> None:
    for not_node in node.find_all(exp.Not):
        child = not_node.this
        if isinstance(child, exp.In):
            expressions = child.args.get("expressions", [])
            query = child.args.get("query")
            if query or any(isinstance(e, exp.Subquery) for e in expressions):
                result.has_not_in_subquery = True
                return


# ---------------------------------------------------------------------------
# A3: LIKE with leading wildcard
# ---------------------------------------------------------------------------
def _check_leading_wildcard_like(node: exp.Expression, result: ParsedQuery) -> None:
    for like in node.find_all(exp.Like):
        pattern = like.expression
        if isinstance(pattern, exp.Literal) and pattern.is_string:
            val = pattern.this
            if isinstance(val, str) and val.startswith("%"):
                result.has_leading_wildcard_like = True
                return


# ---------------------------------------------------------------------------
# A4: SELECT DISTINCT (outer queries only, not EXISTS subqueries)
# ---------------------------------------------------------------------------
def _check_distinct(node: exp.Expression, result: ParsedQuery) -> None:
    for select in node.find_all(exp.Select):
        if not select.args.get("distinct"):
            continue
        parent = select.parent
        if isinstance(parent, exp.Subquery):
            grandparent = parent.parent
            if isinstance(grandparent, exp.Exists):
                continue
        result.has_distinct = True
        return


# ---------------------------------------------------------------------------
# A5: Correlated subqueries
# ---------------------------------------------------------------------------
def _check_correlated_subquery(node: exp.Expression, result: ParsedQuery) -> None:
    outer_tables = _collect_scope_tables(node)

    for subquery in node.find_all(exp.Subquery):
        if isinstance(subquery.parent, exp.Exists):
            continue
        inner_tables = _collect_scope_tables(subquery)
        for col in subquery.find_all(exp.Column):
            table_ref = col.table
            if table_ref and table_ref.lower() in outer_tables and table_ref.lower() not in inner_tables:
                result.has_correlated_subquery = True
                return


def _collect_scope_tables(node: exp.Expression) -> set[str]:
    """Collect table names and aliases directly referenced in this scope."""
    names: set[str] = set()
    for table in node.find_all(exp.Table):
        if table.name:
            names.add(table.name.lower())
        if table.alias:
            names.add(table.alias.lower())
    return names


# ---------------------------------------------------------------------------
# A6: Window functions without PARTITION BY
# ---------------------------------------------------------------------------
def _check_unpartitioned_window(node: exp.Expression, result: ParsedQuery) -> None:
    for window in node.find_all(exp.Window):
        partition_by = window.args.get("partition_by")
        if not partition_by:
            result.has_unpartitioned_window = True
            return


# ---------------------------------------------------------------------------
# A7: Large IN lists
# ---------------------------------------------------------------------------
def _check_large_in_list(node: exp.Expression, result: ParsedQuery) -> None:
    for in_node in node.find_all(exp.In):
        expressions = in_node.args.get("expressions", [])
        if len(expressions) >= LARGE_IN_LIST_THRESHOLD:
            result.large_in_list_count += 1


# ---------------------------------------------------------------------------
# A8: COUNT(DISTINCT ...)
# ---------------------------------------------------------------------------
def _check_count_distinct(node: exp.Expression, result: ParsedQuery) -> None:
    for count in node.find_all(exp.Count):
        if count.args.get("distinct"):
            result.has_count_distinct = True
            return


# ---------------------------------------------------------------------------
# A9: Complex OR filters (3+ branches)
# ---------------------------------------------------------------------------
def _check_complex_or_filter(node: exp.Expression, result: ParsedQuery) -> None:
    for where in node.find_all(exp.Where):
        or_count = _count_or_branches(where.this)
        if or_count >= 3:
            result.has_complex_or_filter = True
            return


def _count_or_branches(expr: exp.Expression) -> int:
    """Count the number of OR branches in a boolean expression."""
    if isinstance(expr, exp.Or):
        return _count_or_branches(expr.left) + _count_or_branches(expr.right)
    return 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _is_wrapped_in_function(col: exp.Column) -> bool:
    parent = col.parent
    while parent:
        if isinstance(parent, (exp.Anonymous, exp.Func)):
            return True
        if isinstance(parent, (exp.Where, exp.Join, exp.Select)):
            break
        parent = parent.parent
    return False
