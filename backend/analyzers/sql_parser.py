from __future__ import annotations

import logging
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


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

    result.tables = list(dict.fromkeys(result.tables))
    return result


def _extract_tables(node: exp.Expression, result: ParsedQuery) -> None:
    virtual_names: set[str] = set()

    # Collect CTE names
    for cte in node.find_all(exp.CTE):
        alias = cte.args.get("alias")
        if alias and isinstance(alias, exp.TableAlias):
            virtual_names.add(alias.name.lower())
        elif alias:
            virtual_names.add(str(alias).lower())

    # Collect subquery aliases
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

        # Skip CTE / subquery alias references
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
    selects = list(node.find_all(exp.Select))
    if not selects:
        return
    outermost = selects[0]
    parent_query = outermost.parent
    if parent_query and not parent_query.find(exp.Where):
        result.missing_where = True


def _extract_filters(node: exp.Expression, result: ParsedQuery) -> None:
    for where in node.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            result.filter_columns.append(col.name)
            if _is_wrapped_in_function(col):
                result.has_function_on_filter_column = True


def _check_order_in_subquery(node: exp.Expression, result: ParsedQuery) -> None:
    for subquery in node.find_all(exp.Subquery):
        if subquery.find(exp.Order):
            result.has_order_by_in_subquery = True
            return


def _check_limit(node: exp.Expression, result: ParsedQuery) -> None:
    if node.find(exp.Limit):
        result.has_limit = True


def _is_wrapped_in_function(col: exp.Column) -> bool:
    parent = col.parent
    while parent:
        if isinstance(parent, (exp.Anonymous, exp.Func)):
            return True
        if isinstance(parent, (exp.Where, exp.Join, exp.Select)):
            break
        parent = parent.parent
    return False
