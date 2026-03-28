"""Tests for backend.analyzers.ai_advisor."""

from backend.analyzers.ai_advisor import _lint_databricks_sql, _parse_ai_response, _validate_sql


class TestParseAIResponse:
    def test_standard_format(self):
        response = (
            "OPTIMIZED SQL:\n"
            "```sql\n"
            "SELECT id FROM t WHERE status = 'active'\n"
            "```\n"
            "EXPLANATION:\n"
            "Removed SELECT * and added a filter."
        )
        sql, explanation = _parse_ai_response(response, "SELECT * FROM t")
        assert "SELECT id FROM t" in sql
        assert "Removed SELECT" in explanation

    def test_no_sql_block(self):
        response = "No changes needed. The query is already optimal."
        sql, explanation = _parse_ai_response(response, "SELECT 1")
        assert sql == "SELECT 1"
        assert "No changes needed" in explanation

    def test_only_sql_block(self):
        response = (
            "Here is the rewrite:\n"
            "```sql\n"
            "SELECT id FROM t\n"
            "```\n"
            "The above removes unnecessary columns."
        )
        sql, explanation = _parse_ai_response(response, "SELECT * FROM t")
        assert "SELECT id FROM t" in sql
        assert "removes unnecessary" in explanation

    def test_empty_response(self):
        sql, explanation = _parse_ai_response("", "SELECT 1")
        assert sql == "SELECT 1"
        assert explanation == ""

    def test_explanation_after_code_fence(self):
        response = (
            "```sql\n"
            "SELECT 1\n"
            "```\n"
            "This simplifies the query."
        )
        sql, explanation = _parse_ai_response(response, "ORIGINAL")
        assert sql == "SELECT 1"
        assert "simplifies" in explanation

    def test_multiple_code_blocks(self):
        response = (
            "```sql\n"
            "SELECT a FROM t\n"
            "```\n"
            "And also:\n"
            "```sql\n"
            "SELECT b FROM t\n"
            "```\n"
            "EXPLANATION:\n"
            "Two options provided."
        )
        sql, explanation = _parse_ai_response(response, "ORIGINAL")
        assert "SELECT a FROM t" in sql
        assert "Two options" in explanation

    def test_explanation_with_inline_code(self):
        response = (
            "OPTIMIZED SQL:\n"
            "```sql\n"
            "SELECT 1\n"
            "```\n"
            "EXPLANATION:\n"
            "```note\nsome note\n```\n"
            "The real explanation is here."
        )
        sql, explanation = _parse_ai_response(response, "ORIGINAL")
        assert sql == "SELECT 1"
        assert "real explanation" in explanation


class TestValidateSql:
    def test_valid_select(self):
        valid, errors = _validate_sql("SELECT id, name FROM customers WHERE id = 1")
        assert valid is True
        assert errors == []

    def test_valid_complex_query(self):
        sql = """
        WITH cte AS (
            SELECT id, COUNT(*) AS cnt FROM orders GROUP BY id
        )
        SELECT c.name, cte.cnt
        FROM customers c
        JOIN cte ON c.id = cte.id
        ORDER BY cte.cnt DESC
        LIMIT 10
        """
        valid, errors = _validate_sql(sql)
        assert valid is True
        assert errors == []

    def test_invalid_syntax(self):
        valid, errors = _validate_sql("SELEC id FORM t WERE x = 1")
        assert valid is False
        assert len(errors) > 0

    def test_unclosed_parenthesis(self):
        valid, errors = _validate_sql("SELECT * FROM t WHERE id IN (1, 2, 3")
        assert valid is False
        assert len(errors) > 0

    def test_empty_string(self):
        valid, errors = _validate_sql("")
        assert valid is False
        assert len(errors) > 0

    def test_whitespace_only(self):
        valid, errors = _validate_sql("   ")
        assert valid is False
        assert len(errors) > 0

    def test_unpivot_single_quoted_alias_flagged(self):
        sql = """
        SELECT * FROM sales
        UNPIVOT (
            revenue FOR category IN (
                q1_rev AS 'Q1 Revenue',
                q2_rev AS 'Q2 Revenue'
            )
        )
        """
        valid, errors = _validate_sql(sql)
        assert valid is False
        assert any("UNPIVOT" in e for e in errors)

    def test_unpivot_backtick_alias_ok(self):
        sql = """
        SELECT * FROM sales
        UNPIVOT (
            revenue FOR category IN (
                q1_rev AS `Q1 Revenue`,
                q2_rev AS `Q2 Revenue`
            )
        )
        """
        _valid, errors = _validate_sql(sql)
        assert not any("UNPIVOT" in e for e in errors)

    def test_pivot_single_quoted_alias_flagged(self):
        sql = """
        SELECT * FROM sales
        PIVOT (
            SUM(amount) FOR quarter IN (
                'Q1' AS 'First Quarter',
                'Q2' AS 'Second Quarter'
            )
        )
        """
        valid, errors = _validate_sql(sql)
        assert valid is False
        assert any("PIVOT" in e for e in errors)


class TestLintDatabricksSql:
    def test_no_pivot_unpivot(self):
        assert _lint_databricks_sql("SELECT 1 FROM t") == []

    def test_single_quote_in_unpivot(self):
        sql = "UNPIVOT (val FOR name IN (a AS 'Label A', b AS 'Label B'))"
        warnings = _lint_databricks_sql(sql)
        assert len(warnings) == 1
        assert "UNPIVOT" in warnings[0]

    def test_backtick_in_unpivot(self):
        sql = "UNPIVOT (val FOR name IN (a AS `Label A`, b AS `Label B`))"
        warnings = _lint_databricks_sql(sql)
        assert warnings == []

    def test_string_value_in_where_not_flagged(self):
        sql = "SELECT * FROM t WHERE name = 'hello'"
        assert _lint_databricks_sql(sql) == []
