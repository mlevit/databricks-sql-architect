"""Tests for backend.analyzers.query_metrics."""

import json

from backend.analyzers.query_metrics import (
    SPILL_THRESHOLD_BYTES,
    analyze_query_metrics,
    build_query_metrics,
)
from backend.models import QueryMetrics


class TestBuildQueryMetrics:
    def test_basic_fields(self):
        row = {
            "statement_id": "abc-123",
            "statement_text": "SELECT 1",
            "execution_status": "FINISHED",
            "total_duration_ms": "1000",
            "read_bytes": "5000",
        }
        m = build_query_metrics(row)
        assert m.statement_id == "abc-123"
        assert m.total_duration_ms == 1000
        assert m.read_bytes == 5000

    def test_none_values(self):
        row = {
            "statement_id": "abc",
            "statement_text": "SELECT 1",
            "execution_status": "FINISHED",
        }
        m = build_query_metrics(row)
        assert m.total_duration_ms is None
        assert m.read_bytes is None

    def test_warehouse_id_from_json_compute(self):
        row = {
            "statement_id": "abc",
            "statement_text": "SELECT 1",
            "execution_status": "FINISHED",
            "compute": json.dumps({"warehouse_id": "wh-123"}),
        }
        m = build_query_metrics(row)
        assert m.warehouse_id == "wh-123"

    def test_warehouse_id_from_dict_compute(self):
        row = {
            "statement_id": "abc",
            "statement_text": "SELECT 1",
            "execution_status": "FINISHED",
            "compute": {"warehouse_id": "wh-456"},
        }
        m = build_query_metrics(row)
        assert m.warehouse_id == "wh-456"

    def test_from_result_cache_variants(self):
        for truthy in (True, "true", "TRUE", "1"):
            row = {
                "statement_id": "x",
                "statement_text": "",
                "execution_status": "",
                "from_result_cache": truthy,
            }
            assert build_query_metrics(row).from_result_cache is True

        for falsy in (False, "false", "0", None):
            row = {
                "statement_id": "x",
                "statement_text": "",
                "execution_status": "",
                "from_result_cache": falsy,
            }
            assert build_query_metrics(row).from_result_cache is False


class TestAnalyzeQueryMetrics:
    def _make_metrics(self, **overrides) -> QueryMetrics:
        defaults = {
            "statement_id": "test",
            "statement_text": "SELECT 1",
            "execution_status": "FINISHED",
        }
        defaults.update(overrides)
        return QueryMetrics(**defaults)

    def test_spill_detection(self):
        m = self._make_metrics(spilled_local_bytes=SPILL_THRESHOLD_BYTES + 1)
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Data spilling to disk" in titles

    def test_no_spill_below_threshold(self):
        m = self._make_metrics(spilled_local_bytes=SPILL_THRESHOLD_BYTES - 1)
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Data spilling to disk" not in titles

    def test_poor_pruning(self):
        m = self._make_metrics(read_files=90, pruned_files=10)
        recs = analyze_query_metrics(m, tables=["my_table"])
        titles = [r.title for r in recs]
        assert "Poor data skipping" in titles

    def test_good_pruning(self):
        m = self._make_metrics(read_files=10, pruned_files=90)
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Poor data skipping" not in titles

    def test_low_cache(self):
        m = self._make_metrics(
            read_io_cache_percent=5,
            read_bytes=500 * 1024 * 1024,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Low IO cache utilization" in titles

    def test_high_shuffle(self):
        m = self._make_metrics(
            read_bytes=100 * 1024 * 1024,
            shuffle_read_bytes=80 * 1024 * 1024,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "High shuffle volume" in titles

    def test_capacity_wait(self):
        m = self._make_metrics(
            total_duration_ms=10000,
            waiting_at_capacity_duration_ms=5000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Significant time waiting for capacity" in titles

    def test_high_compilation(self):
        m = self._make_metrics(
            total_duration_ms=20000,
            compilation_duration_ms=10000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "High compilation time" in titles

    def test_excessive_rows_scanned(self):
        m = self._make_metrics(
            read_rows=10_000_000,
            produced_rows=100,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Excessive rows scanned vs produced" in titles

    def test_high_fetch_time(self):
        m = self._make_metrics(
            total_duration_ms=10000,
            result_fetch_duration_ms=5000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "High result fetch time" in titles

    def test_data_skew_detected(self):
        m = self._make_metrics(
            spilled_local_bytes=SPILL_THRESHOLD_BYTES + 1,
            execution_duration_ms=10000,
            total_task_duration_ms=12000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Data skew likely" in titles

    def test_no_data_skew_without_spill(self):
        m = self._make_metrics(
            spilled_local_bytes=0,
            execution_duration_ms=10000,
            total_task_duration_ms=12000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Data skew likely" not in titles

    def test_no_data_skew_with_high_parallelism(self):
        m = self._make_metrics(
            spilled_local_bytes=SPILL_THRESHOLD_BYTES + 1,
            execution_duration_ms=10000,
            total_task_duration_ms=100000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Data skew likely" not in titles

    def test_low_parallelism(self):
        m = self._make_metrics(
            execution_duration_ms=10000,
            total_task_duration_ms=12000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Low parallelism efficiency" in titles

    def test_no_recs_for_healthy_query(self):
        m = self._make_metrics(
            total_duration_ms=500,
            execution_duration_ms=400,
            read_files=10,
            pruned_files=90,
            read_io_cache_percent=80,
        )
        recs = analyze_query_metrics(m)
        assert len(recs) == 0

    def test_result_cache_not_used(self):
        m = self._make_metrics(
            from_result_cache=False,
            execution_duration_ms=10000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Query not served from result cache" in titles

    def test_result_cache_used_no_rec(self):
        m = self._make_metrics(
            from_result_cache=True,
            execution_duration_ms=10000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Query not served from result cache" not in titles

    def test_result_cache_short_query_no_rec(self):
        m = self._make_metrics(
            from_result_cache=False,
            execution_duration_ms=1000,
        )
        recs = analyze_query_metrics(m)
        titles = [r.title for r in recs]
        assert "Query not served from result cache" not in titles
