"""Tests for backend.analyzers.table_analyzer (new D6–D16 checks).

These tests call the internal _check_* functions directly with synthetic
data so they run without a live Databricks connection.
"""

from backend.analyzers.table_analyzer import (
    _check_high_cardinality_clustering_key,
    _check_hive_partitioning,
    _check_inappropriate_data_types,
    _check_json_string_columns,
    _check_large_table_no_date_clustering,
    _check_non_delta_format,
    _check_string_enum_columns,
    _check_under_partitioned,
    _check_vacuum_needed,
    _check_wide_table,
    is_poor_clustering_candidate,
    LARGE_TABLE_THRESHOLD,
    WIDE_TABLE_COLUMN_THRESHOLD,
)
from backend.models import ColumnInfo, Recommendation


def _col(name: str, data_type: str = "STRING", comment: str | None = None) -> ColumnInfo:
    return ColumnInfo(name=name, data_type=data_type, comment=comment)


class TestUnderPartitioned:
    def test_flags_very_large_partitions(self):
        recs: list[Recommendation] = []
        _check_under_partitioned(
            "db.schema.fact_table",
            partitions=["date"],
            num_files=10,
            size_bytes=20 * 1024**4,  # 20 TB
            recs=recs,
        )
        assert any("Under-partitioned" in r.title for r in recs)

    def test_no_flag_for_normal_table(self):
        recs: list[Recommendation] = []
        _check_under_partitioned(
            "db.schema.fact_table",
            partitions=["date"],
            num_files=1000,
            size_bytes=500 * 1024**3,  # 500 GB
            recs=recs,
        )
        assert not any("Under-partitioned" in r.title for r in recs)

    def test_no_flag_without_partitions(self):
        recs: list[Recommendation] = []
        _check_under_partitioned(
            "db.schema.t", partitions=[], num_files=100, size_bytes=10**12, recs=recs,
        )
        assert recs == []


class TestHighCardinalityClusteringKey:
    def test_uuid_clustering_flagged(self):
        recs: list[Recommendation] = []
        _check_high_cardinality_clustering_key(
            "db.schema.events",
            clustering=["request_id"],
            size_bytes=LARGE_TABLE_THRESHOLD + 1,
            recs=recs,
        )
        assert any("High-cardinality" in r.title for r in recs)

    def test_normal_key_not_flagged(self):
        recs: list[Recommendation] = []
        _check_high_cardinality_clustering_key(
            "db.schema.events",
            clustering=["event_date"],
            size_bytes=LARGE_TABLE_THRESHOLD + 1,
            recs=recs,
        )
        assert recs == []

    def test_small_table_not_flagged(self):
        recs: list[Recommendation] = []
        _check_high_cardinality_clustering_key(
            "db.schema.events",
            clustering=["uuid"],
            size_bytes=100 * 1024 * 1024,
            recs=recs,
        )
        assert recs == []


class TestWideTable:
    def test_wide_table_detected(self):
        cols = [_col(f"col_{i}", "INT") for i in range(WIDE_TABLE_COLUMN_THRESHOLD + 10)]
        recs: list[Recommendation] = []
        _check_wide_table("db.schema.wide", cols, recs)
        assert any("Wide table" in r.title for r in recs)

    def test_narrow_table_ok(self):
        cols = [_col(f"col_{i}", "INT") for i in range(10)]
        recs: list[Recommendation] = []
        _check_wide_table("db.schema.narrow", cols, recs)
        assert recs == []


class TestNonDeltaFormat:
    def test_parquet_flagged(self):
        recs: list[Recommendation] = []
        _check_non_delta_format("db.schema.legacy", "parquet", recs)
        assert any("Non-Delta" in r.title for r in recs)

    def test_delta_ok(self):
        recs: list[Recommendation] = []
        _check_non_delta_format("db.schema.good", "delta", recs)
        assert recs == []

    def test_none_format_ok(self):
        recs: list[Recommendation] = []
        _check_non_delta_format("db.schema.unknown", None, recs)
        assert recs == []


class TestVacuumNeeded:
    def test_no_vacuum_timestamp_flagged(self):
        recs: list[Recommendation] = []
        _check_vacuum_needed(
            "db.schema.stale",
            properties={"delta.minReaderVersion": "1"},
            size_bytes=500 * 1024 * 1024,
            recs=recs,
        )
        assert any("VACUUM" in r.title for r in recs)

    def test_has_vacuum_ok(self):
        recs: list[Recommendation] = []
        _check_vacuum_needed(
            "db.schema.clean",
            properties={"delta.lastVacuumTimestamp": "2024-01-01T00:00:00Z"},
            size_bytes=500 * 1024 * 1024,
            recs=recs,
        )
        assert recs == []

    def test_small_table_skipped(self):
        recs: list[Recommendation] = []
        _check_vacuum_needed(
            "db.schema.tiny", properties={}, size_bytes=1024, recs=recs,
        )
        assert recs == []


class TestInappropriateDataTypes:
    def test_date_as_string(self):
        cols = [_col("event_date", "STRING"), _col("id", "BIGINT")]
        recs: list[Recommendation] = []
        _check_inappropriate_data_types("db.schema.events", cols, recs)
        assert any("Date columns stored as STRING" in r.title for r in recs)

    def test_numeric_as_string(self):
        cols = [_col("total_amount", "STRING"), _col("id", "BIGINT")]
        recs: list[Recommendation] = []
        _check_inappropriate_data_types("db.schema.orders", cols, recs)
        assert any("Numeric columns stored as STRING" in r.title for r in recs)

    def test_proper_types_ok(self):
        cols = [_col("event_date", "DATE"), _col("amount", "DECIMAL")]
        recs: list[Recommendation] = []
        _check_inappropriate_data_types("db.schema.clean", cols, recs)
        assert recs == []


class TestStringEnumColumns:
    def test_status_string_flagged(self):
        cols = [_col("order_status", "STRING"), _col("id", "BIGINT")]
        recs: list[Recommendation] = []
        _check_string_enum_columns("db.schema.orders", cols, recs)
        assert any("low-cardinality" in r.title for r in recs)

    def test_non_enum_ok(self):
        cols = [_col("full_name", "STRING"), _col("id", "BIGINT")]
        recs: list[Recommendation] = []
        _check_string_enum_columns("db.schema.users", cols, recs)
        assert recs == []


class TestLargeTableNoDateClustering:
    def test_flags_large_table_with_date_column(self):
        cols = [_col("event_date", "DATE"), _col("value", "DOUBLE")]
        recs: list[Recommendation] = []
        _check_large_table_no_date_clustering(
            "db.schema.events",
            clustering=[], partitions=[],
            size_bytes=LARGE_TABLE_THRESHOLD + 1,
            columns=cols, recs=recs,
        )
        assert any("date clustering" in r.title for r in recs)

    def test_skips_clustered_table(self):
        cols = [_col("event_date", "DATE")]
        recs: list[Recommendation] = []
        _check_large_table_no_date_clustering(
            "db.schema.events",
            clustering=["event_date"], partitions=[],
            size_bytes=LARGE_TABLE_THRESHOLD + 1,
            columns=cols, recs=recs,
        )
        assert recs == []


class TestJsonStringColumns:
    def test_json_column_detected(self):
        cols = [_col("raw_payload", "STRING"), _col("id", "BIGINT")]
        recs: list[Recommendation] = []
        _check_json_string_columns("db.schema.events", cols, recs)
        assert any("JSON" in r.title for r in recs)

    def test_json_comment_detected(self):
        cols = [_col("data", "STRING", comment="json blob from API")]
        recs: list[Recommendation] = []
        _check_json_string_columns("db.schema.events", cols, recs)
        assert any("JSON" in r.title for r in recs)

    def test_normal_string_ok(self):
        cols = [_col("full_name", "STRING")]
        recs: list[Recommendation] = []
        _check_json_string_columns("db.schema.users", cols, recs)
        assert recs == []


class TestPoorClusteringCandidate:
    """Measure columns and high-cardinality IDs should be rejected as clustering keys."""

    def test_amount_rejected(self):
        assert is_poor_clustering_candidate("amount") is True

    def test_total_amount_rejected(self):
        assert is_poor_clustering_candidate("total_amount") is True

    def test_price_rejected(self):
        assert is_poor_clustering_candidate("price") is True

    def test_revenue_rejected(self):
        assert is_poor_clustering_candidate("revenue") is True

    def test_quantity_rejected(self):
        assert is_poor_clustering_candidate("qty") is True

    def test_uuid_rejected(self):
        assert is_poor_clustering_candidate("uuid") is True

    def test_request_id_rejected(self):
        assert is_poor_clustering_candidate("request_id") is True

    def test_date_column_accepted(self):
        assert is_poor_clustering_candidate("rental_date") is False

    def test_id_column_accepted(self):
        assert is_poor_clustering_candidate("customer_id") is False

    def test_status_column_accepted(self):
        assert is_poor_clustering_candidate("status") is False

    def test_region_column_accepted(self):
        assert is_poor_clustering_candidate("region") is False

    def test_generic_column_accepted(self):
        assert is_poor_clustering_candidate("store_name") is False


class TestHivePartitioning:
    def test_partitioned_table_flagged(self):
        recs: list[Recommendation] = []
        _check_hive_partitioning(
            "db.schema.events", partitions=["date"], clustering=[], recs=recs,
        )
        assert any("Hive-style" in r.title for r in recs)

    def test_already_clustered_ok(self):
        recs: list[Recommendation] = []
        _check_hive_partitioning(
            "db.schema.events",
            partitions=["date"], clustering=["date"], recs=recs,
        )
        assert recs == []

    def test_no_partitions_ok(self):
        recs: list[Recommendation] = []
        _check_hive_partitioning(
            "db.schema.events", partitions=[], clustering=[], recs=recs,
        )
        assert recs == []
