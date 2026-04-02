from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Severity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Category(str, Enum):
    QUERY = "query"
    EXECUTION = "execution"
    TABLE = "table"
    WAREHOUSE = "warehouse"
    STORAGE = "storage"
    DATA_MODELING = "data_modeling"


class Recommendation(BaseModel):
    severity: Severity
    category: Category
    title: str
    description: str
    action: Optional[str] = None
    snippet: Optional[str] = None
    impact: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Estimated performance impact if addressed (1=minimal, 10=transformative)",
    )
    affected_tables: list[str] = Field(default_factory=list)
    per_table_actions: dict[str, str] = Field(default_factory=dict)


class QueryMetrics(BaseModel):
    statement_id: str
    statement_text: str
    execution_status: str
    total_duration_ms: Optional[int] = None
    compilation_duration_ms: Optional[int] = None
    execution_duration_ms: Optional[int] = None
    waiting_for_compute_duration_ms: Optional[int] = None
    waiting_at_capacity_duration_ms: Optional[int] = None
    result_fetch_duration_ms: Optional[int] = None
    total_task_duration_ms: Optional[int] = None
    read_bytes: Optional[int] = None
    read_rows: Optional[int] = None
    read_files: Optional[int] = None
    read_partitions: Optional[int] = None
    pruned_files: Optional[int] = None
    produced_rows: Optional[int] = None
    spilled_local_bytes: Optional[int] = None
    read_io_cache_percent: Optional[int] = None
    from_result_cache: Optional[bool] = None
    shuffle_read_bytes: Optional[int] = None
    written_bytes: Optional[int] = None
    warehouse_id: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None


class ColumnInfo(BaseModel):
    name: str
    data_type: str
    comment: Optional[str] = None


class TableInfo(BaseModel):
    full_name: str
    table_type: Optional[str] = None
    view_definition: Optional[str] = None
    format: Optional[str] = None
    clustering_columns: list[str] = Field(default_factory=list)
    partition_columns: list[str] = Field(default_factory=list)
    num_files: Optional[int] = None
    size_in_bytes: Optional[int] = None
    column_count: Optional[int] = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    properties: dict[str, str] = Field(default_factory=dict)
    has_cbo_stats: bool = False
    stats_num_rows: Optional[int] = None
    stats_total_size: Optional[int] = None
    recommendations: list[Recommendation] = Field(default_factory=list)
    underlying_tables: list[TableInfo] = Field(default_factory=list)


class PlanHighlight(BaseModel):
    line_start: int = Field(description="0-indexed first line of the highlighted region")
    line_end: int = Field(description="0-indexed last line (inclusive)")
    severity: Severity
    reason: str = Field(description="Why this part of the plan is problematic")


class ScanInfo(BaseModel):
    operator: str = Field(description="Scan operator type, e.g. Scan, FileScan, PhotonScan")
    format: str = Field(description="Data format, e.g. parquet, delta, orc")
    table_name: Optional[str] = Field(default=None, description="Fully-qualified table name if extractable")
    count: int = Field(default=1, description="Number of occurrences in the plan")


class PlanSummary(BaseModel):
    raw_plan: str
    scans: list[ScanInfo] = Field(default_factory=list)
    join_types: list[str] = Field(default_factory=list)
    has_filter_pushdown: bool = False
    has_partition_pruning: bool = False
    warnings: list[str] = Field(default_factory=list)
    recommendations: list[Recommendation] = Field(default_factory=list)
    highlights: list[PlanHighlight] = Field(default_factory=list)


class ScalingEvent(BaseModel):
    event_time: str
    event_type: str
    cluster_count: int


class QueryLoadPoint(BaseModel):
    time: str
    running: int
    queued: int


class WarehouseActivity(BaseModel):
    time_window_start: str
    time_window_end: str
    concurrent_query_count: int
    queued_query_count: int
    total_queries_in_window: int
    active_cluster_count: Optional[int] = None
    scaling_events: list[ScalingEvent] = Field(default_factory=list)
    query_load: list[QueryLoadPoint] = Field(default_factory=list)


class WarehouseInfo(BaseModel):
    warehouse_id: str
    name: Optional[str] = None
    warehouse_type: Optional[str] = None
    cluster_size: Optional[str] = None
    min_num_clusters: Optional[int] = None
    max_num_clusters: Optional[int] = None
    num_clusters: Optional[int] = None
    auto_stop_mins: Optional[int] = None
    enable_photon: Optional[bool] = None
    enable_serverless_compute: Optional[bool] = None
    spot_instance_policy: Optional[str] = None
    channel: Optional[str] = None
    activity: Optional[WarehouseActivity] = None
    recommendations: list[Recommendation] = Field(default_factory=list)


class AIRewriteResult(BaseModel):
    original_sql: str
    suggested_sql: str
    explanation: str
    syntax_valid: bool = True
    syntax_errors: list[str] = Field(default_factory=list)


class AnalysisResult(BaseModel):
    query_metrics: QueryMetrics
    tables: list[TableInfo] = Field(default_factory=list)
    plan_summary: Optional[PlanSummary] = None
    warehouse: Optional[WarehouseInfo] = None
    recommendations: list[Recommendation] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class QueryExecutionMetrics(BaseModel):
    total_duration_ms: Optional[int] = None
    compilation_duration_ms: Optional[int] = None
    execution_duration_ms: Optional[int] = None
    result_fetch_duration_ms: Optional[int] = None
    total_task_duration_ms: Optional[int] = None
    read_bytes: Optional[int] = None
    read_rows: Optional[int] = None
    read_files: Optional[int] = None
    read_partitions: Optional[int] = None
    pruned_files: Optional[int] = None
    produced_rows: Optional[int] = None
    spilled_local_bytes: Optional[int] = None
    shuffle_read_bytes: Optional[int] = None
    from_result_cache: Optional[bool] = None


class QueryBenchmarkStats(BaseModel):
    elapsed_ms: int
    row_count: Optional[int] = None
    byte_count: Optional[int] = None
    status: str = "SUCCEEDED"
    error: Optional[str] = None
    metrics: Optional[QueryExecutionMetrics] = None


class BenchmarkResult(BaseModel):
    original: QueryBenchmarkStats
    suggested: QueryBenchmarkStats
