from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel


class Severity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Category(str, Enum):
    QUERY = "query"
    EXECUTION = "execution"
    TABLE = "table"
    WAREHOUSE = "warehouse"


class Recommendation(BaseModel):
    severity: Severity
    category: Category
    title: str
    description: str
    action: Optional[str] = None


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


class TableInfo(BaseModel):
    full_name: str
    format: Optional[str] = None
    clustering_columns: list[str] = []
    partition_columns: list[str] = []
    num_files: Optional[int] = None
    size_in_bytes: Optional[int] = None
    properties: dict[str, str] = {}
    recommendations: list[Recommendation] = []


class PlanSummary(BaseModel):
    raw_plan: str
    scan_types: list[str] = []
    join_types: list[str] = []
    has_filter_pushdown: bool = False
    has_partition_pruning: bool = False
    warnings: list[str] = []


class WarehouseInfo(BaseModel):
    warehouse_id: str
    name: Optional[str] = None
    warehouse_type: Optional[str] = None
    cluster_size: Optional[str] = None
    num_clusters: Optional[int] = None
    enable_photon: Optional[bool] = None
    spot_instance_policy: Optional[str] = None
    channel: Optional[str] = None
    recommendations: list[Recommendation] = []


class AIRewriteResult(BaseModel):
    original_sql: str
    suggested_sql: str
    explanation: str


class AnalysisResult(BaseModel):
    query_metrics: QueryMetrics
    tables: list[TableInfo] = []
    plan_summary: Optional[PlanSummary] = None
    warehouse: Optional[WarehouseInfo] = None
    recommendations: list[Recommendation] = []
