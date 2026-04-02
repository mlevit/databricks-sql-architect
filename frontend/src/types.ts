export type Severity = "critical" | "warning" | "info";
export type Category = "query" | "execution" | "table" | "warehouse" | "storage" | "data_modeling";

export interface Recommendation {
  severity: Severity;
  category: Category;
  title: string;
  description: string;
  action?: string;
  snippet?: string;
  impact: number;
  affected_tables?: string[];
  per_table_actions?: Record<string, string>;
}

export interface QueryMetrics {
  statement_id: string;
  statement_text: string;
  execution_status: string;
  total_duration_ms: number | null;
  compilation_duration_ms: number | null;
  execution_duration_ms: number | null;
  waiting_for_compute_duration_ms: number | null;
  waiting_at_capacity_duration_ms: number | null;
  result_fetch_duration_ms: number | null;
  total_task_duration_ms: number | null;
  read_bytes: number | null;
  read_rows: number | null;
  read_files: number | null;
  read_partitions: number | null;
  pruned_files: number | null;
  produced_rows: number | null;
  spilled_local_bytes: number | null;
  read_io_cache_percent: number | null;
  from_result_cache: boolean | null;
  shuffle_read_bytes: number | null;
  written_bytes: number | null;
  warehouse_id: string | null;
  start_time: string | null;
  end_time: string | null;
}

export interface TableInfo {
  full_name: string;
  format: string | null;
  clustering_columns: string[];
  partition_columns: string[];
  num_files: number | null;
  size_in_bytes: number | null;
  column_count: number | null;
  properties: Record<string, string>;
  has_cbo_stats: boolean;
  stats_num_rows: number | null;
  stats_total_size: number | null;
  recommendations: Recommendation[];
}

export interface PlanHighlight {
  line_start: number;
  line_end: number;
  severity: Severity;
  reason: string;
}

export interface ScanInfo {
  operator: string;
  format: string;
  table_name: string | null;
  count: number;
}

export interface PlanSummary {
  raw_plan: string;
  scans: ScanInfo[];
  join_types: string[];
  has_filter_pushdown: boolean;
  has_partition_pruning: boolean;
  warnings: string[];
  recommendations: Recommendation[];
  highlights: PlanHighlight[];
}

export interface ScalingEvent {
  event_time: string;
  event_type: string;
  cluster_count: number;
}

export interface QueryLoadPoint {
  time: string;
  running: number;
  queued: number;
}

export interface WarehouseActivity {
  time_window_start: string;
  time_window_end: string;
  concurrent_query_count: number;
  queued_query_count: number;
  total_queries_in_window: number;
  active_cluster_count: number | null;
  scaling_events: ScalingEvent[];
  query_load: QueryLoadPoint[];
}

export interface WarehouseInfo {
  warehouse_id: string;
  name: string | null;
  warehouse_type: string | null;
  cluster_size: string | null;
  min_num_clusters: number | null;
  max_num_clusters: number | null;
  num_clusters: number | null;
  auto_stop_mins: number | null;
  enable_photon: boolean | null;
  enable_serverless_compute: boolean | null;
  spot_instance_policy: string | null;
  channel: string | null;
  activity: WarehouseActivity | null;
  recommendations: Recommendation[];
}

export interface AnalysisResult {
  query_metrics: QueryMetrics;
  tables: TableInfo[];
  plan_summary: PlanSummary | null;
  warehouse: WarehouseInfo | null;
  recommendations: Recommendation[];
  warnings: string[];
}

export interface AIRewriteResult {
  original_sql: string;
  suggested_sql: string;
  explanation: string;
  syntax_valid: boolean;
  syntax_errors: string[];
}

export interface QueryExecutionMetrics {
  total_duration_ms: number | null;
  compilation_duration_ms: number | null;
  execution_duration_ms: number | null;
  result_fetch_duration_ms: number | null;
  total_task_duration_ms: number | null;
  read_bytes: number | null;
  read_rows: number | null;
  read_files: number | null;
  read_partitions: number | null;
  pruned_files: number | null;
  produced_rows: number | null;
  spilled_local_bytes: number | null;
  shuffle_read_bytes: number | null;
  from_result_cache: boolean | null;
}

export interface QueryBenchmarkStats {
  elapsed_ms: number;
  row_count: number | null;
  byte_count: number | null;
  status: string;
  error: string | null;
  metrics: QueryExecutionMetrics | null;
}

export interface BenchmarkResult {
  original: QueryBenchmarkStats;
  suggested: QueryBenchmarkStats;
}
