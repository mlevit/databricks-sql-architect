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
  recommendations: Recommendation[];
}

export interface PlanSummary {
  raw_plan: string;
  scan_types: string[];
  join_types: string[];
  has_filter_pushdown: boolean;
  has_partition_pruning: boolean;
  warnings: string[];
}

export interface WarehouseInfo {
  warehouse_id: string;
  name: string | null;
  warehouse_type: string | null;
  cluster_size: string | null;
  num_clusters: number | null;
  enable_photon: boolean | null;
  spot_instance_policy: string | null;
  channel: string | null;
  recommendations: Recommendation[];
}

export interface AnalysisResult {
  query_metrics: QueryMetrics;
  tables: TableInfo[];
  plan_summary: PlanSummary | null;
  warehouse: WarehouseInfo | null;
  recommendations: Recommendation[];
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
