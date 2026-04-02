import type { AnalysisResult, Recommendation, TableInfo, PlanSummary, WarehouseInfo, QueryMetrics } from "./types";

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const mins = Math.floor(ms / 60_000);
  const secs = ((ms % 60_000) / 1000).toFixed(0);
  return `${mins}m ${secs}s`;
}

function humanBytes(b: number | null | undefined): string {
  if (b == null || b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let val = b;
  while (Math.abs(val) >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function fmtNum(n: number | null | undefined): string {
  if (n == null) return "N/A";
  return n.toLocaleString();
}

const SEVERITY_EMOJI: Record<string, string> = { critical: "🔴", warning: "🟡", info: "🔵" };

// ---------------------------------------------------------------------------
// Markdown
// ---------------------------------------------------------------------------

function metricsMarkdown(m: QueryMetrics): string {
  const lines: string[] = [];
  lines.push("## Performance Metrics\n");

  lines.push(`| Metric | Value |`);
  lines.push(`|--------|-------|`);
  lines.push(`| Status | ${m.execution_status} |`);
  if (m.total_duration_ms != null) lines.push(`| Total Duration | ${formatMs(m.total_duration_ms)} |`);
  if (m.compilation_duration_ms != null) lines.push(`| Compilation | ${formatMs(m.compilation_duration_ms)} |`);
  if (m.execution_duration_ms != null) lines.push(`| Execution | ${formatMs(m.execution_duration_ms)} |`);
  if (m.waiting_for_compute_duration_ms != null) lines.push(`| Compute Wait | ${formatMs(m.waiting_for_compute_duration_ms)} |`);
  if (m.waiting_at_capacity_duration_ms != null) lines.push(`| Capacity Wait | ${formatMs(m.waiting_at_capacity_duration_ms)} |`);
  if (m.result_fetch_duration_ms != null) lines.push(`| Result Fetch | ${formatMs(m.result_fetch_duration_ms)} |`);
  lines.push(`| Rows Read | ${fmtNum(m.read_rows)} |`);
  lines.push(`| Rows Produced | ${fmtNum(m.produced_rows)} |`);
  lines.push(`| Data Read | ${humanBytes(m.read_bytes)} |`);
  lines.push(`| Files Read | ${fmtNum(m.read_files)} |`);
  lines.push(`| Files Pruned | ${fmtNum(m.pruned_files)} |`);
  lines.push(`| Partitions Read | ${fmtNum(m.read_partitions)} |`);
  lines.push(`| Disk Spill | ${humanBytes(m.spilled_local_bytes)} |`);
  lines.push(`| IO Cache Hit | ${m.read_io_cache_percent != null ? `${m.read_io_cache_percent}%` : "N/A"} |`);
  lines.push(`| Shuffle Read | ${humanBytes(m.shuffle_read_bytes)} |`);
  lines.push(`| Result Cache | ${m.from_result_cache ? "Yes" : "No"} |`);
  lines.push("");
  return lines.join("\n");
}

function recMarkdown(r: Recommendation): string {
  const lines: string[] = [];
  const icon = SEVERITY_EMOJI[r.severity] ?? "⚪";
  lines.push(`### ${icon} ${r.title}\n`);
  lines.push(`**Severity:** ${r.severity} · **Category:** ${r.category} · **Impact:** ${r.impact}/10\n`);
  lines.push(r.description + "\n");
  if (r.affected_tables && r.affected_tables.length > 0) {
    lines.push(`**Affected tables:** ${r.affected_tables.map((t) => `\`${t}\``).join(", ")}\n`);
  }
  if (r.snippet) {
    lines.push("```sql\n" + r.snippet + "\n```\n");
  }
  if (r.action) {
    lines.push(`**Suggested action:** ${r.action}\n`);
  }
  if (r.per_table_actions && Object.keys(r.per_table_actions).length > 0) {
    lines.push("**Per-table actions:**\n");
    for (const [table, action] of Object.entries(r.per_table_actions)) {
      lines.push(`- \`${table}\`:\n\`\`\`sql\n${action}\n\`\`\`\n`);
    }
  }
  return lines.join("\n");
}

function tableMarkdownEntry(t: TableInfo, depth: number): string {
  const prefix = "#".repeat(Math.min(depth + 3, 6));
  const lines: string[] = [];
  const typeLabel = t.table_type?.toUpperCase() === "VIEW"
    ? ` (${t.table_type})`
    : "";
  lines.push(`${prefix} \`${t.full_name}\`${typeLabel}\n`);
  const attrs: string[] = [];
  if (t.format) attrs.push(`Format: ${t.format}`);
  if (t.num_files != null) attrs.push(`Files: ${fmtNum(t.num_files)}`);
  if (t.size_in_bytes != null) attrs.push(`Size: ${humanBytes(t.size_in_bytes)}`);
  if (t.clustering_columns.length > 0) attrs.push(`Clustering: ${t.clustering_columns.join(", ")}`);
  if (t.partition_columns.length > 0) attrs.push(`Partitioning: ${t.partition_columns.join(", ")}`);
  if (t.has_cbo_stats) {
    let stats = "CBO Stats: collected";
    if (t.stats_num_rows != null) stats += ` (${fmtNum(t.stats_num_rows)} rows)`;
    attrs.push(stats);
  } else {
    attrs.push("CBO Stats: not collected");
  }
  if (attrs.length) lines.push(attrs.join(" · ") + "\n");
  if (t.recommendations.length > 0) {
    for (const r of t.recommendations) {
      lines.push(recMarkdown(r));
    }
  }
  if (t.underlying_tables && t.underlying_tables.length > 0) {
    lines.push(`\n**Underlying Tables:**\n`);
    for (const child of t.underlying_tables) {
      lines.push(tableMarkdownEntry(child, depth + 1));
    }
  }
  return lines.join("\n");
}

function tablesMarkdown(tables: TableInfo[]): string {
  if (tables.length === 0) return "";
  const lines: string[] = [];
  lines.push("## Tables\n");
  for (const t of tables) {
    lines.push(tableMarkdownEntry(t, 0));
  }
  return lines.join("\n");
}

function planMarkdown(plan: PlanSummary | null): string {
  if (!plan) return "";
  const lines: string[] = [];
  lines.push("## Execution Plan\n");
  if (plan.join_types.length > 0) lines.push(`**Join types:** ${plan.join_types.join(", ")}\n`);
  lines.push(`**Filter pushdown:** ${plan.has_filter_pushdown ? "Yes" : "No"}`);
  lines.push(`**Partition pruning:** ${plan.has_partition_pruning ? "Yes" : "No"}\n`);
  if (plan.scans.length > 0) {
    lines.push("**Scans:**\n");
    for (const s of plan.scans) {
      lines.push(`- ${s.operator} (${s.format})${s.table_name ? ` on \`${s.table_name}\`` : ""}${s.count > 1 ? ` ×${s.count}` : ""}`);
    }
    lines.push("");
  }
  if (plan.warnings.length > 0) {
    lines.push("**Warnings:**\n");
    for (const w of plan.warnings) lines.push(`- ${w}`);
    lines.push("");
  }
  if (plan.recommendations.length > 0) {
    for (const r of plan.recommendations) lines.push(recMarkdown(r));
  }
  return lines.join("\n");
}

function warehouseMarkdown(wh: WarehouseInfo | null): string {
  if (!wh) return "";
  const lines: string[] = [];
  lines.push("## Warehouse\n");
  const attrs: string[] = [];
  if (wh.name) attrs.push(`Name: ${wh.name}`);
  if (wh.warehouse_type) attrs.push(`Type: ${wh.warehouse_type}`);
  if (wh.cluster_size) attrs.push(`Size: ${wh.cluster_size}`);
  if (wh.min_num_clusters != null) attrs.push(`Min clusters: ${wh.min_num_clusters}`);
  if (wh.max_num_clusters != null) attrs.push(`Max clusters: ${wh.max_num_clusters}`);
  if (wh.enable_photon != null) attrs.push(`Photon: ${wh.enable_photon ? "Enabled" : "Disabled"}`);
  if (wh.enable_serverless_compute != null) attrs.push(`Serverless: ${wh.enable_serverless_compute ? "Yes" : "No"}`);
  if (attrs.length) lines.push(attrs.join(" · ") + "\n");
  if (wh.activity) {
    lines.push(`**Activity during query:** ${wh.activity.concurrent_query_count} concurrent queries, ${wh.activity.queued_query_count} queued, ${wh.activity.total_queries_in_window} total in window\n`);
  }
  if (wh.recommendations.length > 0) {
    for (const r of wh.recommendations) lines.push(recMarkdown(r));
  }
  return lines.join("\n");
}

export function generateMarkdown(result: AnalysisResult): string {
  const parts: string[] = [];
  const m = result.query_metrics;

  parts.push("# SQL Architect Analysis Report\n");
  parts.push(`**Statement ID:** \`${m.statement_id}\``);
  if (m.start_time) parts.push(`**Time:** ${m.start_time}`);
  parts.push(`**Total Duration:** ${m.total_duration_ms != null ? formatMs(m.total_duration_ms) : "N/A"}`);
  parts.push(`**Recommendations:** ${result.recommendations.length}\n`);

  parts.push("## SQL Query\n");
  parts.push("```sql\n" + m.statement_text + "\n```\n");

  parts.push(metricsMarkdown(m));

  if (result.recommendations.length > 0) {
    parts.push("## Recommendations\n");
    for (const r of result.recommendations) {
      parts.push(recMarkdown(r));
    }
  }

  parts.push(tablesMarkdown(result.tables));
  parts.push(planMarkdown(result.plan_summary));
  parts.push(warehouseMarkdown(result.warehouse));

  parts.push("---\n*Generated by Databricks SQL Architect*\n");
  return parts.filter(Boolean).join("\n");
}

// ---------------------------------------------------------------------------
// HTML
// ---------------------------------------------------------------------------

function escHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

const SEVERITY_COLOR: Record<string, string> = {
  critical: "#ef4444",
  warning: "#f59e0b",
  info: "#3b82f6",
};

const SEVERITY_BG: Record<string, string> = {
  critical: "rgba(239,68,68,0.08)",
  warning: "rgba(245,158,11,0.08)",
  info: "rgba(59,130,246,0.08)",
};

function tableHtmlEntry(t: TableInfo, depth: number): string {
  const indent = depth * 16;
  let html = `<div style="border:1px solid rgba(255,255,255,0.06);border-radius:8px;padding:12px 16px;margin-bottom:8px;margin-left:${indent}px;background:rgba(255,255,255,0.02);${depth > 0 ? "border-left:2px solid rgba(217,70,239,0.3);" : ""}">`;
  html += `<div style="font-family:monospace;font-size:13px;font-weight:600;color:#e2e8f0;margin-bottom:6px;">${escHtml(t.full_name)}`;
  const tt = t.table_type?.toUpperCase();
  if (tt === "VIEW") {
    html += ` <span style="font-size:10px;text-transform:uppercase;background:linear-gradient(135deg,#d946ef,#ec4899);color:#fff;padding:2px 6px;border-radius:8px;font-weight:600;letter-spacing:0.05em;">${escHtml(t.table_type!)}</span>`;
  }
  if (t.format) html += ` <span style="font-size:10px;text-transform:uppercase;background:linear-gradient(135deg,#14b8a6,#22d3ee);color:#fff;padding:2px 6px;border-radius:8px;font-weight:600;letter-spacing:0.05em;">${escHtml(t.format)}</span>`;
  html += `</div>`;
  const attrs: string[] = [];
  if (t.num_files != null) attrs.push(`${fmtNum(t.num_files)} files`);
  if (t.size_in_bytes != null) attrs.push(humanBytes(t.size_in_bytes));
  if (t.clustering_columns.length > 0) attrs.push(`Clustering: ${t.clustering_columns.join(", ")}`);
  if (t.partition_columns.length > 0) attrs.push(`Partitioning: ${t.partition_columns.join(", ")}`);
  attrs.push(`CBO Stats: ${t.has_cbo_stats ? "collected" : "not collected"}`);
  html += `<div style="font-size:12px;color:#94a3b8;">${attrs.map(escHtml).join(" · ")}</div>`;
  for (const r of t.recommendations) html += recHtml(r);
  if (t.underlying_tables && t.underlying_tables.length > 0) {
    html += `<div style="font-size:10px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:10px 0 6px;">Underlying Tables</div>`;
    for (const child of t.underlying_tables) {
      html += tableHtmlEntry(child, depth + 1);
    }
  }
  html += `</div>`;
  return html;
}

function recHtml(r: Recommendation): string {
  const color = SEVERITY_COLOR[r.severity] ?? "#94a3b8";
  const bg = SEVERITY_BG[r.severity] ?? "transparent";
  const icon = SEVERITY_EMOJI[r.severity] ?? "⚪";
  let html = `<div style="border-left:3px solid ${color};background:${bg};border-radius:8px;padding:12px 16px;margin-bottom:8px;">`;
  html += `<div style="font-weight:600;font-size:14px;margin-bottom:4px;">${icon} ${escHtml(r.title)}</div>`;
  html += `<div style="font-size:12px;color:#94a3b8;margin-bottom:6px;">`;
  html += `<span style="text-transform:capitalize;">${escHtml(r.severity)}</span> · ${escHtml(r.category)} · Impact: ${r.impact}/10</div>`;
  html += `<div style="font-size:13px;color:#cbd5e1;line-height:1.6;">${escHtml(r.description)}</div>`;
  if (r.affected_tables && r.affected_tables.length > 0) {
    html += `<div style="font-size:12px;color:#94a3b8;margin-top:6px;">Affected tables: ${r.affected_tables.map((t) => `<code style="background:rgba(255,255,255,0.06);padding:1px 5px;border-radius:4px;font-size:11px;">${escHtml(t)}</code>`).join(", ")}</div>`;
  }
  if (r.snippet) {
    html += `<pre style="background:#0f172a;border-radius:6px;padding:10px 12px;margin-top:8px;overflow-x:auto;font-size:12px;color:#e2e8f0;"><code>${escHtml(r.snippet)}</code></pre>`;
  }
  if (r.action) {
    html += `<div style="font-size:12px;color:#94a3b8;margin-top:6px;"><strong>Suggested action:</strong> ${escHtml(r.action)}</div>`;
  }
  if (r.per_table_actions && Object.keys(r.per_table_actions).length > 0) {
    html += `<div style="margin-top:8px;">`;
    for (const [table, action] of Object.entries(r.per_table_actions)) {
      html += `<div style="font-size:12px;color:#94a3b8;margin-bottom:4px;"><code style="background:rgba(255,255,255,0.06);padding:1px 5px;border-radius:4px;font-size:11px;">${escHtml(table)}</code></div>`;
      html += `<pre style="background:#0f172a;border-radius:6px;padding:8px 10px;overflow-x:auto;font-size:12px;color:#e2e8f0;margin-bottom:6px;"><code>${escHtml(action)}</code></pre>`;
    }
    html += `</div>`;
  }
  html += `</div>`;
  return html;
}

function metricsRow(label: string, value: string): string {
  return `<tr><td style="padding:6px 12px;border-bottom:1px solid rgba(255,255,255,0.04);font-size:13px;color:#94a3b8;">${escHtml(label)}</td><td style="padding:6px 12px;border-bottom:1px solid rgba(255,255,255,0.04);font-size:13px;color:#e2e8f0;font-weight:500;">${escHtml(value)}</td></tr>`;
}

export function generateHtml(result: AnalysisResult): string {
  const m = result.query_metrics;

  let body = "";

  // Header
  body += `<div style="margin-bottom:24px;">`;
  body += `<h1 style="font-size:24px;font-weight:700;margin:0 0 8px 0;background:linear-gradient(135deg,#3b82f6,#8b5cf6);-webkit-background-clip:text;-webkit-text-fill-color:transparent;">SQL Architect Analysis Report</h1>`;
  body += `<div style="font-size:13px;color:#64748b;">Statement <code style="background:rgba(255,255,255,0.06);padding:2px 6px;border-radius:4px;font-size:12px;color:#94a3b8;">${escHtml(m.statement_id)}</code>`;
  if (m.start_time) body += ` · ${escHtml(m.start_time)}`;
  body += `</div>`;
  body += `</div>`;

  // Duration hero
  if (m.total_duration_ms != null) {
    const col = m.total_duration_ms < 5000 ? "#22d3ee" : m.total_duration_ms < 30000 ? "#f59e0b" : "#ef4444";
    body += `<div style="font-size:48px;font-weight:300;color:${col};margin-bottom:20px;">${escHtml(formatMs(m.total_duration_ms))}</div>`;
  }

  // SQL
  body += `<h2 style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:24px 0 8px;">SQL Query</h2>`;
  body += `<pre style="background:#0f172a;border:1px solid rgba(255,255,255,0.06);border-radius:8px;padding:14px 16px;overflow-x:auto;font-size:13px;line-height:1.6;color:#e2e8f0;"><code>${escHtml(m.statement_text)}</code></pre>`;

  // Metrics table
  body += `<h2 style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:28px 0 10px;">Performance Metrics</h2>`;
  body += `<table style="width:100%;border-collapse:collapse;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:8px;overflow:hidden;">`;
  body += metricsRow("Status", m.execution_status);
  if (m.total_duration_ms != null) body += metricsRow("Total Duration", formatMs(m.total_duration_ms));
  if (m.compilation_duration_ms != null) body += metricsRow("Compilation", formatMs(m.compilation_duration_ms));
  if (m.execution_duration_ms != null) body += metricsRow("Execution", formatMs(m.execution_duration_ms));
  if (m.waiting_for_compute_duration_ms != null) body += metricsRow("Compute Wait", formatMs(m.waiting_for_compute_duration_ms));
  if (m.waiting_at_capacity_duration_ms != null) body += metricsRow("Capacity Wait", formatMs(m.waiting_at_capacity_duration_ms));
  body += metricsRow("Rows Read", fmtNum(m.read_rows));
  body += metricsRow("Rows Produced", fmtNum(m.produced_rows));
  body += metricsRow("Data Read", humanBytes(m.read_bytes));
  body += metricsRow("Files Read", fmtNum(m.read_files));
  body += metricsRow("Files Pruned", fmtNum(m.pruned_files));
  body += metricsRow("Disk Spill", humanBytes(m.spilled_local_bytes));
  body += metricsRow("IO Cache Hit", m.read_io_cache_percent != null ? `${m.read_io_cache_percent}%` : "N/A");
  body += metricsRow("Shuffle Read", humanBytes(m.shuffle_read_bytes));
  body += metricsRow("Result Cache", m.from_result_cache ? "Yes" : "No");
  body += `</table>`;

  // Recommendations
  if (result.recommendations.length > 0) {
    body += `<h2 style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:28px 0 10px;">Recommendations <span style="background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff;font-size:11px;padding:2px 8px;border-radius:10px;margin-left:6px;">${result.recommendations.length}</span></h2>`;
    for (const r of result.recommendations) body += recHtml(r);
  }

  // Tables
  if (result.tables.length > 0) {
    body += `<h2 style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:28px 0 10px;">Tables</h2>`;
    for (const t of result.tables) {
      body += tableHtmlEntry(t, 0);
    }
  }

  // Plan
  if (result.plan_summary) {
    const plan = result.plan_summary;
    body += `<h2 style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:28px 0 10px;">Execution Plan</h2>`;
    const planAttrs: string[] = [];
    if (plan.join_types.length > 0) planAttrs.push(`Joins: ${plan.join_types.join(", ")}`);
    planAttrs.push(`Filter pushdown: ${plan.has_filter_pushdown ? "Yes" : "No"}`);
    planAttrs.push(`Partition pruning: ${plan.has_partition_pruning ? "Yes" : "No"}`);
    body += `<div style="font-size:12px;color:#94a3b8;margin-bottom:8px;">${planAttrs.map(escHtml).join(" · ")}</div>`;
    if (plan.scans.length > 0) {
      body += `<div style="font-size:12px;color:#94a3b8;margin-bottom:6px;"><strong>Scans:</strong></div><ul style="margin:0 0 8px 0;padding-left:20px;">`;
      for (const s of plan.scans) {
        body += `<li style="font-size:12px;color:#cbd5e1;margin-bottom:2px;">${escHtml(s.operator)} (${escHtml(s.format)})${s.table_name ? ` on <code style="background:rgba(255,255,255,0.06);padding:1px 5px;border-radius:4px;font-size:11px;">${escHtml(s.table_name)}</code>` : ""}${s.count > 1 ? ` ×${s.count}` : ""}</li>`;
      }
      body += `</ul>`;
    }
    if (plan.warnings.length > 0) {
      body += `<div style="font-size:12px;color:#f59e0b;margin-bottom:6px;"><strong>Warnings:</strong></div><ul style="margin:0 0 8px 0;padding-left:20px;">`;
      for (const w of plan.warnings) body += `<li style="font-size:12px;color:#fbbf24;margin-bottom:2px;">${escHtml(w)}</li>`;
      body += `</ul>`;
    }
    for (const r of plan.recommendations) body += recHtml(r);
  }

  // Warehouse
  if (result.warehouse) {
    const wh = result.warehouse;
    body += `<h2 style="font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:#64748b;font-weight:600;margin:28px 0 10px;">Warehouse</h2>`;
    body += `<div style="border:1px solid rgba(255,255,255,0.06);border-radius:8px;padding:12px 16px;margin-bottom:8px;background:rgba(255,255,255,0.02);">`;
    const attrs: string[] = [];
    if (wh.name) attrs.push(`Name: ${wh.name}`);
    if (wh.warehouse_type) attrs.push(`Type: ${wh.warehouse_type}`);
    if (wh.cluster_size) attrs.push(`Size: ${wh.cluster_size}`);
    if (wh.min_num_clusters != null) attrs.push(`Min clusters: ${wh.min_num_clusters}`);
    if (wh.max_num_clusters != null) attrs.push(`Max clusters: ${wh.max_num_clusters}`);
    if (wh.enable_photon != null) attrs.push(`Photon: ${wh.enable_photon ? "Enabled" : "Disabled"}`);
    if (wh.enable_serverless_compute != null) attrs.push(`Serverless: ${wh.enable_serverless_compute ? "Yes" : "No"}`);
    body += `<div style="font-size:12px;color:#94a3b8;margin-bottom:8px;">${attrs.map(escHtml).join(" · ")}</div>`;
    if (wh.activity) {
      body += `<div style="font-size:12px;color:#94a3b8;">Activity: ${wh.activity.concurrent_query_count} concurrent, ${wh.activity.queued_query_count} queued, ${wh.activity.total_queries_in_window} total</div>`;
    }
    body += `</div>`;
    for (const r of wh.recommendations) body += recHtml(r);
  }

  // Footer
  body += `<div style="margin-top:32px;padding-top:16px;border-top:1px solid rgba(255,255,255,0.06);font-size:12px;color:#475569;">Generated by Databricks SQL Architect</div>`;

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SQL Architect Report – ${escHtml(m.statement_id)}</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background: #060918; color: #e2e8f0; margin: 0; padding: 40px; line-height: 1.5; }
  code { font-family: 'SF Mono', 'Fira Code', 'Fira Mono', Menlo, monospace; }
  * { box-sizing: border-box; }
  @media (prefers-color-scheme: light) {
    body { background: #ffffff; color: #1e293b; }
  }
</style>
</head>
<body>
<div style="max-width:800px;margin:0 auto;">
${body}
</div>
</body>
</html>`;
}

// ---------------------------------------------------------------------------
// Download helpers
// ---------------------------------------------------------------------------

function downloadBlob(content: string, filename: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export function downloadMarkdown(result: AnalysisResult) {
  const md = generateMarkdown(result);
  const id = result.query_metrics.statement_id;
  downloadBlob(md, `sql-architect-report-${id}.md`, "text/markdown;charset=utf-8");
}

export function downloadHtml(result: AnalysisResult) {
  const html = generateHtml(result);
  const id = result.query_metrics.statement_id;
  downloadBlob(html, `sql-architect-report-${id}.html`, "text/html;charset=utf-8");
}
