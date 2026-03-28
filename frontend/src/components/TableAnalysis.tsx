import { useState } from "react";
import type { TableInfo } from "../types";

interface Props {
  tables: TableInfo[];
}

export default function TableAnalysis({ tables }: Props) {
  if (tables.length === 0) return null;

  return (
    <div className="panel table-analysis">
      <h2>Table Analysis</h2>
      <div className="table-analysis__list">
        {tables.map((t) => (
          <TableCard key={t.full_name} table={t} />
        ))}
      </div>
    </div>
  );
}

function TableCard({ table }: { table: TableInfo }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="table-card">
      <button
        className="table-card__header"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="table-card__name">{table.full_name}</span>
        <span className="table-card__meta">
          {table.format && <span className="badge badge--neutral">{table.format}</span>}
          {table.num_files != null && <span>{table.num_files.toLocaleString()} files</span>}
          {table.size_in_bytes != null && <span>{humanBytes(table.size_in_bytes)}</span>}
        </span>
        <span className={`table-card__chevron ${expanded ? "expanded" : ""}`}>&#9660;</span>
      </button>

      {expanded && (
        <div className="table-card__body">
          <div className="table-card__detail">
            <strong>Clustering:</strong>{" "}
            {table.clustering_columns.length > 0
              ? table.clustering_columns.join(", ")
              : "None"}
          </div>
          <div className="table-card__detail">
            <strong>Partitioning:</strong>{" "}
            {table.partition_columns.length > 0
              ? table.partition_columns.join(", ")
              : "None"}
          </div>

          {table.recommendations.length > 0 && (
            <div className="table-card__recs">
              {table.recommendations.map((r, i) => (
                <div key={i} className={`rec-inline rec-inline--${r.severity}`}>
                  <span className="rec-inline__title">{r.title}</span>
                  <span className="rec-inline__desc">{r.description}</span>
                  {r.action && (
                    <code className="rec-inline__action">{r.action}</code>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function humanBytes(b: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let val = b;
  while (Math.abs(val) >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
