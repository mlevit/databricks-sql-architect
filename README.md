# Databricks Query Performance Analyzer

A Databricks App that analyzes SQL query performance and provides actionable recommendations for optimization.

Given a `statement_id`, the app inspects the query text, execution plan, runtime metrics, underlying table metadata, and warehouse configuration to surface concrete improvement suggestions.

## Features

- **Query Metrics Analysis** — duration breakdown, spill to disk, data skipping effectiveness, cache utilization, I/O volumes
- **SQL Pattern Detection** — identifies `SELECT *`, missing filters, functions on filter/join columns, cross joins, and more
- **Execution Plan Inspection** — parses `EXPLAIN EXTENDED` output for full table scans, join strategies, and filter pushdown
- **Table Metadata** — checks Delta table clustering, partitioning, file count, and sizing via `DESCRIBE DETAIL`
- **Warehouse Configuration** — validates Photon enablement, warehouse type, and cluster size
- **AI Query Rewrite** — uses `ai_query` with Claude to suggest an optimized version of the query, with a side-by-side diff view
- **Shareable URLs** — analysis links include the `statement_id` so results can be shared with teammates
- **Real-time Progress** — server-sent events stream analysis status to a progress stepper in the UI

## Architecture

| Layer | Tech | Description |
|-------|------|-------------|
| **Frontend** | React + Vite + TypeScript | Tabbed dashboard with metrics cards, recommendations, plan viewer, and AI rewrite panel |
| **Backend** | FastAPI + Uvicorn | REST + SSE API that orchestrates analysis modules |
| **Data** | Databricks Python SDK + SQL | Queries `system.query.history`, runs `EXPLAIN` / `DESCRIBE DETAIL`, calls warehouse APIs |
| **AI** | `ai_query` (Claude) | Rewrites queries based on detected issues |

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Access to `system.query.history`
- A SQL warehouse configured as an app resource

## Deployment

This app is designed to run on [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html).

1. **Configure the app resource** — add a SQL warehouse resource with the key `sql-warehouse` in your Databricks App settings
2. **Deploy** — use the Databricks CLI or UI to deploy the app from this repository

The `app.yaml` maps the warehouse resource to the `DATABRICKS_WAREHOUSE_ID` environment variable automatically.

## Local Development

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install frontend dependencies and build
npm install
npm run build

# Set environment variables
export DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>

# Start the server
uvicorn backend.main:app --reload
```

## Project Structure

```
├── app.yaml                    # Databricks App config
├── backend/
│   ├── main.py                 # FastAPI app and routes
│   ├── analyzer.py             # Analysis orchestrator
│   ├── db.py                   # Databricks SDK wrapper
│   ├── models.py               # Pydantic data models
│   └── analyzers/
│       ├── sql_parser.py       # SQL parsing with sqlglot
│       ├── query_metrics.py    # Execution metrics analysis
│       ├── plan_analyzer.py    # EXPLAIN plan parsing
│       ├── table_analyzer.py   # Table metadata checks
│       ├── warehouse_analyzer.py  # Warehouse config checks
│       └── ai_advisor.py       # AI-powered query rewrite
├── frontend/
│   ├── index.html
│   └── src/
│       ├── App.tsx             # Main app with tabbed layout
│       ├── api.ts              # API client with SSE support
│       ├── types.ts            # TypeScript interfaces
│       └── components/         # React components
├── requirements.txt
└── package.json
```
