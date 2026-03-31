import type { WarehouseInfo as WarehouseInfoType, TableInfo, PlanSummary as PlanSummaryType } from "../types";
import WarehouseInfo from "./WarehouseInfo";
import TableAnalysis from "./TableAnalysis";
import PlanSummary from "./PlanSummary";

interface Props {
  warehouse: WarehouseInfoType | null;
  tables: TableInfo[];
  planSummary: PlanSummaryType | null;
}

export default function SystemTab({ warehouse, tables, planSummary }: Props) {
  return (
    <div className="flex flex-col gap-5">
      {warehouse ? (
        <WarehouseInfo warehouse={warehouse} />
      ) : (
        <div className="glass-card p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-2">Warehouse</h2>
          <p className="text-slate-500">No warehouse information available.</p>
        </div>
      )}
      <TableAnalysis tables={tables} />
      {planSummary ? (
        <PlanSummary plan={planSummary} />
      ) : (
        <div className="glass-card p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-2">Execution Plan</h2>
          <p className="text-slate-500">No execution plan available. EXPLAIN is only supported for SELECT statements.</p>
        </div>
      )}
    </div>
  );
}
