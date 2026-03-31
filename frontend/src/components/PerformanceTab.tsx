import type { QueryMetrics } from "../types";
import QueryOverview from "./QueryOverview";
import MetricsCards from "./MetricsCards";

interface Props {
  metrics: QueryMetrics;
}

export default function PerformanceTab({ metrics }: Props) {
  return (
    <div className="flex flex-col gap-5">
      <QueryOverview metrics={metrics} />
      <MetricsCards metrics={metrics} />
    </div>
  );
}
