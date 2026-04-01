import type { Recommendation } from "../types";
import Recommendations from "./Recommendations";
import AIRewrite from "./AIRewrite";

interface Props {
  recommendations: Recommendation[];
  statementId: string;
  warehouseId?: string;
}

export default function OptimizationTab({ recommendations, statementId, warehouseId }: Props) {
  return (
    <div className="flex flex-col gap-5">
      <Recommendations recommendations={recommendations} />
      <AIRewrite statementId={statementId} warehouseId={warehouseId} />
    </div>
  );
}
