import type { WarehouseInfo as WarehouseInfoType } from "../types";
import { RecommendationCard } from "./shared/recommendation";

interface Props {
  warehouse: WarehouseInfoType;
}

export default function WarehouseInfo({ warehouse }: Props) {
  const photonValue =
    warehouse.enable_photon === true
      ? "Enabled"
      : warehouse.enable_photon === false
        ? "Disabled"
        : "Unknown";

  const details: { label: string; value: string }[] = [
    { label: "Name", value: warehouse.name || "N/A" },
    { label: "Type", value: warehouse.warehouse_type || "N/A" },
    { label: "Size", value: warehouse.cluster_size || "N/A" },
    { label: "Clusters", value: warehouse.num_clusters?.toString() || "N/A" },
    { label: "Photon", value: photonValue },
    { label: "Spot Policy", value: warehouse.spot_instance_policy || "N/A" },
    { label: "Channel", value: warehouse.channel || "N/A" },
  ];

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">Warehouse Configuration</h2>
      <div className="grid grid-cols-[repeat(auto-fill,minmax(150px,1fr))] gap-3 mb-3">
        {details.map((d) => (
          <div key={d.label} className="flex flex-col">
            <span className="text-[0.68rem] uppercase tracking-wide text-gray-400 font-medium">
              {d.label}
            </span>
            <span className="font-semibold text-sm">{d.value}</span>
          </div>
        ))}
      </div>

      {warehouse.recommendations.length > 0 && (
        <div className="flex flex-col gap-2">
          {warehouse.recommendations.map((r, i) => (
            <RecommendationCard key={i} recommendation={r} variant="compact" />
          ))}
        </div>
      )}
    </div>
  );
}
