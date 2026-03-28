import type { WarehouseInfo as WarehouseInfoType } from "../types";

interface Props {
  warehouse: WarehouseInfoType;
}

const SEVERITY_STYLES: Record<string, string> = {
  critical: "bg-red-50 border-l-red-500",
  warning: "bg-amber-50 border-l-amber-500",
  info: "bg-blue-50 border-l-blue-600",
};

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
        <div className="flex flex-col gap-1.5">
          {warehouse.recommendations.map((r, i) => (
            <div
              key={i}
              className={`p-2 rounded text-[0.78rem] flex flex-col gap-0.5 border-l-[3px] ${SEVERITY_STYLES[r.severity] || "bg-gray-50 border-l-gray-300"}`}
            >
              <span className="font-semibold">{r.title}</span>
              <span className="text-gray-500">{r.description}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
