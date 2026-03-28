import type { WarehouseInfo as WarehouseInfoType } from "../types";

interface Props {
  warehouse: WarehouseInfoType;
}

export default function WarehouseInfo({ warehouse }: Props) {
  const details: { label: string; value: string }[] = [
    { label: "Name", value: warehouse.name || "N/A" },
    { label: "Type", value: warehouse.warehouse_type || "N/A" },
    { label: "Size", value: warehouse.cluster_size || "N/A" },
    { label: "Clusters", value: warehouse.num_clusters?.toString() || "N/A" },
    { label: "Photon", value: warehouse.enable_photon ? "Enabled" : "Disabled" },
    { label: "Spot Policy", value: warehouse.spot_instance_policy || "N/A" },
    { label: "Channel", value: warehouse.channel || "N/A" },
  ];

  return (
    <div className="panel warehouse-info">
      <h2>Warehouse Configuration</h2>
      <div className="warehouse-info__grid">
        {details.map((d) => (
          <div key={d.label} className="warehouse-info__item">
            <span className="warehouse-info__label">{d.label}</span>
            <span className="warehouse-info__value">{d.value}</span>
          </div>
        ))}
      </div>

      {warehouse.recommendations.length > 0 && (
        <div className="warehouse-info__recs">
          {warehouse.recommendations.map((r, i) => (
            <div key={i} className={`rec-inline rec-inline--${r.severity}`}>
              <span className="rec-inline__title">{r.title}</span>
              <span className="rec-inline__desc">{r.description}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
