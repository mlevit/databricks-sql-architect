from __future__ import annotations

import logging

from backend.db import get_warehouse_config
from backend.models import Category, Recommendation, Severity, WarehouseInfo

logger = logging.getLogger(__name__)


def analyze_warehouse(warehouse_id: str) -> WarehouseInfo:
    try:
        config = get_warehouse_config(warehouse_id)
    except Exception as exc:
        logger.warning("Failed to fetch warehouse config for %s: %s", warehouse_id, exc)
        return WarehouseInfo(warehouse_id=warehouse_id)

    recs: list[Recommendation] = []

    # Photon not enabled
    if config.get("enable_photon") is False:
        recs.append(Recommendation(
            severity=Severity.WARNING,
            category=Category.WAREHOUSE,
            title="Photon not enabled",
            description=(
                "This warehouse does not have Photon enabled. Photon is Databricks' "
                "native vectorized engine that can dramatically speed up queries, "
                "especially scans, joins, and aggregations."
            ),
            action="Enable Photon on the warehouse configuration.",
            impact=7,
        ))

    # Classic warehouse type
    wh_type = config.get("warehouse_type", "")
    if wh_type and "CLASSIC" in str(wh_type).upper():
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.WAREHOUSE,
            title="Classic warehouse type",
            description=(
                "This warehouse uses the Classic type. Serverless or Pro warehouses "
                "offer faster startup, better scaling, and additional optimizations."
            ),
            action="Consider migrating to a Serverless or Pro SQL warehouse.",
            impact=4,
        ))

    # Single cluster
    num_clusters = config.get("num_clusters")
    if num_clusters is not None and num_clusters <= 1:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.WAREHOUSE,
            title="Single-cluster warehouse",
            description=(
                "The warehouse is configured with a maximum of 1 cluster. "
                "Concurrent queries will queue rather than scale out."
            ),
            action=(
                "If you experience queuing (high waiting_at_capacity), "
                "increase the max number of clusters for auto-scaling."
            ),
            impact=3,
        ))

    # W4: Workload isolation
    is_serverless = config.get("enable_serverless_compute") is True
    if not is_serverless:
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.WAREHOUSE,
            title="Consider workload isolation",
            description=(
                "This warehouse is not Serverless and serves all workloads on "
                "shared compute. Heavy ad-hoc analytical queries running alongside "
                "dashboard refresh queries can cause resource contention and queuing."
            ),
            action=(
                "Use separate SQL warehouses for different workload types: "
                "one for interactive dashboards, one for ad-hoc analysis, and one "
                "for scheduled ETL. Serverless warehouses auto-scale and provide "
                "built-in isolation."
            ),
            impact=2,
        ))

    return WarehouseInfo(
        warehouse_id=config.get("warehouse_id", warehouse_id),
        name=config.get("name"),
        warehouse_type=str(wh_type) if wh_type else None,
        cluster_size=config.get("cluster_size"),
        num_clusters=num_clusters,
        enable_photon=config.get("enable_photon"),
        spot_instance_policy=config.get("spot_instance_policy"),
        channel=config.get("channel"),
        recommendations=recs,
    )
