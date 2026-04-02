from __future__ import annotations

import logging

from backend.db import (
    fetch_concurrent_queries,
    fetch_query_load_timeline,
    fetch_scaling_events,
    get_warehouse_config,
)
from backend.models import (
    Category,
    QueryLoadPoint,
    Recommendation,
    ScalingEvent,
    Severity,
    WarehouseActivity,
    WarehouseInfo,
)

logger = logging.getLogger(__name__)

HIGH_CONCURRENCY_THRESHOLD = 10
HIGH_QUEUED_RATIO = 0.25


def analyze_warehouse(
    warehouse_id: str,
    *,
    statement_id: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> WarehouseInfo:
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
    max_num_clusters = config.get("max_num_clusters")
    if max_num_clusters is not None and max_num_clusters <= 1:
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

    activity = _fetch_activity(
        warehouse_id, statement_id, start_time, end_time,
        fallback_cluster_count=config.get("num_clusters"),
    )
    if activity:
        recs.extend(_activity_recommendations(activity))

    return WarehouseInfo(
        warehouse_id=config.get("warehouse_id", warehouse_id),
        name=config.get("name"),
        warehouse_type=str(wh_type) if wh_type else None,
        cluster_size=config.get("cluster_size"),
        min_num_clusters=config.get("min_num_clusters"),
        max_num_clusters=max_num_clusters,
        num_clusters=config.get("num_clusters"),
        auto_stop_mins=config.get("auto_stop_mins"),
        enable_photon=config.get("enable_photon"),
        enable_serverless_compute=config.get("enable_serverless_compute"),
        spot_instance_policy=config.get("spot_instance_policy"),
        channel=config.get("channel"),
        activity=activity,
        recommendations=recs,
    )


def _fetch_activity(
    warehouse_id: str,
    statement_id: str | None,
    start_time: str | None,
    end_time: str | None,
    *,
    fallback_cluster_count: int | None = None,
) -> WarehouseActivity | None:
    """Best-effort fetch of warehouse activity during the query window."""
    if not (statement_id and start_time and end_time):
        return None

    concurrent: dict[str, int] = {"total_queries": 0, "queued_queries": 0}
    try:
        concurrent = fetch_concurrent_queries(
            warehouse_id, statement_id, start_time, end_time,
        )
    except Exception as exc:
        logger.warning("Failed to fetch concurrent queries: %s", exc)

    scaling: list[ScalingEvent] = []
    active_cluster_count: int | None = None
    try:
        raw_events = fetch_scaling_events(warehouse_id, start_time, end_time)
        for ev in raw_events:
            scaling.append(ScalingEvent(
                event_time=str(ev.get("event_time", "")),
                event_type=str(ev.get("event_type", "")),
                cluster_count=int(ev.get("cluster_count", 0)),
            ))
        if scaling:
            active_cluster_count = scaling[0].cluster_count
    except Exception as exc:
        logger.warning("Failed to fetch scaling events: %s", exc)

    if active_cluster_count is None and fallback_cluster_count is not None:
        active_cluster_count = fallback_cluster_count

    query_load: list[QueryLoadPoint] = []
    try:
        raw_load = fetch_query_load_timeline(warehouse_id, start_time, end_time)
        for pt in raw_load:
            query_load.append(QueryLoadPoint(
                time=str(pt.get("bucket_time", "")),
                running=int(pt.get("running_count", 0)),
                queued=int(pt.get("queued_count", 0)),
            ))
    except Exception as exc:
        logger.warning("Failed to fetch query load timeline: %s", exc)

    peak_concurrent = max((pt.running for pt in query_load), default=0)

    return WarehouseActivity(
        time_window_start=start_time,
        time_window_end=end_time,
        concurrent_query_count=peak_concurrent,
        queued_query_count=concurrent["queued_queries"],
        total_queries_in_window=concurrent["total_queries"],
        active_cluster_count=active_cluster_count,
        scaling_events=scaling,
        query_load=query_load,
    )


def _activity_recommendations(activity: WarehouseActivity) -> list[Recommendation]:
    """Generate recommendations based on warehouse activity at query time."""
    recs: list[Recommendation] = []

    total = activity.concurrent_query_count
    queued = activity.queued_query_count

    if total >= HIGH_CONCURRENCY_THRESHOLD and queued > 0:
        queued_pct = queued / total if total else 0
        if queued_pct >= HIGH_QUEUED_RATIO:
            recs.append(Recommendation(
                severity=Severity.WARNING,
                category=Category.WAREHOUSE,
                title="High warehouse load with queuing",
                description=(
                    f"During this query's execution, {total} other queries were running "
                    f"on the same warehouse, with {queued} experiencing queuing. "
                    "This level of contention likely contributed to slower performance."
                ),
                action=(
                    "Increase the max number of clusters for auto-scaling, use a "
                    "dedicated warehouse for heavy workloads, or schedule resource-"
                    "intensive queries during off-peak hours."
                ),
                impact=6,
            ))
        else:
            recs.append(Recommendation(
                severity=Severity.INFO,
                category=Category.WAREHOUSE,
                title="High concurrent query load",
                description=(
                    f"During this query's execution, {total} other queries were running "
                    "on the same warehouse. While minimal queuing was observed, high "
                    "concurrency can still cause resource contention."
                ),
                action=(
                    "Monitor warehouse utilization trends. If performance degrades, "
                    "consider scaling up or isolating workloads."
                ),
                impact=3,
            ))

    scale_ups = [e for e in activity.scaling_events if e.event_type == "SCALED_UP"]
    if scale_ups:
        max_cluster = max(e.cluster_count for e in activity.scaling_events)
        min_cluster = min(
            e.cluster_count for e in activity.scaling_events
            if e.event_type in ("SCALED_UP", "SCALED_DOWN", "RUNNING")
        ) if activity.scaling_events else 0
        recs.append(Recommendation(
            severity=Severity.INFO,
            category=Category.WAREHOUSE,
            title="Warehouse scaled during query execution",
            description=(
                f"The warehouse scaled up {len(scale_ups)} time(s) during or around "
                f"query execution, reaching {max_cluster} cluster(s) from {min_cluster}. "
                "This indicates demand exceeded the current cluster count."
            ),
            action=(
                "If scaling delays affected query latency, consider pre-warming "
                "the warehouse with a higher minimum cluster count."
            ),
            impact=4,
        ))

    return recs
