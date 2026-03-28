from __future__ import annotations

import logging
import re

from backend.models import PlanSummary

logger = logging.getLogger(__name__)

SCAN_PATTERNS = [
    (re.compile(r"Scan\s+(\S+)", re.IGNORECASE), "Scan"),
    (re.compile(r"FileScan\s+(\S+)", re.IGNORECASE), "FileScan"),
    (re.compile(r"PhotonScan\s+(\S+)", re.IGNORECASE), "PhotonScan"),
]

JOIN_PATTERNS = [
    re.compile(r"(BroadcastHashJoin)", re.IGNORECASE),
    re.compile(r"(SortMergeJoin)", re.IGNORECASE),
    re.compile(r"(ShuffledHashJoin)", re.IGNORECASE),
    re.compile(r"(BroadcastNestedLoopJoin)", re.IGNORECASE),
    re.compile(r"(CartesianProduct)", re.IGNORECASE),
    re.compile(r"(PhotonBroadcastHashJoin)", re.IGNORECASE),
    re.compile(r"(PhotonShuffledHashJoin)", re.IGNORECASE),
    re.compile(r"(PhotonSortMergeJoin)", re.IGNORECASE),
]

FILTER_PUSHDOWN_PATTERN = re.compile(
    r"(PushedFilters|PushedPredicates|dataFilters)", re.IGNORECASE
)
PARTITION_PRUNING_PATTERN = re.compile(
    r"(PartitionFilters|partitionFilters)", re.IGNORECASE
)


def analyze_plan(raw_plan: str) -> PlanSummary:
    scan_types: list[str] = []
    join_types: list[str] = []
    warnings: list[str] = []
    has_filter_pushdown = False
    has_partition_pruning = False

    for pattern, label in SCAN_PATTERNS:
        for match in pattern.finditer(raw_plan):
            scan_types.append(f"{label}: {match.group(1)}")

    seen_joins: set[str] = set()
    for pattern in JOIN_PATTERNS:
        for match in pattern.finditer(raw_plan):
            jtype = match.group(1)
            if jtype not in seen_joins:
                seen_joins.add(jtype)
                join_types.append(jtype)

    if FILTER_PUSHDOWN_PATTERN.search(raw_plan):
        has_filter_pushdown = True

    if PARTITION_PRUNING_PATTERN.search(raw_plan):
        has_partition_pruning = True

    # Warnings based on plan content
    if "CartesianProduct" in raw_plan or "BroadcastNestedLoopJoin" in raw_plan:
        warnings.append(
            "Cartesian product or nested loop join detected -- "
            "this is extremely expensive for large datasets."
        )

    if "SortMergeJoin" in raw_plan and "BroadcastHashJoin" not in raw_plan:
        small_table_hint = re.search(
            r"SortMergeJoin.*?(\d+)\s*bytes", raw_plan, re.DOTALL
        )
        if small_table_hint:
            try:
                size = int(small_table_hint.group(1))
                if size < 100 * 1024 * 1024:
                    warnings.append(
                        "SortMergeJoin used but one side may be small enough "
                        "for a BroadcastHashJoin. Consider adding a broadcast hint."
                    )
            except ValueError:
                pass

    full_scan_count = raw_plan.lower().count("scan") - raw_plan.lower().count("pushdown")
    if full_scan_count > 5:
        warnings.append(
            f"High number of scan operations detected ({full_scan_count}). "
            "Verify that filters and clustering reduce data read."
        )

    return PlanSummary(
        raw_plan=raw_plan,
        scan_types=scan_types,
        join_types=join_types,
        has_filter_pushdown=has_filter_pushdown,
        has_partition_pruning=has_partition_pruning,
        warnings=warnings,
    )
