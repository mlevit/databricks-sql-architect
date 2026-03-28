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

# C1: Per-scan pushdown detection
SCAN_BLOCK_RE = re.compile(
    r"((?:File|Photon)?Scan\s+\S+.*?)(?=\n\s*\n|\n\S|\Z)",
    re.IGNORECASE | re.DOTALL,
)
PUSHED_FILTERS_RE = re.compile(
    r"PushedFilters:\s*\[([^\]]*)\]", re.IGNORECASE
)
PUSHED_PREDICATES_RE = re.compile(
    r"PushedPredicates:\s*\[([^\]]*)\]", re.IGNORECASE
)

# C2: Exchange / shuffle nodes
EXCHANGE_RE = re.compile(r"\b(Exchange|ShuffleExchange|PhotonShuffleExchange)\b", re.IGNORECASE)

# C3: Sort operations
SORT_RE = re.compile(r"\bSort\b", re.IGNORECASE)

# C4: Skew indicators
SKEW_RE = re.compile(r"\b(SkewJoin|AQEShuffleRead|OptimizeSkewedJoin)\b", re.IGNORECASE)

# C5: Per-scan partition filter detection
PARTITION_FILTERS_RE = re.compile(
    r"PartitionFilters:\s*\[([^\]]*)\]", re.IGNORECASE
)

HIGH_EXCHANGE_COUNT = 5
HIGH_SORT_COUNT = 4


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

    # Cartesian product / nested loop
    if "CartesianProduct" in raw_plan or "BroadcastNestedLoopJoin" in raw_plan:
        warnings.append(
            "Cartesian product or nested loop join detected -- "
            "this is extremely expensive for large datasets."
        )

    # SortMergeJoin that could be BroadcastHashJoin
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

    # C1: Full scans without filter pushdown (replaces crude count heuristic)
    _check_scans_without_pushdown(raw_plan, warnings)

    # C2: Exchange / shuffle node counting
    _check_exchange_count(raw_plan, warnings)

    # C3: Sort operation detection
    _check_sort_count(raw_plan, warnings)

    # C4: Skew join hints
    _check_skew_indicators(raw_plan, warnings)

    # C5: Scans with no partition pruning
    _check_scans_without_partition_pruning(raw_plan, warnings)

    return PlanSummary(
        raw_plan=raw_plan,
        scan_types=scan_types,
        join_types=join_types,
        has_filter_pushdown=has_filter_pushdown,
        has_partition_pruning=has_partition_pruning,
        warnings=warnings,
    )


def _check_scans_without_pushdown(raw_plan: str, warnings: list[str]) -> None:
    """C1: Identify scan operations that have empty PushedFilters/PushedPredicates."""
    unpushed_tables: list[str] = []

    for block_match in SCAN_BLOCK_RE.finditer(raw_plan):
        block = block_match.group(0)
        table_match = re.search(r"(?:File|Photon)?Scan\s+(\S+)", block, re.IGNORECASE)
        if not table_match:
            continue
        table_name = table_match.group(1).strip("[](),")

        pushed = PUSHED_FILTERS_RE.search(block) or PUSHED_PREDICATES_RE.search(block)
        if pushed:
            content = pushed.group(1).strip()
            if content:
                continue

        unpushed_tables.append(table_name)

    if unpushed_tables:
        tables_str = ", ".join(dict.fromkeys(unpushed_tables))
        warnings.append(
            f"Full scan without filter pushdown on: {tables_str}. "
            "No predicates are being pushed to the scan layer, so the engine must "
            "read all data before filtering."
        )


def _check_exchange_count(raw_plan: str, warnings: list[str]) -> None:
    """C2: High number of exchange (shuffle) nodes indicates excessive redistribution."""
    count = len(EXCHANGE_RE.findall(raw_plan))
    if count > HIGH_EXCHANGE_COUNT:
        warnings.append(
            f"High number of data exchange operations ({count}). "
            "Each exchange shuffles data across the network. Consider reducing "
            "joins, using broadcast hints for small tables, or clustering tables "
            "on join keys to co-locate data."
        )


def _check_sort_count(raw_plan: str, warnings: list[str]) -> None:
    """C3: Excessive sort operations suggest unnecessary ORDER BY or missing clustering."""
    count = len(SORT_RE.findall(raw_plan))
    if count > HIGH_SORT_COUNT:
        warnings.append(
            f"High number of sort operations ({count}). "
            "Sorts are expensive for large datasets. Check for unnecessary ORDER BY "
            "clauses in subqueries or CTEs, and ensure tables are clustered to "
            "reduce sort requirements."
        )


def _check_skew_indicators(raw_plan: str, warnings: list[str]) -> None:
    """C4: Detect skew-related plan operators."""
    matches = SKEW_RE.findall(raw_plan)
    if matches:
        indicators = ", ".join(dict.fromkeys(matches))
        warnings.append(
            f"Data skew handling detected in plan ({indicators}). "
            "The optimizer identified uneven data distribution across partitions. "
            "Investigate the join key distribution — a few values may hold most of "
            "the data, causing hot partitions."
        )


def _check_scans_without_partition_pruning(raw_plan: str, warnings: list[str]) -> None:
    """C5: Identify scans with empty PartitionFilters on partitioned tables."""
    unpruned_tables: list[str] = []

    for block_match in SCAN_BLOCK_RE.finditer(raw_plan):
        block = block_match.group(0)
        table_match = re.search(r"(?:File|Photon)?Scan\s+(\S+)", block, re.IGNORECASE)
        if not table_match:
            continue
        table_name = table_match.group(1).strip("[](),")

        pf_match = PARTITION_FILTERS_RE.search(block)
        if pf_match and not pf_match.group(1).strip():
            unpruned_tables.append(table_name)

    if unpruned_tables:
        tables_str = ", ".join(dict.fromkeys(unpruned_tables))
        warnings.append(
            f"Scan without partition pruning on: {tables_str}. "
            "The table appears to be partitioned but the query does not filter "
            "on the partition column, so all partitions are scanned."
        )
