from __future__ import annotations

import logging
import re

from backend.models import PlanHighlight, PlanSummary, Severity

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
LARGE_SCAN_BYTES_THRESHOLD = 1024 * 1024 * 1024  # 1 GB
BROADCAST_TOO_LARGE_THRESHOLD = 500 * 1024 * 1024  # 500 MB

# C6: Scan size extraction near join operators
SCAN_SIZE_RE = re.compile(
    r"(?:File|Photon)?Scan\s+(\S+).*?sizeInBytes=(\d+)",
    re.IGNORECASE | re.DOTALL,
)

# C7: Broadcast side size
BROADCAST_SIZE_RE = re.compile(
    r"Broadcast(?:Hash|NestedLoop)Join.*?(\d+)\s*bytes",
    re.IGNORECASE | re.DOTALL,
)


def _offset_to_line(raw_plan: str) -> list[int]:
    """Build a list mapping character offsets to 0-indexed line numbers."""
    line_starts: list[int] = [0]
    for i, ch in enumerate(raw_plan):
        if ch == "\n":
            line_starts.append(i + 1)
    return line_starts


def _line_of(line_starts: list[int], offset: int) -> int:
    """Return the 0-indexed line number for a character offset (binary search)."""
    lo, hi = 0, len(line_starts) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if line_starts[mid] <= offset:
            lo = mid
        else:
            hi = mid - 1
    return lo


def _add_highlight(
    highlights: list[PlanHighlight],
    line_starts: list[int],
    match: re.Match[str],
    severity: Severity,
    reason: str,
) -> None:
    start_line = _line_of(line_starts, match.start())
    end_line = _line_of(line_starts, match.end() - 1) if match.end() > match.start() else start_line
    highlights.append(PlanHighlight(
        line_start=start_line,
        line_end=end_line,
        severity=severity,
        reason=reason,
    ))


def analyze_plan(raw_plan: str) -> PlanSummary:
    scan_types: list[str] = []
    join_types: list[str] = []
    warnings: list[str] = []
    highlights: list[PlanHighlight] = []
    has_filter_pushdown = False
    has_partition_pruning = False

    line_starts = _offset_to_line(raw_plan)

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
    cartesian_re = re.compile(r"\b(CartesianProduct|BroadcastNestedLoopJoin)\b", re.IGNORECASE)
    for match in cartesian_re.finditer(raw_plan):
        _add_highlight(highlights, line_starts, match, Severity.CRITICAL,
                       "Cartesian product or nested loop join — extremely expensive for large datasets")
    if cartesian_re.search(raw_plan):
        warnings.append(
            "Cartesian product or nested loop join detected -- "
            "this is extremely expensive for large datasets."
        )

    # SortMergeJoin that could be BroadcastHashJoin
    smj_re = re.compile(r"\bSortMergeJoin\b", re.IGNORECASE)
    if smj_re.search(raw_plan) and "BroadcastHashJoin" not in raw_plan:
        for match in smj_re.finditer(raw_plan):
            _add_highlight(highlights, line_starts, match, Severity.WARNING,
                           "SortMergeJoin — consider a broadcast hint if one side is small")
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

    # C1: Full scans without filter pushdown
    _check_scans_without_pushdown(raw_plan, warnings, highlights, line_starts)

    # C2: Exchange / shuffle node counting
    _check_exchange_count(raw_plan, warnings, highlights, line_starts)

    # C3: Sort operation detection
    _check_sort_count(raw_plan, warnings, highlights, line_starts)

    # C4: Skew join hints
    _check_skew_indicators(raw_plan, warnings, highlights, line_starts)

    # C5: Scans with no partition pruning
    _check_scans_without_partition_pruning(raw_plan, warnings, highlights, line_starts)

    # C6: Two large scan sides in a join (fact-to-fact)
    _check_large_fact_join(raw_plan, warnings, highlights, line_starts)

    # C7: Broadcast join with a table that's too large
    _check_broadcast_too_large(raw_plan, warnings, highlights, line_starts)

    return PlanSummary(
        raw_plan=raw_plan,
        scan_types=scan_types,
        join_types=join_types,
        has_filter_pushdown=has_filter_pushdown,
        has_partition_pruning=has_partition_pruning,
        warnings=warnings,
        highlights=highlights,
    )


def _check_scans_without_pushdown(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
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
        _add_highlight(highlights, line_starts, block_match, Severity.WARNING,
                       f"Full scan on {table_name} — no filter pushdown, all data is read before filtering")

    if unpushed_tables:
        tables_str = ", ".join(dict.fromkeys(unpushed_tables))
        warnings.append(
            f"Full scan without filter pushdown on: {tables_str}. "
            "No predicates are being pushed to the scan layer, so the engine must "
            "read all data before filtering."
        )


def _check_exchange_count(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
    """C2: High number of exchange (shuffle) nodes indicates excessive redistribution."""
    matches = list(EXCHANGE_RE.finditer(raw_plan))
    count = len(matches)
    if count > HIGH_EXCHANGE_COUNT:
        for match in matches:
            _add_highlight(highlights, line_starts, match, Severity.WARNING,
                           f"Shuffle/exchange operation ({count} total) — data is redistributed across the network")
        warnings.append(
            f"High number of data exchange operations ({count}). "
            "Each exchange shuffles data across the network. Consider reducing "
            "joins, using broadcast hints for small tables, or clustering tables "
            "on join keys to co-locate data."
        )


def _check_sort_count(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
    """C3: Excessive sort operations suggest unnecessary ORDER BY or missing clustering."""
    matches = list(SORT_RE.finditer(raw_plan))
    count = len(matches)
    if count > HIGH_SORT_COUNT:
        for match in matches:
            _add_highlight(highlights, line_starts, match, Severity.WARNING,
                           f"Sort operation ({count} total) — expensive for large datasets, check for unnecessary ORDER BY")
        warnings.append(
            f"High number of sort operations ({count}). "
            "Sorts are expensive for large datasets. Check for unnecessary ORDER BY "
            "clauses in subqueries or CTEs, and ensure tables are clustered to "
            "reduce sort requirements."
        )


def _check_skew_indicators(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
    """C4: Detect skew-related plan operators."""
    iter_matches = list(SKEW_RE.finditer(raw_plan))
    if iter_matches:
        indicators = ", ".join(dict.fromkeys(m.group(0) for m in iter_matches))
        for match in iter_matches:
            _add_highlight(highlights, line_starts, match, Severity.WARNING,
                           "Data skew detected — uneven partition distribution causes hot partitions")
        warnings.append(
            f"Data skew handling detected in plan ({indicators}). "
            "The optimizer identified uneven data distribution across partitions. "
            "Investigate the join key distribution — a few values may hold most of "
            "the data, causing hot partitions."
        )


def _check_scans_without_partition_pruning(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
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
            _add_highlight(highlights, line_starts, block_match, Severity.WARNING,
                           f"No partition pruning on {table_name} — all partitions are scanned")

    if unpruned_tables:
        tables_str = ", ".join(dict.fromkeys(unpruned_tables))
        warnings.append(
            f"Scan without partition pruning on: {tables_str}. "
            "The table appears to be partitioned but the query does not filter "
            "on the partition column, so all partitions are scanned."
        )


def _check_large_fact_join(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
    """C6: Detect joins where both scan sides exceed LARGE_SCAN_BYTES_THRESHOLD."""
    scan_sizes: list[tuple[str, int, re.Match[str]]] = []
    for match in SCAN_SIZE_RE.finditer(raw_plan):
        table_name = match.group(1).strip("[](),")
        try:
            size = int(match.group(2))
            scan_sizes.append((table_name, size, match))
        except (ValueError, TypeError):
            pass

    large_scans = [(t, s, m) for t, s, m in scan_sizes if s >= LARGE_SCAN_BYTES_THRESHOLD]
    if len(large_scans) >= 2:
        for table_name, size, match in large_scans[:3]:
            _add_highlight(highlights, line_starts, match, Severity.CRITICAL,
                           f"Large table scan ({size / (1024 ** 3):.1f} GB) — fact-to-fact join is extremely expensive")
        tables_str = ", ".join(f"{t} ({s / (1024 ** 3):.1f} GB)" for t, s, _ in large_scans[:3])
        warnings.append(
            f"Large fact-to-fact join detected: {tables_str}. "
            "Joining two large tables directly is extremely expensive. "
            "Consider pre-aggregating one side, joining through a dimension table, "
            "or using a broadcast hint if one side can be reduced via filtering."
        )


def _check_broadcast_too_large(
    raw_plan: str,
    warnings: list[str],
    highlights: list[PlanHighlight],
    line_starts: list[int],
) -> None:
    """C7: Detect broadcast joins where the broadcast side is too large."""
    for match in BROADCAST_SIZE_RE.finditer(raw_plan):
        try:
            size = int(match.group(1))
            if size > BROADCAST_TOO_LARGE_THRESHOLD:
                size_mb = size / (1024 * 1024)
                _add_highlight(highlights, line_starts, match, Severity.CRITICAL,
                               f"Broadcast join with oversized table ({size_mb:,.0f} MB) — risk of OOM errors")
                warnings.append(
                    f"Broadcast join with large table ({size_mb:,.0f} MB). "
                    "Broadcasting a table larger than ~500 MB can cause out-of-memory "
                    "errors on executor nodes. Remove the broadcast hint and let the "
                    "optimizer choose a shuffle-based join strategy instead."
                )
                return
        except (ValueError, TypeError):
            pass
