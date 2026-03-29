import type { QueryMetrics } from "../types";
import { formatNumber, humanBytes } from "../utils";

interface Props {
  metrics: QueryMetrics;
}

type MetricStatus = "good" | "warning" | "bad" | "neutral";

interface MetricCard {
  label: string;
  value: string;
  sub?: string;
  status: MetricStatus;
  context: string;
}

const GB = 1_073_741_824;

const STATUS_STYLES: Record<MetricStatus, { card: string; context: string }> = {
  good: {
    card: "border-l-[3px] border-l-emerald-500 bg-emerald-50/50",
    context: "text-emerald-600",
  },
  warning: {
    card: "border-l-[3px] border-l-amber-400 bg-amber-50/50",
    context: "text-amber-600",
  },
  bad: {
    card: "border-l-[3px] border-l-red-500 bg-red-50/50",
    context: "text-red-600",
  },
  neutral: {
    card: "",
    context: "text-gray-400",
  },
};

function buildCards(m: QueryMetrics): MetricCard[] {
  const cards: MetricCard[] = [];

  // Rows Read — compare produced vs read for selectivity
  {
    const read = m.read_rows;
    const produced = m.produced_rows;
    let status: MetricStatus = "neutral";
    let context = "";

    if (read != null && read > 0 && produced != null) {
      if (produced === 0) {
        status = "warning";
        context = "No rows produced — full scan may be avoidable";
      } else {
        const ratio = produced / read;
        const mult = Math.round(read / produced).toLocaleString();
        if (ratio >= 0.1) {
          status = "good";
          context = "Good selectivity — low read amplification";
        } else if (ratio >= 0.01) {
          status = "warning";
          context = `Reading ${mult}× more rows than produced`;
        } else {
          status = "bad";
          context = `Poor selectivity — reading ${mult}× more rows than produced`;
        }
      }
    } else if (read != null && read > 0) {
      if (read < 10_000_000) {
        status = "good";
        context = "Moderate row count";
      } else if (read < 1_000_000_000) {
        status = "warning";
        context = "High row count — review filters";
      } else {
        status = "bad";
        context = "Very high row count — add filters or partitioning";
      }
    } else if (read === 0) {
      status = "good";
      context = "No rows scanned";
    }

    cards.push({
      label: "Rows Read",
      value: formatNumber(read),
      sub: produced != null ? `${formatNumber(produced)} produced` : undefined,
      status,
      context,
    });
  }

  // Data Read
  {
    const bytes = m.read_bytes;
    let status: MetricStatus = "neutral";
    let context = "";

    if (bytes != null) {
      if (bytes === 0) {
        status = "good";
        context = "No data scanned";
      } else if (bytes < GB) {
        status = "good";
        context = "Small data scan";
      } else if (bytes < 100 * GB) {
        status = "warning";
        context = "Moderate data volume — check columns and filters";
      } else {
        status = "bad";
        context = "Large data scan — optimize filters or reduce scope";
      }
    }

    cards.push({
      label: "Data Read",
      value: humanBytes(bytes),
      sub:
        m.written_bytes != null && m.written_bytes > 0
          ? `${humanBytes(m.written_bytes)} written`
          : undefined,
      status,
      context,
    });
  }

  // Files Read — factor in pruning ratio when available
  {
    const readFiles = m.read_files;
    const pruned = m.pruned_files;
    let status: MetricStatus = "neutral";
    let context = "";

    if (readFiles != null) {
      if (pruned != null && readFiles + pruned > 0) {
        const total = readFiles + pruned;
        const prunePct = Math.round((pruned / total) * 100);
        if (pruned / total > 0.7 || readFiles < 100) {
          status = "good";
          context = `Good file pruning — ${prunePct}% of files skipped`;
        } else if (pruned / total > 0.3) {
          status = "warning";
          context = `Moderate file pruning — only ${prunePct}% skipped`;
        } else {
          status = "bad";
          context = `Poor file pruning — only ${prunePct}% skipped`;
        }
      } else if (readFiles < 100) {
        status = "good";
        context = "Low file count";
      } else if (readFiles < 10_000) {
        status = "warning";
        context = "Moderate file count — consider compaction";
      } else {
        status = "bad";
        context = "High file count — consider compaction or clustering";
      }
    }

    cards.push({
      label: "Files Read",
      value: formatNumber(readFiles),
      sub: pruned != null ? `${formatNumber(pruned)} pruned` : undefined,
      status,
      context,
    });
  }

  // Partitions Read
  {
    const parts = m.read_partitions;
    let status: MetricStatus = "neutral";
    let context = "";

    if (parts != null) {
      if (parts < 100) {
        status = "good";
        context = "Low partition count";
      } else if (parts < 1_000) {
        status = "warning";
        context = "Moderate partition count — check partition filters";
      } else {
        status = "bad";
        context = "High partition count — add partition predicates";
      }
    }

    cards.push({
      label: "Partitions Read",
      value: formatNumber(parts),
      status,
      context,
    });
  }

  // Spill to Disk — any spill is a concern
  {
    const spill = m.spilled_local_bytes;
    let status: MetricStatus = "neutral";
    let context = "";

    if (spill != null) {
      if (spill === 0) {
        status = "good";
        context = "No disk spill";
      } else if (spill < GB) {
        status = "warning";
        context = "Minor spill — may indicate memory pressure";
      } else {
        status = "bad";
        context = "Significant spill — increase cluster size or optimize query";
      }
    }

    cards.push({
      label: "Spill to Disk",
      value: humanBytes(spill),
      status,
      context,
    });
  }

  // IO Cache Hit — higher is better
  {
    const pct = m.read_io_cache_percent;
    let status: MetricStatus = "neutral";
    let context = "";

    if (pct != null) {
      if (pct >= 70) {
        status = "good";
        context = "Strong cache utilization";
      } else if (pct >= 30) {
        status = "warning";
        context = "Moderate cache hit rate";
      } else {
        status = "bad";
        context = "Low cache hit — data may not be co-located";
      }
    }

    cards.push({
      label: "IO Cache Hit",
      value: pct != null ? `${pct}%` : "N/A",
      status,
      context,
    });
  }

  // Shuffle Read — less is better
  {
    const shuffleBytes = m.shuffle_read_bytes;
    let status: MetricStatus = "neutral";
    let context = "";

    if (shuffleBytes != null) {
      if (shuffleBytes === 0) {
        status = "good";
        context = "No shuffle overhead";
      } else if (shuffleBytes < GB) {
        status = "warning";
        context = "Some shuffle — review joins and aggregations";
      } else {
        status = "bad";
        context = "High shuffle — optimize joins or data distribution";
      }
    }

    cards.push({
      label: "Shuffle Read",
      value: humanBytes(shuffleBytes),
      status,
      context,
    });
  }

  // Result Cache
  {
    const cached = m.from_result_cache;
    cards.push({
      label: "Result Cache",
      value: cached ? "Yes" : "No",
      status: cached === true ? "good" : "neutral",
      context: cached === true ? "Served from result cache" : "Full query execution required",
    });
  }

  return cards;
}

export default function MetricsCards({ metrics }: Props) {
  const cards = buildCards(metrics);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-base font-semibold mb-3">Execution Metrics</h2>
      <div className="grid grid-cols-[repeat(auto-fill,minmax(170px,1fr))] gap-3">
        {cards.map((c) => {
          const s = STATUS_STYLES[c.status];
          return (
            <div
              key={c.label}
              className={`border border-gray-200 rounded-lg p-3.5 ${s.card}`}
            >
              <div className="text-[0.7rem] uppercase tracking-wide text-gray-400 mb-0.5 font-medium">
                {c.label}
              </div>
              <div className="text-xl font-semibold text-gray-900">
                {c.value}
              </div>
              {c.sub && (
                <div className="text-[0.72rem] text-gray-400 mt-0.5">
                  {c.sub}
                </div>
              )}
              {c.context && (
                <div className={`text-[0.65rem] mt-1.5 font-medium ${s.context}`}>
                  {c.context}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
