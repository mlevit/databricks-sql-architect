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
  pct?: number;
}

const GB = 1_073_741_824;

const STATUS_CONFIG: Record<MetricStatus, { gradient: string; text: string; context: string; glow: string }> = {
  good: {
    gradient: "from-cyan-500 to-teal-500",
    text: "text-cyan-400",
    context: "text-cyan-500/80",
    glow: "hover:shadow-[0_0_20px_rgba(34,211,238,0.1)]",
  },
  warning: {
    gradient: "from-amber-500 to-orange-500",
    text: "text-amber-400",
    context: "text-amber-500/80",
    glow: "hover:shadow-[0_0_20px_rgba(245,158,11,0.1)]",
  },
  bad: {
    gradient: "from-rose-500 to-red-500",
    text: "text-rose-400",
    context: "text-rose-500/80",
    glow: "hover:shadow-[0_0_20px_rgba(244,63,94,0.1)]",
  },
  neutral: {
    gradient: "from-slate-600 to-slate-700",
    text: "text-slate-300",
    context: "text-slate-500",
    glow: "",
  },
};

function RingGauge({ pct, color }: { pct: number; color: string }) {
  const r = 14;
  const circ = 2 * Math.PI * r;
  const offset = circ - (pct / 100) * circ;
  return (
    <svg width="36" height="36" viewBox="0 0 36 36" className="shrink-0">
      <circle cx="18" cy="18" r={r} fill="none" stroke="rgba(255,255,255,0.05)" strokeWidth="3" />
      <circle cx="18" cy="18" r={r} fill="none" stroke={color} strokeWidth="3" strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round" transform="rotate(-90 18 18)" className="transition-all duration-700" />
      <text x="18" y="18" textAnchor="middle" dominantBaseline="central" className="text-[8px] fill-current font-semibold" style={{ fill: color }}>{pct}%</text>
    </svg>
  );
}

function buildCards(m: QueryMetrics): MetricCard[] {
  const cards: MetricCard[] = [];

  {
    const read = m.read_rows;
    const produced = m.produced_rows;
    let status: MetricStatus = "neutral";
    let context = "";
    if (read != null && read > 0 && produced != null) {
      if (produced === 0) { status = "warning"; context = "No rows produced"; }
      else {
        const ratio = produced / read;
        const mult = Math.round(read / produced).toLocaleString();
        if (ratio >= 0.1) { status = "good"; context = "Good selectivity"; }
        else if (ratio >= 0.01) { status = "warning"; context = `${mult}× amplification`; }
        else { status = "bad"; context = `${mult}× amplification`; }
      }
    } else if (read != null && read > 0) {
      if (read < 10_000_000) { status = "good"; context = "Moderate rows"; }
      else if (read < 1_000_000_000) { status = "warning"; context = "High row count"; }
      else { status = "bad"; context = "Very high rows"; }
    } else if (read === 0) { status = "good"; context = "No rows scanned"; }
    cards.push({ label: "Rows Read", value: formatNumber(read), sub: produced != null ? `${formatNumber(produced)} produced` : undefined, status, context });
  }

  {
    const bytes = m.read_bytes;
    let status: MetricStatus = "neutral"; let context = "";
    if (bytes != null) {
      if (bytes === 0) { status = "good"; context = "No data scanned"; }
      else if (bytes < GB) { status = "good"; context = "Small scan"; }
      else if (bytes < 100 * GB) { status = "warning"; context = "Moderate volume"; }
      else { status = "bad"; context = "Large scan"; }
    }
    cards.push({ label: "Data Read", value: humanBytes(bytes), sub: m.written_bytes != null && m.written_bytes > 0 ? `${humanBytes(m.written_bytes)} written` : undefined, status, context });
  }

  {
    const readFiles = m.read_files; const pruned = m.pruned_files;
    let status: MetricStatus = "neutral"; let context = "";
    if (readFiles != null) {
      if (pruned != null && readFiles + pruned > 0) {
        const total = readFiles + pruned; const prunePct = Math.round((pruned / total) * 100);
        if (pruned / total > 0.7 || readFiles < 100) { status = "good"; context = `${prunePct}% pruned`; }
        else if (pruned / total > 0.3) { status = "warning"; context = `${prunePct}% pruned`; }
        else { status = "bad"; context = `Only ${prunePct}% pruned`; }
      } else if (readFiles < 100) { status = "good"; context = "Low files"; }
      else if (readFiles < 10_000) { status = "warning"; context = "Consider compaction"; }
      else { status = "bad"; context = "Compaction needed"; }
    }
    cards.push({ label: "Files Read", value: formatNumber(readFiles), sub: pruned != null ? `${formatNumber(pruned)} pruned` : undefined, status, context });
  }

  {
    const parts = m.read_partitions; let status: MetricStatus = "neutral"; let context = "";
    if (parts != null) { if (parts < 100) { status = "good"; context = "Low partitions"; } else if (parts < 1_000) { status = "warning"; context = "Check filters"; } else { status = "bad"; context = "Add predicates"; } }
    cards.push({ label: "Partitions", value: formatNumber(parts), status, context });
  }

  {
    const spill = m.spilled_local_bytes; let status: MetricStatus = "neutral"; let context = "";
    if (spill != null) { if (spill === 0) { status = "good"; context = "No spill"; } else if (spill < GB) { status = "warning"; context = "Minor spill"; } else { status = "bad"; context = "Heavy spill"; } }
    cards.push({ label: "Disk Spill", value: humanBytes(spill), status, context });
  }

  {
    const pct = m.read_io_cache_percent; let status: MetricStatus = "neutral"; let context = "";
    if (pct != null) { if (pct >= 70) { status = "good"; context = "Strong cache"; } else if (pct >= 30) { status = "warning"; context = "Moderate cache"; } else { status = "bad"; context = "Low cache"; } }
    cards.push({ label: "IO Cache", value: pct != null ? `${pct}%` : "N/A", status, context, pct: pct ?? undefined });
  }

  {
    const shuffleBytes = m.shuffle_read_bytes; let status: MetricStatus = "neutral"; let context = "";
    if (shuffleBytes != null) { if (shuffleBytes === 0) { status = "good"; context = "No shuffle"; } else if (shuffleBytes < GB) { status = "warning"; context = "Some shuffle"; } else { status = "bad"; context = "High shuffle"; } }
    cards.push({ label: "Shuffle", value: humanBytes(shuffleBytes), status, context });
  }

  {
    const cached = m.from_result_cache;
    cards.push({ label: "Result Cache", value: cached ? "Yes" : "No", status: cached === true ? "good" : "neutral", context: cached === true ? "From cache" : "Full execution" });
  }

  return cards;
}

export default function MetricsCards({ metrics }: Props) {
  const cards = buildCards(metrics);

  return (
    <div className="glass-card p-6">
      <h2 className="text-[0.65rem] font-semibold uppercase tracking-wider text-slate-500 mb-4">Execution Metrics</h2>
      <div className="grid grid-cols-[repeat(auto-fill,minmax(170px,1fr))] gap-3">
        {cards.map((c) => {
          const cfg = STATUS_CONFIG[c.status];
          const showRing = c.label === "IO Cache" && c.pct != null;
          return (
            <div
              key={c.label}
              className={`relative bg-white/[0.02] border border-white/[0.06] rounded-xl p-4 overflow-hidden transition-all duration-300 ${cfg.glow}`}
            >
              <div className={`absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r ${cfg.gradient}`} />
              <div className="text-[0.65rem] uppercase tracking-wider text-slate-500 mb-1 font-medium">{c.label}</div>
              <div className="flex items-center gap-2">
                <div className={`text-2xl font-light ${cfg.text}`}>{c.value}</div>
                {showRing && <RingGauge pct={c.pct!} color={cfg.text.includes("cyan") ? "#22d3ee" : cfg.text.includes("amber") ? "#f59e0b" : cfg.text.includes("rose") ? "#f43f5e" : "#94a3b8"} />}
              </div>
              {c.sub && <div className="text-[0.68rem] text-slate-500 mt-0.5">{c.sub}</div>}
              {c.context && <div className={`text-[0.6rem] mt-1.5 font-medium ${cfg.context}`}>{c.context}</div>}
            </div>
          );
        })}
      </div>
    </div>
  );
}
