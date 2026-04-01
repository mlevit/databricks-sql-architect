import type { StepProgress } from "../api";

const ALL_STEPS = [
  { label: "Query history", icon: "M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" },
  { label: "SQL parsing", icon: "M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" },
  { label: "Metrics", icon: "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6m6 0h6m-6 0V9a2 2 0 012-2h2a2 2 0 012 2v10m6 0v-4a2 2 0 00-2-2h-2a2 2 0 00-2 2v4" },
  { label: "Tables", icon: "M3 10h18M3 14h18M3 6h18M3 18h18" },
  { label: "Plan", icon: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" },
  { label: "Warehouse", icon: "M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2" },
  { label: "Recommendations", icon: "M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" },
];

interface Props {
  current: StepProgress | null;
}

export default function ProgressStepper({ current }: Props) {
  return (
    <div className="glass-card p-6" aria-live="polite" aria-busy={current !== null}>
      <div className="flex items-start justify-between gap-1 overflow-x-auto scrollbar-hide">
        {ALL_STEPS.map((step, idx) => {
          let state: "pending" | "running" | "done" = "pending";
          if (current) {
            if (idx < current.step) state = "done";
            else if (idx === current.step) state = current.status;
          }

          const isLast = idx === ALL_STEPS.length - 1;

          return (
            <div key={idx} className="flex items-start flex-1 min-w-0">
              <div className="flex flex-col items-center gap-1.5 min-w-[48px]">
                <div
                  className={`w-9 h-9 rounded-full flex items-center justify-center shrink-0 transition-all duration-500 ${
                    state === "done"
                      ? "bg-gradient-to-br from-blue-500 to-violet-500 text-white shadow-[0_0_15px_rgba(59,130,246,0.3)]"
                      : state === "running"
                        ? "bg-blue-500/20 text-blue-400 border border-blue-500/50"
                        : "bg-white/[0.04] text-slate-600 border border-white/[0.08]"
                  }`}
                  style={state === "running" ? { animation: "pulse-glow 2s ease-in-out infinite" } : undefined}
                >
                  {state === "done" ? (
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                      <path d="M3 8.5L6.5 12L13 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                    </svg>
                  ) : state === "running" ? (
                    <span className="stepper-spinner" />
                  ) : (
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                      <path d={step.icon} />
                    </svg>
                  )}
                </div>
                <span className={`text-[0.6rem] text-center leading-tight font-medium transition-colors ${
                  state === "done" ? "text-blue-400" : state === "running" ? "text-white" : "text-slate-600"
                }`}>
                  {step.label}
                </span>
              </div>
              {!isLast && (
                <div className="flex-1 h-px mt-[18px] mx-1 min-w-[12px]">
                  <div
                    className={`h-full rounded-full transition-all duration-700 ${
                      state === "done"
                        ? "bg-gradient-to-r from-blue-500 to-violet-500"
                        : "bg-white/[0.06]"
                    }`}
                  />
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
