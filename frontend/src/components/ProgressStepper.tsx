import type { StepProgress } from "../api";

const ALL_STEPS = [
  "Fetching query history",
  "Parsing SQL structure",
  "Analyzing execution metrics",
  "Analyzing tables",
  "Analyzing execution plan",
  "Analyzing warehouse config",
  "Generating recommendations",
];

interface Props {
  current: StepProgress | null;
}

const ICON_STYLES: Record<string, string> = {
  pending: "bg-gray-100 text-gray-400 border-gray-300",
  running: "bg-blue-50 text-blue-600 border-blue-600",
  done: "bg-green-500 text-white border-green-500",
};

const LABEL_STYLES: Record<string, string> = {
  pending: "text-gray-400",
  running: "text-gray-900 font-semibold",
  done: "text-green-700",
};

export default function ProgressStepper({ current }: Props) {
  const isBusy = current !== null;

  return (
    <div
      className="bg-white rounded-lg border border-gray-200 p-6"
      aria-live="polite"
      aria-busy={isBusy}
    >
      <div className="flex flex-col relative">
        {ALL_STEPS.map((label, idx) => {
          let state: "pending" | "running" | "done" = "pending";
          if (current) {
            if (idx < current.step) {
              state = "done";
            } else if (idx === current.step) {
              state = current.status;
            }
          }

          return (
            <div key={idx} className="flex items-center gap-3 py-2 relative">
              {/* Vertical connector line */}
              <div
                className="absolute left-[13px] top-0 bottom-0 w-0.5 bg-gray-200"
                style={{
                  top: idx === 0 ? "50%" : 0,
                  bottom: idx === ALL_STEPS.length - 1 ? "50%" : 0,
                }}
              />

              <div
                className={`w-7 h-7 rounded-full flex items-center justify-center shrink-0 text-xs font-semibold relative z-10 border-[1.5px] transition-all duration-300 ${ICON_STYLES[state]}`}
              >
                {state === "done" && (
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">
                    <path
                      d="M2.5 7.5L5.5 10.5L11.5 3.5"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                )}
                {state === "running" && <span className="stepper-spinner" />}
                {state === "pending" && (
                  <span className="text-[0.68rem]">{idx + 1}</span>
                )}
              </div>
              <span className={`text-sm font-medium transition-colors duration-200 ${LABEL_STYLES[state]}`}>
                {label}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
