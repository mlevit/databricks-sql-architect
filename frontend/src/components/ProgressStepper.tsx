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

export default function ProgressStepper({ current }: Props) {
  return (
    <div className="stepper">
      <div className="stepper__track">
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
            <div key={idx} className={`stepper__step stepper__step--${state}`}>
              <div className="stepper__icon">
                {state === "done" && (
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                    <path
                      d="M2.5 7.5L5.5 10.5L11.5 3.5"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                )}
                {state === "running" && <span className="stepper__spinner" />}
                {state === "pending" && (
                  <span className="stepper__number">{idx + 1}</span>
                )}
              </div>
              <span className="stepper__label">{label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
