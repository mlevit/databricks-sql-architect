import type { Recommendation } from "../types";

interface Props {
  recommendations: Recommendation[];
}

const SEVERITY_ICONS: Record<string, string> = {
  critical: "\u26D4",
  warning: "\u26A0\uFE0F",
  info: "\u2139\uFE0F",
};

export default function Recommendations({ recommendations }: Props) {
  if (recommendations.length === 0) {
    return (
      <div className="panel recommendations">
        <h2>Recommendations</h2>
        <p className="recommendations__empty">No issues detected. Your query looks good!</p>
      </div>
    );
  }

  return (
    <div className="panel recommendations">
      <h2>
        Recommendations{" "}
        <span className="recommendations__count">{recommendations.length}</span>
      </h2>
      <div className="recommendations__list">
        {recommendations.map((r, i) => (
          <div key={i} className={`rec rec--${r.severity}`}>
            <div className="rec__header">
              <span className="rec__icon">{SEVERITY_ICONS[r.severity] || ""}</span>
              <span className="rec__title">{r.title}</span>
              <span className={`badge badge--${r.severity}`}>{r.category}</span>
            </div>
            <p className="rec__desc">{r.description}</p>
            {r.action && (
              <div className="rec__action">
                <strong>Suggested action:</strong>{" "}
                <code>{r.action}</code>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
