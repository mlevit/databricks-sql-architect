import QueryInput from "./QueryInput";

interface Props {
  onSubmit: (statementId: string) => void;
  loading: boolean;
  initialValue: string;
}

export default function LandingPage({ onSubmit, loading, initialValue }: Props) {
  return (
    <div className="min-h-screen flex flex-col items-center justify-center px-6 relative overflow-hidden">
      {/* Animated gradient mesh orbs */}
      <div className="absolute inset-0 pointer-events-none">
        <div
          className="absolute w-[600px] h-[600px] rounded-full blur-[120px] opacity-60"
          style={{
            background: "radial-gradient(circle, rgba(59,130,246,0.3) 0%, transparent 70%)",
            top: "10%",
            left: "15%",
            animation: "float-orb 12s ease-in-out infinite",
          }}
        />
        <div
          className="absolute w-[500px] h-[500px] rounded-full blur-[100px] opacity-50"
          style={{
            background: "radial-gradient(circle, rgba(139,92,246,0.3) 0%, transparent 70%)",
            top: "30%",
            right: "10%",
            animation: "float-orb 15s ease-in-out infinite 2s",
          }}
        />
        <div
          className="absolute w-[400px] h-[400px] rounded-full blur-[80px] opacity-40"
          style={{
            background: "radial-gradient(circle, rgba(34,211,238,0.25) 0%, transparent 70%)",
            bottom: "15%",
            left: "30%",
            animation: "float-orb 18s ease-in-out infinite 4s",
          }}
        />
      </div>

      {/* Content */}
      <div className="flex flex-col items-center gap-10 w-full max-w-lg relative z-10 -mt-16">
        <div className="flex flex-col items-center gap-3">
          <h1 className="text-6xl font-bold tracking-tight gradient-text">
            SQL Architect
          </h1>
          <p className="text-slate-400 text-base tracking-wide">
            Deep analysis for Databricks SQL
          </p>
        </div>

        {/* Glass input card */}
        <div className="w-full glass-card p-6">
          <QueryInput
            onSubmit={onSubmit}
            loading={loading}
            initialValue={initialValue}
            variant="landing"
          />
        </div>
      </div>
    </div>
  );
}
