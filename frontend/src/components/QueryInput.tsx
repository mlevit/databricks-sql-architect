import { useEffect, useState } from "react";

interface Props {
  onSubmit: (statementId: string) => void;
  loading: boolean;
  initialValue?: string;
  variant?: "landing" | "compact";
}

export default function QueryInput({
  onSubmit,
  loading,
  initialValue = "",
  variant = "compact",
}: Props) {
  const [value, setValue] = useState(initialValue);

  useEffect(() => {
    if (initialValue) setValue(initialValue);
  }, [initialValue]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = value.trim();
    if (trimmed) onSubmit(trimmed);
  };

  if (variant === "landing") {
    return (
      <form className="flex flex-col gap-4 w-full" onSubmit={handleSubmit}>
        <input
          id="statement-id"
          type="text"
          placeholder="Enter a statement ID to analyze..."
          value={value}
          onChange={(e) => setValue(e.target.value)}
          disabled={loading}
          className="w-full bg-white/[0.04] border border-white/[0.08] text-slate-100 text-base rounded-xl px-5 py-4 outline-none transition-all disabled:opacity-50 disabled:cursor-not-allowed placeholder:text-slate-500 focus:border-blue-500/40 focus:bg-white/[0.06]"
          style={{ boxShadow: "none" }}
          onFocus={(e) => { e.target.style.boxShadow = "0 0 20px rgba(59,130,246,0.2), 0 0 60px rgba(59,130,246,0.05)"; }}
          onBlur={(e) => { e.target.style.boxShadow = "none"; }}
        />
        <button
          type="submit"
          disabled={loading || !value.trim()}
          className="w-full text-white font-medium rounded-xl text-base px-5 py-3.5 transition-all disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer bg-gradient-to-r from-blue-600 to-violet-600 hover:from-blue-500 hover:to-violet-500 hover:shadow-[0_0_30px_rgba(59,130,246,0.3)]"
        >
          {loading ? "Analyzing..." : "Analyze"}
        </button>
      </form>
    );
  }

  return (
    <form className="flex items-center gap-2 w-full max-w-md" onSubmit={handleSubmit}>
      <input
        id="statement-id"
        type="text"
        placeholder="Statement ID..."
        value={value}
        onChange={(e) => setValue(e.target.value)}
        disabled={loading}
        className="flex-1 min-w-0 bg-white/[0.04] border border-white/[0.08] text-slate-100 text-sm rounded-full px-4 py-1.5 outline-none transition-all disabled:opacity-50 disabled:cursor-not-allowed placeholder:text-slate-500 font-mono focus:border-blue-500/40 focus:bg-white/[0.06]"
      />
      <button
        type="submit"
        disabled={loading || !value.trim()}
        className="text-white font-medium rounded-full text-sm px-5 py-1.5 transition-all disabled:opacity-30 disabled:cursor-not-allowed whitespace-nowrap cursor-pointer bg-gradient-to-r from-blue-600 to-violet-600 hover:from-blue-500 hover:to-violet-500"
      >
        {loading ? "Analyzing..." : "Analyze"}
      </button>
    </form>
  );
}
