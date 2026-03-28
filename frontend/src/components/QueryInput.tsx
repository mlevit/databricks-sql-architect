import { useEffect, useState } from "react";

interface Props {
  onSubmit: (statementId: string) => void;
  loading: boolean;
  initialValue?: string;
}

export default function QueryInput({ onSubmit, loading, initialValue = "" }: Props) {
  const [value, setValue] = useState(initialValue);

  useEffect(() => {
    if (initialValue) setValue(initialValue);
  }, [initialValue]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = value.trim();
    if (trimmed) onSubmit(trimmed);
  };

  return (
    <form className="flex items-end gap-2 w-full max-w-md" onSubmit={handleSubmit}>
      <div className="flex flex-col gap-0.5 flex-1 min-w-0">
        <label
          htmlFor="statement-id"
          className="text-[0.7rem] uppercase tracking-wide text-gray-400 font-medium"
        >
          Statement ID
        </label>
        <input
          id="statement-id"
          type="text"
          placeholder="Enter statement ID to analyze..."
          value={value}
          onChange={(e) => setValue(e.target.value)}
          disabled={loading}
          className="bg-white border border-gray-300 text-gray-900 text-sm font-mono rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-colors disabled:bg-gray-100 disabled:cursor-not-allowed placeholder:text-gray-400"
        />
      </div>
      <button
        type="submit"
        disabled={loading || !value.trim()}
        className="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2 transition-colors disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
      >
        {loading ? "Analyzing..." : "Analyze"}
      </button>
    </form>
  );
}
