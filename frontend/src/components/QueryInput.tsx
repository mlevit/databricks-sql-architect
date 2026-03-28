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
    <form className="query-input" onSubmit={handleSubmit}>
      <div className="query-input__field">
        <label htmlFor="statement-id">Statement ID</label>
        <input
          id="statement-id"
          type="text"
          placeholder="e.g. 01efb3c7-d5a0-1234-abcd-0123456789ab"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          disabled={loading}
        />
      </div>
      <button type="submit" disabled={loading || !value.trim()}>
        {loading ? "Analyzing..." : "Analyze"}
      </button>
    </form>
  );
}
