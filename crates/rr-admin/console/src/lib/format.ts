const numberFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
});

export function formatNumber(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : numberFormatter.format(value);
}

export function formatBytes(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  const unitIndex = Math.max(0, Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1));
  const scaled = value / 1024 ** unitIndex;
  return `${numberFormatter.format(scaled)} ${units[unitIndex]}`;
}

export function formatDuration(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value < 0) return "—";
  const totalSeconds = Math.floor(value);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return [hours ? `${hours}h` : "", minutes ? `${minutes}m` : "", seconds || totalSeconds === 0 ? `${seconds}s` : ""]
    .filter(Boolean)
    .join(" ");
}

export function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : `${date.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, "")} UTC`;
}

export function formatLeader(nodeId: number | null | undefined): string {
  return nodeId == null || nodeId === 0 ? "No leader observed" : `Node ${nodeId}`;
}

export function formatAge(ageMs: number | null | undefined): string {
  if (ageMs == null || !Number.isFinite(ageMs) || ageMs < 0) return "age unknown";
  if (ageMs < 1_000) return "<1s old";
  if (ageMs < 3_600_000) return `${Math.floor(ageMs / 1_000)}s old`;
  return `${formatDuration(Math.floor(ageMs / 1_000))} old`;
}

export function sanitizeError(message: string): string {
  return message
    .replace(/\bBearer\s+[^\s,;]+/gi, "Bearer [redacted]")
    .replace(/(https?:\/\/)[^\s/@:]+:[^\s/@]+@/gi, "$1[redacted]@")
    .replace(/\b(token|password|secret|api[_-]?key|authorization)=([^\s&]+)/gi, "$1=[redacted]");
}
