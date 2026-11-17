export function refreshDelay(intervalMs: number, hidden: boolean, paused: boolean): number | null {
  if (paused) return null;
  return hidden ? Math.max(intervalMs * 6, 30_000) : intervalMs;
}

export function nextReconnectDelay(attempt: number): number {
  return Math.min(1_000 * 2 ** Math.max(0, attempt), 15_000);
}
