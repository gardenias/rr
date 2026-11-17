import {
  formatBytes,
  formatDuration,
  formatLeader,
  formatNumber,
  formatTimestamp,
  sanitizeError,
} from "@/lib/format";

describe("evidence-safe formatting", () => {
  it("keeps absent values distinct from zero", () => {
    expect(formatNumber(undefined)).toBe("—");
    expect(formatNumber(null)).toBe("—");
    expect(formatNumber(Number.NaN)).toBe("—");
    expect(formatNumber(0)).toBe("0");
    expect(formatBytes(undefined)).toBe("—");
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(0.5)).toBe("0.5 B");
    expect(formatBytes(1536)).toBe("1.5 KiB");
  });

  it("formats durations and UTC timestamps without inventing invalid values", () => {
    expect(formatDuration(0)).toBe("0s");
    expect(formatDuration(3661)).toBe("1h 1m 1s");
    expect(formatDuration(undefined)).toBe("—");
    expect(formatTimestamp("not-a-time")).toBe("—");
    expect(formatTimestamp("2026-11-16T08:00:00Z")).toContain("2026-11-16");
    expect(formatTimestamp("2026-11-16T08:00:00Z")).toContain("UTC");
  });

  it("never presents the legacy zero leader sentinel as node zero", () => {
    expect(formatLeader(0)).toBe("No leader observed");
    expect(formatLeader(null)).toBe("No leader observed");
    expect(formatLeader(undefined)).toBe("No leader observed");
    expect(formatLeader(12)).toBe("Node 12");
  });

  it("removes credentials and bearer tokens from display errors", () => {
    const input =
      "request failed Bearer top-secret token=abc123 at https://viewer:password@example.test/path?token=url-secret";
    const output = sanitizeError(input);
    expect(output).not.toContain("top-secret");
    expect(output).not.toContain("abc123");
    expect(output).not.toContain("password");
    expect(output).not.toContain("url-secret");
    expect(output).toContain("[redacted]");
  });
});
