import { nextReconnectDelay, refreshDelay } from "@/lib/refresh-policy";

describe("refresh and reconnect policy", () => {
  it("pauses completely and slows background refreshes", () => {
    expect(refreshDelay(5_000, false, true)).toBeNull();
    expect(refreshDelay(5_000, true, false)).toBe(30_000);
    expect(refreshDelay(5_000, false, false)).toBe(5_000);
  });

  it("uses bounded exponential SSE reconnect delays", () => {
    expect(nextReconnectDelay(0)).toBe(1_000);
    expect(nextReconnectDelay(1)).toBe(2_000);
    expect(nextReconnectDelay(20)).toBe(15_000);
  });
});
