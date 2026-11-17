import {
  clampPageSize,
  parseQueryState,
  serializeQueryState,
} from "@/lib/query-state";

describe("shareable query state", () => {
  it("retains only documented windows and filter keys", () => {
    const state = parseQueryState(
      "?window=1h&node_id=7&group_id=3&token=do-not-share&unknown=value",
    );
    expect(state).toEqual({
      window: "1h",
      filters: { node_id: "7", group_id: "3" },
    });
    expect(serializeQueryState(state)).toBe("window=1h&node_id=7&group_id=3");
    expect(serializeQueryState(state)).not.toContain("token");
  });

  it("uses the real-time default and bounded page sizes", () => {
    expect(parseQueryState("?window=forever").window).toBe("15m");
    expect(clampPageSize(undefined)).toBe(50);
    expect(clampPageSize(0)).toBe(50);
    expect(clampPageSize(500)).toBe(200);
    expect(clampPageSize(25)).toBe(25);
  });
});
