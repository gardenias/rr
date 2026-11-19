import { hrefWithConnection, switchConnectionHref } from "@/lib/query-state";

describe("connection URL builders", () => {
  it("round-trips connection with ordinary navigation state", () => {
    expect(hrefWithConnection(
      "/groups/",
      "?window=1h&group_id=3&connection=cluster%20a&cursor=do-not-copy",
    )).toBe("/groups/?window=1h&group_id=3&connection=cluster+a");
  });

  it("keeps only window when switching connections", () => {
    expect(switchConnectionHref(
      "/nodes/",
      "?window=6h&node_id=7&group_id=3&filter=node&op=read&source=admin&view=raft&severity=ERROR&state=ACTIVE&cursor=old&tab=details&connection=cluster-a",
      "cluster-b",
    )).toBe("/nodes/?window=6h&connection=cluster-b");
  });

  it("uses the default window when switching from an invalid window", () => {
    expect(switchConnectionHref("/overview/", "?window=forever&view=old", "cluster-b"))
      .toBe("/overview/?window=15m&connection=cluster-b");
  });
});
