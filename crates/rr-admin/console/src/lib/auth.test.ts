import { loginRedirectFor, safeReturnPath } from "@/lib/auth";

describe("login return paths", () => {
  it("allows internal console destinations with shareable filters", () => {
    expect(safeReturnPath("/groups/?window=1h&group_id=3")).toBe(
      "/groups/?window=1h&group_id=3",
    );
  });

  it("rejects external, protocol-relative, and credential-bearing destinations", () => {
    expect(safeReturnPath("https://example.test")).toBe("/connections/");
    expect(safeReturnPath("//example.test")).toBe("/connections/");
    expect(safeReturnPath("/overview/?token=secret")).toBe("/connections/");
    expect(safeReturnPath("/groups/?connection=cluster-a&password=secret")).toBe("/connections/");
    expect(safeReturnPath("/login/")).toBe("/connections/");
  });

  it("rejects backslash, encoded separator, control-character, and unknown-route destinations", () => {
    expect(safeReturnPath("/\\evil.example")).toBe("/connections/");
    expect(safeReturnPath("/%5Cevil.example")).toBe("/connections/");
    expect(safeReturnPath("/%255Cevil.example")).toBe("/connections/");
    expect(safeReturnPath("/%0Aevil.example")).toBe("/connections/");
    expect(safeReturnPath("/not-a-console-route/")).toBe("/connections/");
  });

  it("allows static connection management routes and defaults to their index", () => {
    expect(safeReturnPath("/connections/")).toBe("/connections/");
    expect(safeReturnPath("/connections/new/")).toBe("/connections/new/");
    expect(safeReturnPath("/connections/?edit=cluster-a")).toBe("/connections/?edit=cluster-a");
    expect(safeReturnPath("/connections/evil/")).toBe("/connections/");
    expect(safeReturnPath(null)).toBe("/connections/");
  });

  it("builds an encoded login redirect that round-trips connection context", () => {
    expect(loginRedirectFor("/groups/?window=1h&group_id=3&connection=cluster-a")).toBe(
      "/login/?next=%2Fgroups%2F%3Fwindow%3D1h%26group_id%3D3%26connection%3Dcluster-a",
    );
    const reserved = "/groups/?window=1h&connection=cluster%2Fa%2Bb";
    expect(safeReturnPath(reserved)).toBe(reserved);
    expect(safeReturnPath(safeReturnPath(reserved))).toBe(reserved);
  });
});
