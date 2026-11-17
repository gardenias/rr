import { loginRedirectFor, safeReturnPath } from "@/lib/auth";

describe("login return paths", () => {
  it("allows internal console destinations with shareable filters", () => {
    expect(safeReturnPath("/groups/?window=1h&group_id=3")).toBe(
      "/groups/?window=1h&group_id=3",
    );
  });

  it("rejects external, protocol-relative, and credential-bearing destinations", () => {
    expect(safeReturnPath("https://example.test")).toBe("/overview/");
    expect(safeReturnPath("//example.test")).toBe("/overview/");
    expect(safeReturnPath("/overview/?token=secret")).toBe("/overview/");
    expect(safeReturnPath("/login/")).toBe("/overview/");
  });

  it("rejects backslash, encoded separator, control-character, and unknown-route destinations", () => {
    expect(safeReturnPath("/\\evil.example")).toBe("/overview/");
    expect(safeReturnPath("/%5Cevil.example")).toBe("/overview/");
    expect(safeReturnPath("/%255Cevil.example")).toBe("/overview/");
    expect(safeReturnPath("/%0Aevil.example")).toBe("/overview/");
    expect(safeReturnPath("/not-a-console-route/")).toBe("/overview/");
  });

  it("builds an encoded login redirect for every cluster route", () => {
    expect(loginRedirectFor("/groups/?window=1h&group_id=3")).toBe(
      "/login/?next=%2Fgroups%2F%3Fwindow%3D1h%26group_id%3D3",
    );
  });
});
