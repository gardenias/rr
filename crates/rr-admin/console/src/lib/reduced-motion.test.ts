import { existsSync, readFileSync } from "node:fs";

describe("reduced motion", () => {
  it("disables non-essential animation and smooth scrolling", () => {
    expect(existsSync("src/app/globals.css")).toBe(true);
    const css = readFileSync("src/app/globals.css", "utf8");
    expect(css).toContain("prefers-reduced-motion: reduce");
    expect(css).toMatch(/animation-duration:\s*0\.01ms\s*!important/);
    expect(css).toMatch(/scroll-behavior:\s*auto\s*!important/);
  });
});
