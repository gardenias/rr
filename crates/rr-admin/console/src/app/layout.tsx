import "@fontsource-variable/inter";
import "@fontsource-variable/noto-sans-sc";
import "@fontsource-variable/jetbrains-mono";
import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";
import "./globals.css";

export const metadata: Metadata = {
  title: { default: "Rr Console", template: "%s · Rr Console" },
  description: "Read-only, evidence-first observation for Rr clusters.",
};

export const viewport: Viewport = {
  colorScheme: "dark",
  themeColor: "#0b0e13",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
