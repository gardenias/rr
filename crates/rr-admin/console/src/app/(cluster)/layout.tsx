import { Suspense, type ReactNode } from "react";
import { ConsoleShell } from "@/components/console/console-shell";

export default function ClusterLayout({ children }: { children: ReactNode }) {
  return <Suspense fallback={<main className="login-page">Connecting to the Console data plane…</main>}><ConsoleShell>{children}</ConsoleShell></Suspense>;
}
