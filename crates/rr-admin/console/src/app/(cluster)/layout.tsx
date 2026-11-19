import { Suspense, type ReactNode } from "react";
import { ConnectionGate } from "@/components/connections/connection-gate";
import { ConsoleShell } from "@/components/console/console-shell";
import { ConnectionProvider } from "@/lib/connection-context";

export default function ClusterLayout({ children }: { children: ReactNode }) {
  return (
    <Suspense fallback={<main className="login-page">Validating cluster connection…</main>}>
      <ConnectionProvider>
        <ConnectionGate><ConsoleShell>{children}</ConsoleShell></ConnectionGate>
      </ConnectionProvider>
    </Suspense>
  );
}
