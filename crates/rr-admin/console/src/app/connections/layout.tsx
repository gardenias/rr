import { Suspense, type ReactNode } from "react";
import { ConnectionProvider } from "@/lib/connection-context";

export default function ConnectionsLayout({ children }: { children: ReactNode }) {
  return (
    <Suspense fallback={<main className="connection-page"><p role="status">Loading connections…</p></main>}>
      <ConnectionProvider>{children}</ConnectionProvider>
    </Suspense>
  );
}
