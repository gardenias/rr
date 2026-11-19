"use client";

import { ArrowLeft } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ConnectionForm } from "@/components/connections/connection-form";
import { useActiveConnection } from "@/lib/connection-context";
import type { ConnectionSummary } from "@/lib/connections";

export default function NewConnectionPage() {
  const router = useRouter();
  const { keychainAvailable, refresh } = useActiveConnection();

  const complete = (connection: ConnectionSummary, connected: boolean) => {
    if (connected) {
      router.push(`/overview/?connection=${encodeURIComponent(connection.id)}`);
    } else {
      router.push("/connections/");
    }
  };

  return (
    <main className="connection-page" id="main-content">
      <header className="connection-topbar">
        <Link href="/connections/" className="connection-brand" aria-label="Rr Console connections">
          <svg viewBox="0 0 32 32" width="26" height="26" aria-hidden="true">
            <circle cx="16" cy="16" r="11" fill="none" stroke="currentColor" strokeWidth="1.5" />
            <circle cx="16" cy="5" r="3" fill="var(--acc)" />
            <circle cx="6.5" cy="21.5" r="3" fill="var(--bg-0)" stroke="currentColor" strokeWidth="1.5" />
            <circle cx="25.5" cy="21.5" r="3" fill="var(--bg-0)" stroke="currentColor" strokeWidth="1.5" />
          </svg>
          <strong>Rr</strong>{" "}<span>Console</span>
        </Link>
      </header>
      <div className="connection-workspace connection-workspace-form">
        <Link href="/connections/" className="back-to-connections"><ArrowLeft aria-hidden="true" size={14} />Back to connections</Link>
        <div className="connection-page-heading">
          <div>
            <p className="page-eyebrow">New profile</p>
            <h1>New connection</h1>
            <p>Define a cluster source and its security settings. Test does not save the candidate.</p>
          </div>
        </div>
        <ConnectionForm keychainAvailable={keychainAvailable} onSaved={() => { void refresh(); }} onComplete={complete} />
      </div>
    </main>
  );
}
