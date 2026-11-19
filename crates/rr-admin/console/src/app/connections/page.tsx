"use client";

import { Plus, Settings2 } from "lucide-react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useRef } from "react";
import { ConnectionList } from "@/components/connections/connection-list";
import { ConnectionForm } from "@/components/connections/connection-form";
import { Button } from "@/components/ui/button";
import { Dialog } from "@/components/ui/dialog";
import { useActiveConnection } from "@/lib/connection-context";

export default function ConnectionsPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { connections, keychainAvailable, refresh } = useActiveConnection();
  const restoreFocusId = useRef<string | null>(null);
  const editId = searchParams.get("edit");
  const editing = connections.find((connection) => connection.id === editId) ?? null;

  const closeEditor = () => {
    restoreFocusId.current = editing?.id ?? null;
    router.replace("/connections/");
  };

  useEffect(() => {
    if (editId || !restoreFocusId.current) return;
    const id = restoreFocusId.current;
    restoreFocusId.current = null;
    const edit = document.getElementById(`edit-connection-${id}`) as HTMLButtonElement | null;
    if (edit?.disabled) {
      document.getElementById(`disconnect-connection-${id}`)?.focus();
    } else {
      edit?.focus();
    }
  }, [editId, connections]);

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
        <span className="connection-topbar-label"><Settings2 aria-hidden="true" size={13} />Connection manager</span>
      </header>
      <div className="connection-workspace">
        <div className="connection-page-heading">
          <div>
            <p className="page-eyebrow">Cluster access</p>
            <h1>Connections</h1>
            <p>Manage saved cluster endpoints and explicitly start or stop their observation runtimes.</p>
          </div>
          <Link id="new-connection" className="button button-default" href="/connections/new/"><Plus aria-hidden="true" size={15} />New connection</Link>
        </div>
        <ConnectionList onEdit={(connection) => router.push(`/connections/?edit=${encodeURIComponent(connection.id)}`)} />
      </div>

      <Dialog
        open={editing !== null}
        onOpenChange={(open) => { if (!open) closeEditor(); }}
        title={editing ? `Edit ${editing.name}` : "Edit connection"}
        description="Update the saved profile. Runtime configuration changes apply on the next connection."
        className="connection-dialog"
      >
        {editing?.status === "DISCONNECTED" ? (
          <ConnectionForm
            key={editing.id}
            connection={editing}
            keychainAvailable={keychainAvailable}
            onSaved={() => { void refresh(); }}
            onComplete={() => { void refresh(); closeEditor(); }}
          />
        ) : editing ? (
          <div className="connected-edit-lock">
            <p>Disconnect this connection before editing its profile.</p>
            <Button variant="secondary" onClick={closeEditor}>Return to connections</Button>
          </div>
        ) : null}
      </Dialog>
    </main>
  );
}
