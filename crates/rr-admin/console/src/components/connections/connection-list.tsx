"use client";

import { Cable, CircleAlert, Eye, KeyRound, Plug, PlugZap, Plus, Trash2 } from "lucide-react";
import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { ErrorState } from "@/components/console/states";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog } from "@/components/ui/dialog";
import { ConsoleApiError } from "@/lib/api";
import { useActiveConnection } from "@/lib/connection-context";
import { connectionsApi, type ConnectionStatus, type ConnectionSummary } from "@/lib/connections";
import { formatTimestamp, sanitizeError } from "@/lib/format";

interface OperationResult {
  kind: "success" | "error";
  message: string;
  correlationId?: string | null;
}

const statusLabels: Record<ConnectionStatus, string> = {
  CONNECTING: "Connecting",
  CONNECTED: "Connected",
  DEGRADED: "Degraded",
  UNREACHABLE: "Unreachable",
  DISCONNECTED: "Disconnected",
};

function statusTone(status: ConnectionStatus): "neutral" | "accent" | "ok" | "bad" {
  if (status === "CONNECTED") return "ok";
  if (status === "CONNECTING" || status === "DEGRADED") return "accent";
  if (status === "UNREACHABLE") return "bad";
  return "neutral";
}

function addressSummary(connection: ConnectionSummary): string {
  return [
    ...connection.admin_seeds.map((seed) => seed.admin_addr),
    connection.discovery,
  ].filter(Boolean).join(" · ") || "No cluster source";
}

export function ConnectionList({ onEdit }: { onEdit: (connection: ConnectionSummary) => void }) {
  const { connections, recentConnectionId, loading, error, refresh } = useActiveConnection();
  const [pending, setPending] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, OperationResult>>({});
  const [deleting, setDeleting] = useState<ConnectionSummary | null>(null);
  const restoreDeleteFocus = useRef<string | null>(null);
  const deletedId = useRef<string | null>(null);

  useEffect(() => {
    if (deleting || !restoreDeleteFocus.current) return;
    const id = restoreDeleteFocus.current;
    restoreDeleteFocus.current = null;
    const wasDeleted = deletedId.current === id;
    deletedId.current = null;
    if (wasDeleted) {
      document.getElementById("new-connection")?.focus();
    } else {
      document.getElementById(`delete-connection-${id}`)?.focus();
    }
  }, [deleting]);

  const run = async (connection: ConnectionSummary, action: "test" | "connect" | "disconnect" | "delete") => {
    const operation = `${connection.id}:${action}`;
    setPending(operation);
    setResults((current) => {
      const next = { ...current };
      delete next[connection.id];
      return next;
    });
    try {
      if (action === "test") {
        await connectionsApi.test(connection.id);
        setResults((current) => ({ ...current, [connection.id]: { kind: "success", message: "Connection test passed." } }));
      } else {
        if (action === "connect") await connectionsApi.connect(connection.id);
        if (action === "disconnect") await connectionsApi.disconnect(connection.id);
        if (action === "delete") await connectionsApi.remove(connection.id);
        await refresh();
        if (action === "delete") {
          deletedId.current = connection.id;
          setDeleting(null);
        }
      }
    } catch (cause) {
      if (action === "delete") await refresh();
      const apiError = cause instanceof ConsoleApiError ? cause : null;
      setResults((current) => ({
        ...current,
        [connection.id]: {
          kind: "error",
          message: sanitizeError(cause instanceof Error ? cause.message : `Could not ${action} this connection. Try again.`),
          correlationId: apiError?.correlationId,
        },
      }));
    } finally {
      setPending(null);
    }
  };

  if (loading && connections.length === 0) {
    return (
      <div className="connection-list-skeleton" role="status" aria-label="Loading connections">
        <span className="sr-only">Loading connections…</span>
        <div /><div /><div />
      </div>
    );
  }

  if (error && connections.length === 0) {
    return (
      <div className="connection-load-error">
        <ErrorState title="Connections unavailable" message={error.message} correlationId={error.correlationId} recovery="The saved profiles have not been changed." />
        <Button variant="secondary" onClick={() => void refresh()}>Retry loading connections</Button>
      </div>
    );
  }

  if (connections.length === 0) {
    return (
      <section className="connection-empty" aria-labelledby="empty-connections-title">
        <div className="empty-connection-mark" aria-hidden="true"><Cable size={26} /></div>
        <div>
          <h2 id="empty-connections-title">No connections yet</h2>
          <p>Save an admin seed or discovery endpoint to start observing a cluster. Credentials remain on the Console host.</p>
        </div>
        <Link className="button button-secondary" href="/connections/new/"><Plus aria-hidden="true" size={15} />Create first connection</Link>
      </section>
    );
  }

  return (
    <>
      {loading ? <p className="connection-refresh-status" role="status" aria-label="Refreshing connections">Refreshing connections…</p> : null}
      {error ? (
        <div className="connection-load-error">
          <ErrorState title="Could not refresh connections" message={error.message} correlationId={error.correlationId} recovery="The last loaded profiles remain visible." />
          <Button variant="secondary" onClick={() => void refresh()}>Retry loading connections</Button>
        </div>
      ) : null}
      <div className="connection-list" aria-label="Saved connections">
        {connections.map((connection) => {
          const result = results[connection.id];
          const isConnected = connection.status !== "DISCONNECTED";
          const canOpen = ["CONNECTED", "DEGRADED", "UNREACHABLE"].includes(connection.status);
          return (
            <article className="connection-item" aria-label={`${connection.name} connection`} key={connection.id}>
              <div className="connection-item-heading">
                <div className="connection-identity">
                  <div className="connection-title-line">
                    <h2>{connection.name}</h2>
                    {connection.environment ? <Badge>{connection.environment}</Badge> : null}
                    {recentConnectionId === connection.id ? <Badge tone="accent">Recent</Badge> : null}
                  </div>
                  <p className="connection-address">{addressSummary(connection)}</p>
                </div>
                <Badge tone={statusTone(connection.status)}>{statusLabels[connection.status]}</Badge>
              </div>

              <dl className="connection-facts">
                <div><dt>Runtime</dt><dd>{connection.node_count == null ? "—" : `${connection.node_count} nodes`} <span aria-hidden="true">/</span> {connection.group_count == null ? "—" : `${connection.group_count} groups`}</dd></div>
                <div><dt>Last connected</dt><dd>{formatTimestamp(connection.last_connected_at)}</dd></div>
                <div><dt>Credentials</dt><dd>{connection.credentials_configured ? connection.credential_source?.toLowerCase() ?? "configured" : "Not configured"}</dd></div>
              </dl>

              {connection.last_error ? (
                <div className="connection-inline-error"><CircleAlert aria-hidden="true" size={15} /><span>{sanitizeError(connection.last_error)}</span></div>
              ) : null}
              {result ? (
                <div className={result.kind === "error" ? "connection-inline-error" : "connection-inline-success"} role={result.kind === "error" ? "alert" : "status"}>
                  {result.kind === "error" ? <CircleAlert aria-hidden="true" size={15} /> : <PlugZap aria-hidden="true" size={15} />}
                  <span>{result.message}{result.correlationId ? ` Correlation ID: ${result.correlationId}` : ""}</span>
                </div>
              ) : null}

              {isConnected ? (
                <p className="connection-edit-lock" id={`edit-lock-${connection.id}`}>Disconnect this connection before editing.</p>
              ) : null}

              <div className="connection-item-actions">
                <div className="connection-primary-actions">
                  {canOpen ? (
                    <Link className="button button-default button-size-small" aria-label={`Open ${connection.name}`} href={`/overview/?connection=${encodeURIComponent(connection.id)}`}>
                      <Eye aria-hidden="true" size={13} />Open
                    </Link>
                  ) : null}
                  {isConnected ? (
                    <Button id={`disconnect-connection-${connection.id}`} aria-label={`Disconnect ${connection.name}`} size="small" variant="secondary" disabled={pending !== null} onClick={() => void run(connection, "disconnect")}>
                      <Plug aria-hidden="true" size={13} />{pending === `${connection.id}:disconnect` ? "Disconnecting…" : "Disconnect"}
                    </Button>
                  ) : (
                    <Button aria-label={`Connect ${connection.name}`} size="small" disabled={pending !== null || connection.status === "CONNECTING"} onClick={() => void run(connection, "connect")}>
                      <PlugZap aria-hidden="true" size={13} />{pending === `${connection.id}:connect` ? "Connecting…" : "Connect"}
                    </Button>
                  )}
                  <Button aria-label={`Test ${connection.name}`} size="small" variant="ghost" disabled={pending !== null} onClick={() => void run(connection, "test")}>
                    {pending === `${connection.id}:test` ? "Testing…" : "Test"}
                  </Button>
                </div>
                <div className="connection-secondary-actions">
                  <Button id={`edit-connection-${connection.id}`} aria-label={`Edit ${connection.name}`} aria-describedby={isConnected ? `edit-lock-${connection.id}` : undefined} size="small" variant="ghost" disabled={pending !== null || isConnected} onClick={() => onEdit(connection)}>Edit</Button>
                  <Button id={`delete-connection-${connection.id}`} aria-label={`Delete ${connection.name}`} size="small" variant="ghost" className="connection-delete" disabled={pending !== null} onClick={() => { restoreDeleteFocus.current = connection.id; setDeleting(connection); }}><Trash2 aria-hidden="true" size={13} />Delete</Button>
                </div>
              </div>
            </article>
          );
        })}
      </div>
      <div className="connection-security-note"><KeyRound aria-hidden="true" size={14} />Secrets are write-only and never returned to this browser.</div>

      <Dialog
        open={deleting !== null}
        onOpenChange={(open) => {
          if (!open && pending !== `${deleting?.id}:delete`) setDeleting(null);
        }}
        title={deleting ? `Delete ${deleting.name}?` : "Delete connection?"}
        description="This disconnects the runtime, deletes the saved profile, and removes its keychain entries. This cannot be undone."
      >
        {deleting && results[deleting.id]?.kind === "error" ? (
          <div className="connection-inline-error delete-connection-error" role="alert">
            <CircleAlert aria-hidden="true" size={15} />
            <span>{results[deleting.id]?.message}{results[deleting.id]?.correlationId ? ` Correlation ID: ${results[deleting.id]?.correlationId}` : ""}</span>
          </div>
        ) : null}
        <div className="delete-connection-actions">
          <Button variant="secondary" disabled={pending !== null} onClick={() => setDeleting(null)}>Keep connection</Button>
          <Button variant="danger" disabled={pending !== null} onClick={() => { if (deleting) void run(deleting, "delete"); }}>
            {pending?.endsWith(":delete") ? "Deleting connection…" : "Delete connection"}
          </Button>
        </div>
      </Dialog>
    </>
  );
}
