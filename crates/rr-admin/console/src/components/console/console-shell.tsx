"use client";

import {
  Activity,
  Bell,
  CircleGauge,
  Database,
  FileClock,
  GitFork,
  Group,
  LogOut,
  Menu,
  Network,
  Pause,
  Play,
  Server,
  ServerCog,
} from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useState, type ReactNode } from "react";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { GlobalSearch } from "@/components/console/global-search";
import { SourceEvidence } from "@/components/console/source-evidence";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Drawer } from "@/components/ui/drawer";
import { consoleApi } from "@/lib/api";
import { useActiveConnection } from "@/lib/connection-context";
import { hrefWithConnection, switchConnectionHref } from "@/lib/query-state";
import { clusterRoutes } from "@/lib/routes";
import { useConsoleStream } from "@/lib/use-console-stream";
import { RefreshProvider, useRefresh } from "@/lib/refresh-context";
import { useRouteQueryState } from "@/lib/use-route-query-state";
import { formatTimestamp } from "@/lib/format";
import { cn } from "@/lib/utils";

const routeIcons = {
  overview: CircleGauge,
  nodes: Server,
  topology: Network,
  groups: Group,
  performance: Activity,
  snapshots: Database,
  lifecycle: FileClock,
  events: Bell,
  sources: ServerCog,
};

function Brand({ href }: { href: string }) {
  return (
    <Link href={href} className="console-brand">
      <svg viewBox="0 0 32 32" width="28" height="28" aria-hidden="true">
        <circle cx="16" cy="16" r="11" fill="none" stroke="currentColor" strokeWidth="1.5" />
        <circle cx="16" cy="5" r="3" fill="var(--acc)" />
        <circle cx="6.5" cy="21.5" r="3" fill="var(--bg-0)" stroke="currentColor" strokeWidth="1.5" />
        <circle cx="25.5" cy="21.5" r="3" fill="var(--bg-0)" stroke="currentColor" strokeWidth="1.5" />
      </svg>
      <span><strong>Rr</strong><small>CONSOLE</small></span>
    </Link>
  );
}

function Navigation({ onNavigate }: { onNavigate?: () => void }) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  return (
    <nav className="console-navigation" aria-label="Console navigation">
      {["Cluster", "Observe", "System"].map((section) => (
        <div className="nav-section" key={section}>
          <div className="nav-section-label">{section}</div>
          {clusterRoutes.filter((route) => route.section === section).map((route) => {
            const Icon = routeIcons[route.key];
            const active = pathname.startsWith(route.href);
            return (
              <Link
                key={route.key}
                href={hrefWithConnection(route.href, searchParams)}
                className={cn("nav-link", active && "nav-link-active")}
                aria-current={active ? "page" : undefined}
                onClick={onNavigate}
              >
                <Icon aria-hidden="true" size={15} />
                <span>{route.title}</span>
              </Link>
            );
          })}
        </div>
      ))}
    </nav>
  );
}

function ConnectionSelector() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const router = useRouter();
  const { activeConnection, connections } = useActiveConnection();
  if (!activeConnection) return null;
  const activeConnections = connections.filter((connection) => connection.status !== "DISCONNECTED");
  const tone = activeConnection.status === "CONNECTED"
    ? "ok"
    : activeConnection.status === "UNREACHABLE" ? "bad" : "accent";
  return (
    <div className="connection-selector">
      <label>
        <span className="sr-only">Active connection</span>
        <select
          aria-label="Active connection"
          value={activeConnection.id}
          onChange={(event) => router.push(switchConnectionHref(pathname, searchParams.toString(), event.target.value))}
        >
          {activeConnections.map((connection) => (
            <option key={connection.id} value={connection.id}>{connection.name} · {connection.status.toLowerCase()}</option>
          ))}
        </select>
      </label>
      <Badge tone={tone}>{activeConnection.status}</Badge>
      <Link href="/connections/" aria-label="Manage connections">Manage</Link>
    </div>
  );
}

function TimeWindowControl() {
  const { state, setWindow } = useRouteQueryState();
  return (
    <div className="window-control" role="group" aria-label="Observation time window">
      {(["15m", "1h", "6h", "24h"] as const).map((window) => (
        <button
          key={window}
          type="button"
          aria-pressed={state.window === window}
          onClick={() => setWindow(window)}
        >
          {window}
        </button>
      ))}
    </div>
  );
}

function ShellContent({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const router = useRouter();
  const currentRoute = clusterRoutes.find((route) => pathname.startsWith(route.href)) ?? clusterRoutes[0];
  const {
    connectionId,
    summary: sharedSummary,
    intervalMs,
    setIntervalMs,
    paused,
    setPaused,
    lastUpdated,
    streamState,
  } = useRefresh();
  const summary = sharedSummary?.data;
  const meta = sharedSummary?.meta;
  const [mobileOpen, setMobileOpen] = useState(false);
  useConsoleStream();

  const logout = async () => {
    try {
      await consoleApi.logout();
    } finally {
      router.replace("/login/");
    }
  };

  return (
    <div className="console-shell">
      <a className="skip-link" href="#console-main">Skip to main content</a>
      <aside className="console-sidebar">
        <Brand href={hrefWithConnection("/overview", searchParams)} />
        <Navigation />
        <div className="sidebar-foot"><GitFork aria-hidden="true" size={12} /> Schema v1 · Read-only</div>
      </aside>
      <div className="console-workspace">
        <header className="console-topbar">
          <div className="topbar-route">
            <div className="mobile-navigation">
              <Drawer
                trigger={<Button variant="ghost" size="icon" aria-label="Open navigation"><Menu aria-hidden="true" size={17} /></Button>}
                title="Console navigation"
                description="Cluster observation views"
                open={mobileOpen}
                onOpenChange={setMobileOpen}
              >
                <Brand href={hrefWithConnection("/overview", searchParams)} />
                <Navigation onNavigate={() => setMobileOpen(false)} />
              </Drawer>
            </div>
            <span>{currentRoute.title}</span>
          </div>
          <div className="topbar-actions">
            <ConnectionSelector />
            <GlobalSearch key={connectionId ?? "none"} />
            <Link href={hrefWithConnection("/events", searchParams)} className="alert-indicator" aria-label={`${summary?.active_alerts ?? "Unknown"} active alerts`}>
              <Bell aria-hidden="true" size={14} />
              <span>{summary?.active_alerts ?? "—"}</span>
            </Link>
            <Badge tone="system" className="environment-badge">{summary?.environment ?? "Environment unknown"}</Badge>
            <Badge tone={streamState === "LIVE" ? "ok" : streamState === "RECONNECTING" ? "accent" : "neutral"} className="stream-badge">
              <span className="stream-dot" aria-hidden="true" />{streamState === "LIVE" ? "Data plane live" : streamState === "RECONNECTING" ? "Reconnecting" : streamState === "PAUSED" ? "Refresh paused" : streamState === "STOPPED" ? "Connection unavailable" : "Connecting"}
            </Badge>
            <label className="refresh-interval">
              <span className="sr-only">Refresh interval</span>
              <select value={intervalMs} onChange={(event) => setIntervalMs(Number(event.target.value))}>
                <option value={5000}>5s refresh</option>
                <option value={15000}>15s refresh</option>
                <option value={30000}>30s refresh</option>
              </select>
            </label>
            <Button variant="ghost" size="small" onClick={() => setPaused((value) => !value)} aria-pressed={paused}>
              {paused ? <Play aria-hidden="true" size={13} /> : <Pause aria-hidden="true" size={13} />}
              {paused ? "Resume" : "Pause"}
            </Button>
            <span className="last-updated">Updated <time>{formatTimestamp(lastUpdated)}</time></span>
            <Button variant="ghost" size="icon" onClick={logout} aria-label="Sign out"><LogOut aria-hidden="true" size={14} /></Button>
          </div>
        </header>
        <div className="cluster-strip">
          <div className="cluster-identity">
            <span className="cluster-label">Cluster</span>
            <strong>{summary?.cluster_name ?? meta?.cluster_id ?? "Discovering cluster"}</strong>
            {meta ? <FreshnessBadge meta={meta} /> : <Badge>Freshness unknown</Badge>}
          </div>
          <div className="cluster-controls">
            <TimeWindowControl />
            {meta ? <SourceEvidence meta={meta} /> : null}
          </div>
        </div>
        <main key={connectionId ?? "none"} id="console-main" className="console-main" tabIndex={-1}>{children}</main>
      </div>
    </div>
  );
}

export function ConsoleShell({ children }: { children: ReactNode }) {
  const { activeConnection } = useActiveConnection();
  return <RefreshProvider connectionId={activeConnection?.id ?? null}><ShellContent>{children}</ShellContent></RefreshProvider>;
}
