"use client";

import { FileClock, Group, Network, Search, Server } from "lucide-react";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { Dialog } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { serializeQueryState } from "@/lib/query-state";
import { clusterRoutes } from "@/lib/routes";
import { useRouteQueryState } from "@/lib/use-route-query-state";

export function GlobalSearch() {
  const router = useRouter();
  const { state } = useRouteQueryState();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setOpen((value) => !value);
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  const navigate = (href: string, filter?: string) => {
    const search = serializeQueryState({ ...state, filters: { ...state.filters, filter } });
    router.push(`${href}${search ? `?${search}` : ""}`);
    setOpen(false);
    setQuery("");
  };
  const normalized = query.trim().toLowerCase();
  const matchingRoutes = clusterRoutes.filter((route) => route.title.toLowerCase().includes(normalized));

  return (
    <>
      <button type="button" className="global-search-trigger" onClick={() => setOpen(true)} aria-label="Search ⌘K">
        <Search aria-hidden="true" size={14} />
        <span>Search</span>
        <kbd>⌘K</kbd>
      </button>
      <Dialog
        open={open}
        onOpenChange={setOpen}
        title="Search console"
        description="Navigate to a view or carry an evidence ID into its filtered route."
      >
        <div className="command-search">
          <Search aria-hidden="true" size={15} />
          <Input
            autoFocus
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Node, group, partition, boot ID, or git commit"
            aria-label="Search query"
          />
        </div>
        <div className="command-results">
          {query.trim() ? (
            <div className="command-group">
              <div className="command-group-label">Evidence lookups</div>
              <button type="button" onClick={() => navigate("/nodes", query)}><Server aria-hidden="true" size={14} />Find node “{query}”</button>
              <button type="button" onClick={() => navigate("/groups", query)}><Group aria-hidden="true" size={14} />Find Raft group “{query}”</button>
              <button type="button" onClick={() => navigate("/topology", query)}><Network aria-hidden="true" size={14} />Find partition “{query}”</button>
              <button type="button" onClick={() => navigate("/lifecycle", query)}><FileClock aria-hidden="true" size={14} />Find boot ID or commit “{query}”</button>
            </div>
          ) : null}
          <div className="command-group">
            <div className="command-group-label">Views</div>
            {matchingRoutes.map((route) => (
              <button type="button" key={route.key} onClick={() => navigate(route.href)}>{route.title}<span>{route.section}</span></button>
            ))}
          </div>
        </div>
      </Dialog>
    </>
  );
}
