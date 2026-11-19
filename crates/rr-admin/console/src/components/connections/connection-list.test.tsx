import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ConsoleApiError } from "@/lib/api";
import type { ConnectionSummary } from "@/lib/connections";
import { ConnectionList } from "./connection-list";

const mocks = vi.hoisted(() => ({
  context: { current: null as null | Record<string, unknown> },
  refresh: vi.fn(),
  connect: vi.fn(),
  disconnect: vi.fn(),
  remove: vi.fn(),
  test: vi.fn(),
}));

vi.mock("@/lib/connection-context", () => ({
  useActiveConnection: () => mocks.context.current,
}));
vi.mock("@/lib/connections", () => ({
  connectionsApi: {
    connect: mocks.connect,
    disconnect: mocks.disconnect,
    remove: mocks.remove,
    test: mocks.test,
  },
}));

function profile(overrides: Partial<ConnectionSummary> = {}): ConnectionSummary {
  return {
    id: "cluster-a",
    name: "Production",
    environment: "prod",
    admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
    discovery: null,
    cluster_name: "production",
    admin_tls: { ca: null, cert: null, key: null, domain: null },
    discovery_tls: { ca: null, cert: null, key: null, domain: null },
    discovery_username: null,
    discovery_allow_insecure: false,
    prometheus: null,
    poll_interval_secs: 5,
    created_at: "2026-11-18T08:00:00Z",
    updated_at: "2026-11-18T08:00:00Z",
    observed_cluster_id: "cluster-a",
    last_connected_at: "2026-11-18T08:01:00Z",
    source: "PROFILE",
    status: "DISCONNECTED",
    credentials_configured: true,
    credential_source: "MEMORY",
    last_error: null,
    node_count: null,
    group_count: null,
    ...overrides,
  };
}

function context(overrides: Record<string, unknown> = {}) {
  return {
    connections: [],
    keychainAvailable: true,
    loading: false,
    error: null,
    connectionId: null,
    activeConnection: null,
    recentConnectionId: null,
    refresh: mocks.refresh,
    ...overrides,
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe("ConnectionList", () => {
  beforeEach(() => {
    mocks.refresh.mockReset().mockResolvedValue(undefined);
    mocks.connect.mockReset().mockResolvedValue({ data: profile({ status: "CONNECTED" }) });
    mocks.disconnect.mockReset().mockResolvedValue({ data: profile() });
    mocks.remove.mockReset().mockResolvedValue(undefined);
    mocks.test.mockReset().mockResolvedValue({ data: { tested: true } });
  });

  it("announces loading without rendering actions", () => {
    mocks.context.current = context({ loading: true });
    render(<ConnectionList onEdit={vi.fn()} />);

    expect(screen.getByRole("status", { name: "Loading connections" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "Connect" })).not.toBeInTheDocument();
  });

  it("offers a retry when the connection list fails", async () => {
    const user = userEvent.setup();
    mocks.context.current = context({
      error: new ConsoleApiError(500, "CONNECTION_STORE_FAILED", "Profile store unavailable", "corr-list"),
    });
    render(<ConnectionList onEdit={vi.fn()} />);

    expect(screen.getByRole("alert")).toHaveTextContent("Profile store unavailable");
    await user.click(screen.getByRole("button", { name: "Retry loading connections" }));
    expect(mocks.refresh).toHaveBeenCalledTimes(1);
  });

  it("teaches the next step in the empty state", () => {
    mocks.context.current = context();
    render(<ConnectionList onEdit={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "No connections yet" })).toBeVisible();
    expect(screen.getByText(/Save an admin seed or discovery endpoint/)).toBeVisible();
    expect(screen.getByRole("link", { name: "Create first connection" })).toHaveAttribute("href", "/connections/new");
  });

  it("renders list DTO runtime counts and disables edit while connected", async () => {
    const user = userEvent.setup();
    const onEdit = vi.fn();
    mocks.context.current = context({
      connections: [profile({ status: "CONNECTED", node_count: 3, group_count: 12 })],
    });
    render(<ConnectionList onEdit={onEdit} />);

    const item = screen.getByRole("article", { name: "Production connection" });
    expect(item).toHaveTextContent("3 nodes / 12 groups");
    expect(item).toHaveTextContent("Disconnect this connection before editing.");
    const edit = within(item).getByRole("button", { name: "Edit Production" });
    expect(edit).toBeDisabled();
    expect(edit).toHaveAccessibleDescription("Disconnect this connection before editing.");
    await user.click(edit);
    expect(onEdit).not.toHaveBeenCalled();
  });

  it("opens every observable active runtime, including legacy startup", () => {
    mocks.context.current = context({
      connections: [
        profile({ id: "connected", name: "Connected", status: "CONNECTED" }),
        profile({ id: "degraded", name: "Degraded", status: "DEGRADED" }),
        profile({ id: "unreachable", name: "Unreachable", status: "UNREACHABLE" }),
        profile({ id: "startup-1", name: "Legacy", source: "STARTUP", status: "CONNECTED" }),
        profile({ id: "connecting", name: "Connecting", status: "CONNECTING" }),
        profile({ id: "disconnected", name: "Disconnected", status: "DISCONNECTED" }),
      ],
    });
    render(<ConnectionList onEdit={vi.fn()} />);

    for (const name of ["Connected", "Degraded", "Unreachable"]) {
      expect(screen.getByRole("link", { name: `Open ${name}` })).toHaveAttribute("href", `/overview?connection=${name.toLowerCase()}`);
    }
    expect(screen.getByRole("link", { name: "Open Legacy" })).toHaveAttribute("href", "/overview?connection=startup-1");
    expect(screen.queryByRole("link", { name: "Open Connecting" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Open Disconnected" })).not.toBeInTheDocument();
  });

  it("projects failure, degradation and recovery without dropping the open action", () => {
    const view = render(<ConnectionList onEdit={vi.fn()} />);
    for (const status of ["CONNECTED", "UNREACHABLE", "DEGRADED", "CONNECTED"] as const) {
      mocks.context.current = context({ connections: [profile({ status })] });
      view.rerender(<ConnectionList onEdit={vi.fn()} />);
      const item = screen.getByRole("article", { name: "Production connection" });
      expect(within(item).getByText(`${status[0]}${status.slice(1).toLowerCase()}`)).toBeVisible();
      expect(within(item).getByRole("link", { name: "Open Production" })).toBeVisible();
      expect(within(item).getByRole("button", { name: "Disconnect Production" })).toBeEnabled();
    }
  });

  it("runs test, connect, disconnect, edit and confirmed delete actions", async () => {
    const user = userEvent.setup();
    const onEdit = vi.fn();
    mocks.context.current = context({ connections: [profile()] });
    const view = render(<ConnectionList onEdit={onEdit} />);

    await user.click(screen.getByRole("button", { name: "Test Production" }));
    expect(await screen.findByText("Connection test passed.")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Connect Production" }));
    await waitFor(() => expect(mocks.connect).toHaveBeenCalledWith("cluster-a"));
    await user.click(screen.getByRole("button", { name: "Edit Production" }));
    expect(onEdit).toHaveBeenCalledWith(expect.objectContaining({ id: "cluster-a" }));

    mocks.context.current = context({ connections: [profile({ status: "CONNECTED" })] });
    view.rerender(<ConnectionList onEdit={onEdit} />);
    await user.click(screen.getByRole("button", { name: "Disconnect Production" }));
    await waitFor(() => expect(mocks.disconnect).toHaveBeenCalledWith("cluster-a"));

    await user.click(screen.getByRole("button", { name: "Delete Production" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete Production?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete connection" }));
    await waitFor(() => expect(mocks.remove).toHaveBeenCalledWith("cluster-a"));
    expect(mocks.refresh).toHaveBeenCalled();
  });

  it("keeps a failed deletion diagnostic open while refreshing the real disconnected state", async () => {
    const user = userEvent.setup();
    mocks.remove.mockRejectedValue(new ConsoleApiError(500, "CONNECTION_STORE_FAILED", "Profile store is read-only", "corr-delete"));
    mocks.context.current = context({ connections: [profile({ status: "CONNECTED" })] });
    const view = render(<ConnectionList onEdit={vi.fn()} />);
    mocks.refresh.mockImplementation(async () => {
      mocks.context.current = context({ connections: [profile({ status: "DISCONNECTED" })] });
      view.rerender(<ConnectionList onEdit={vi.fn()} />);
    });

    await user.click(screen.getByRole("button", { name: "Delete Production" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete Production?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete connection" }));

    expect(await within(dialog).findByRole("alert")).toHaveTextContent("Profile store is read-only");
    expect(within(dialog).getByRole("alert")).toHaveTextContent("Correlation ID: corr-delete");
    expect(dialog).toBeVisible();
    const item = screen.getByRole("article", { name: "Production connection", hidden: true });
    expect(item).toHaveTextContent("Disconnected");
    expect(mocks.refresh).toHaveBeenCalledTimes(1);
  });

  it("keeps existing profiles visible while the list refreshes", () => {
    mocks.context.current = context({ connections: [profile()], loading: true });
    render(<ConnectionList onEdit={vi.fn()} />);

    expect(screen.getByRole("article", { name: "Production connection" })).toBeVisible();
    expect(screen.getByRole("status", { name: "Refreshing connections" })).toBeVisible();
    expect(screen.queryByRole("status", { name: "Loading connections" })).not.toBeInTheDocument();
  });

  it("keeps sanitized operation failures next to the affected connection", async () => {
    const user = userEvent.setup();
    mocks.connect.mockRejectedValue(new ConsoleApiError(502, "CONNECTION_INITIALIZATION_FAILED", "No admin endpoint responded", "corr-connect"));
    mocks.context.current = context({ connections: [profile()] });
    render(<ConnectionList onEdit={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Connect Production" }));

    const item = screen.getByRole("article", { name: "Production connection" });
    expect(await within(item).findByRole("alert")).toHaveTextContent("No admin endpoint responded");
    expect(within(item).getByText(/corr-connect/)).toBeVisible();
  });

  it("clears a stale operation error after a successful retry", async () => {
    const user = userEvent.setup();
    mocks.connect
      .mockRejectedValueOnce(new ConsoleApiError(502, "CONNECTION_INITIALIZATION_FAILED", "No admin endpoint responded", "corr-connect"))
      .mockResolvedValueOnce({ data: profile({ status: "CONNECTED" }) });
    mocks.context.current = context({ connections: [profile()] });
    render(<ConnectionList onEdit={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Connect Production" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("No admin endpoint responded");
    await user.click(screen.getByRole("button", { name: "Connect Production" }));

    await waitFor(() => expect(screen.queryByRole("alert")).not.toBeInTheDocument());
    expect(mocks.connect).toHaveBeenCalledTimes(2);
  });

  it("focuses New connection after a successful delete removes the trigger", async () => {
    const user = userEvent.setup();
    mocks.context.current = context({ connections: [profile()] });
    render(<><a id="new-connection" href="/connections/new/">New connection</a><ConnectionList onEdit={vi.fn()} /></>);

    await user.click(screen.getByRole("button", { name: "Delete Production" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete Production?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete connection" }));

    await waitFor(() => expect(screen.getByRole("link", { name: "New connection" })).toHaveFocus());
    expect(mocks.remove).toHaveBeenCalledWith("cluster-a");
    expect(mocks.refresh).toHaveBeenCalledTimes(1);
  });

  it("keeps a pending delete open through close attempts and focuses New after success", async () => {
    const user = userEvent.setup();
    const pending = deferred();
    mocks.remove.mockReturnValue(pending.promise);
    mocks.context.current = context({ connections: [profile()] });
    render(<><a id="new-connection" href="/connections/new/">New connection</a><ConnectionList onEdit={vi.fn()} /></>);
    await user.click(screen.getByRole("button", { name: "Delete Production" }));
    const dialog = await screen.findByRole("dialog", { name: "Delete Production?" });
    await user.click(within(dialog).getByRole("button", { name: "Delete connection" }));
    await waitFor(() => expect(mocks.remove).toHaveBeenCalledWith("cluster-a"));

    await user.click(within(dialog).getByRole("button", { name: "Close dialog" }));
    expect(dialog).toBeVisible();
    await user.keyboard("{Escape}");
    expect(dialog).toBeVisible();
    const keep = within(dialog).getByRole("button", { name: "Keep connection" });
    expect(keep).toBeDisabled();
    keep.click();
    expect(dialog).toBeVisible();

    await act(async () => pending.resolve());

    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.getByRole("link", { name: "New connection" })).toHaveFocus();
    expect(mocks.refresh).toHaveBeenCalledTimes(1);
  });
});
