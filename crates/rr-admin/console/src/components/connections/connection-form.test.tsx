import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ConsoleApiError } from "@/lib/api";
import type { ConnectionSummary } from "@/lib/connections";
import { ConnectionForm } from "./connection-form";

const mocks = vi.hoisted(() => ({
  create: vi.fn(),
  update: vi.fn(),
  testCandidate: vi.fn(),
  connect: vi.fn(),
}));

vi.mock("@/lib/connections", () => ({
  connectionsApi: {
    create: mocks.create,
    update: mocks.update,
    testCandidate: mocks.testCandidate,
    connect: mocks.connect,
  },
}));

const existing: ConnectionSummary = {
  id: "cluster-a",
  name: "Production",
  environment: "prod",
  admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
  discovery: "etcd://127.0.0.1:2379",
  cluster_name: "production",
  admin_tls: { ca: "/etc/rr/ca.pem", cert: null, key: null, domain: null },
  discovery_tls: { ca: "/etc/rr/etcd-ca.pem", cert: null, key: null, domain: null },
  discovery_username: "viewer",
  discovery_allow_insecure: false,
  prometheus: null,
  poll_interval_secs: 5,
  created_at: "2026-11-18T08:00:00Z",
  updated_at: "2026-11-18T08:00:00Z",
  observed_cluster_id: "cluster-a",
  last_connected_at: null,
  source: "PROFILE",
  status: "DISCONNECTED",
  credentials_configured: true,
  credential_source: "MEMORY",
  last_error: null,
  node_count: null,
  group_count: null,
};

function response<T>(data: T) {
  return { data, meta: {} };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function fillRequiredFields(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText("Connection name"), "Production");
  await user.type(screen.getByLabelText("Seed 1 node ID"), "1");
  await user.type(screen.getByLabelText("Seed 1 AdminService address"), "127.0.0.1:52001");
}

describe("ConnectionForm", () => {
  beforeEach(() => {
    mocks.create.mockReset().mockResolvedValue(response(existing));
    mocks.update.mockReset().mockResolvedValue(response(existing));
    mocks.testCandidate.mockReset().mockResolvedValue(response({ tested: true }));
    mocks.connect.mockReset().mockResolvedValue(response({ ...existing, status: "CONNECTED" }));
  });

  it("rejects a missing name and missing cluster source with field-level guidance", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(await screen.findByText("Enter a connection name.")).toBeVisible();
    expect(screen.getByText("Add at least one admin seed or discovery endpoint.")).toBeVisible();
    expect(screen.getByLabelText("Connection name")).toHaveFocus();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("validates only the touched field on blur", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);

    await user.click(screen.getByLabelText("Connection name"));
    await user.tab();

    expect(await screen.findByText("Enter a connection name.")).toBeVisible();
    expect(screen.queryByText("Add at least one admin seed or discovery endpoint.")).not.toBeInTheDocument();
  });

  it("submits admin seeds and discovery together instead of making them exclusive", async () => {
    const user = userEvent.setup();
    const onComplete = vi.fn();
    render(<ConnectionForm keychainAvailable onComplete={onComplete} />);
    await fillRequiredFields(user);
    await user.type(screen.getByLabelText("Discovery endpoints"), "etcd://127.0.0.1:2379");
    await user.type(screen.getByLabelText("Discovery cluster"), "production");
    await user.click(screen.getByRole("checkbox", { name: "Allow insecure discovery transport" }));

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(1));
    expect(mocks.create.mock.calls[0]![0]).toMatchObject({
      name: "Production",
      admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
      discovery: "etcd://127.0.0.1:2379",
      cluster_name: "production",
    });
    expect(onComplete).toHaveBeenCalledWith(existing, false);
  });

  it("keeps a failed candidate test diagnostic visible and still allows saving", async () => {
    const user = userEvent.setup();
    mocks.testCandidate.mockRejectedValue(new ConsoleApiError(502, "CONNECTION_TEST_FAILED", "AdminService authentication failed", "corr-test"));
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);

    await user.click(screen.getByRole("button", { name: "Test connection" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("AdminService authentication failed");
    expect(screen.getByRole("alert")).toHaveTextContent("Correlation ID: corr-test");
    await user.click(screen.getByRole("button", { name: "Save connection" }));

    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(1));
  });

  it("omits an unchanged admin token when editing", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm connection={existing} keychainAvailable />);

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    await waitFor(() => expect(mocks.update).toHaveBeenCalledTimes(1));
    expect(mocks.update.mock.calls[0]![0]).not.toHaveProperty("admin_token");
    expect(mocks.update.mock.calls[0]![0]).not.toHaveProperty("discovery_password");
  });

  it("serializes null for clear and a non-empty value for secret replacement", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm connection={existing} keychainAvailable />);
    const adminModes = screen.getByRole("radiogroup", { name: "Admin token update" });
    const discoveryModes = screen.getByRole("radiogroup", { name: "Discovery password update" });
    await user.click(within(adminModes).getByRole("radio", { name: "Clear secret" }));
    await user.click(within(discoveryModes).getByRole("radio", { name: "Replace secret" }));
    await user.type(screen.getByLabelText("New discovery password"), "new-discovery-secret");

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    await waitFor(() => expect(mocks.update).toHaveBeenCalledTimes(1));
    expect(mocks.update.mock.calls[0]![1]).toMatchObject({
      admin_token: null,
      discovery_password: "new-discovery-secret",
    });
  });

  it("explains process-only credentials when the OS keychain is unavailable", () => {
    render(<ConnectionForm keychainAvailable={false} />);

    expect(screen.getByText(/OS keychain is unavailable/)).toBeVisible();
    expect(screen.getByRole("checkbox", { name: "Remember credentials in OS keychain" })).toBeDisabled();
  });

  it("persists a new profile before reporting a failed save-and-connect", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    const onComplete = vi.fn();
    mocks.connect.mockRejectedValue(new ConsoleApiError(502, "CONNECTION_INITIALIZATION_FAILED", "No admin endpoint responded", "corr-connect"));
    render(<ConnectionForm keychainAvailable onSaved={onSaved} onComplete={onComplete} />);
    await fillRequiredFields(user);

    await user.click(screen.getByRole("button", { name: "Save & connect" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Connection saved, but connecting failed");
    expect(screen.getByRole("alert")).toHaveTextContent("No admin endpoint responded");
    expect(screen.getByRole("alert")).toHaveTextContent("Correlation ID: corr-connect");
    expect(mocks.create).toHaveBeenCalledTimes(1);
    expect(mocks.connect).toHaveBeenCalledWith("cluster-a");
    expect(onSaved).toHaveBeenCalledWith(existing);
    expect(onComplete).not.toHaveBeenCalled();
  });

  it("uses the saved profile ID for a retry after save-and-connect fails", async () => {
    const user = userEvent.setup();
    mocks.connect.mockRejectedValueOnce(new Error("Unavailable")).mockResolvedValueOnce(response({ ...existing, status: "CONNECTED" }));
    const onComplete = vi.fn();
    render(<ConnectionForm keychainAvailable onComplete={onComplete} />);
    await fillRequiredFields(user);
    await user.click(screen.getByRole("button", { name: "Save & connect" }));
    await screen.findByText(/Connection saved, but connecting failed/);

    await user.click(screen.getByRole("button", { name: "Save & connect" }));

    await waitFor(() => expect(mocks.update).toHaveBeenCalledWith("cluster-a", expect.any(Object)));
    expect(mocks.create).toHaveBeenCalledTimes(1);
    expect(mocks.connect).toHaveBeenCalledTimes(2);
    expect(onComplete).toHaveBeenCalledWith(expect.objectContaining({ status: "CONNECTED" }), true);
  });

  it("adds and removes accessible admin seed rows", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    await user.click(screen.getByRole("button", { name: "Add admin seed" }));
    await user.type(screen.getByLabelText("Seed 2 node ID"), "2");
    await user.type(screen.getByLabelText("Seed 2 AdminService address"), "127.0.0.1:52002");
    await user.click(screen.getByRole("button", { name: "Remove seed 1" }));
    await user.click(screen.getByRole("button", { name: "Save connection" }));

    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(1));
    expect(mocks.create.mock.calls[0]![0].admin_seeds).toEqual([
      { node_id: 2, admin_addr: "127.0.0.1:52002" },
    ]);
  });

  it("opens advanced errors and focuses the invalid field", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    const summary = screen.getByText("Advanced settings");
    const details = summary.closest("details");
    await user.click(summary);
    const pollInterval = screen.getByLabelText("Poll interval (seconds)");
    await user.clear(pollInterval);
    await user.type(pollInterval, "0");
    await user.click(summary);
    expect(details).not.toHaveAttribute("open");

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(details).toHaveAttribute("open");
    await waitFor(() => expect(pollInterval).toHaveFocus());
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("rejects an empty secret replacement and focuses the secret field", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm connection={existing} keychainAvailable />);
    const adminModes = screen.getByRole("radiogroup", { name: "Admin token update" });
    await user.click(within(adminModes).getByRole("radio", { name: "Replace secret" }));

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(await screen.findByText("Enter a replacement admin token, or choose Keep or Clear.")).toBeVisible();
    expect(screen.getByLabelText("New admin token")).toHaveFocus();
    expect(mocks.update).not.toHaveBeenCalled();
  });

  it("rejects duplicate seed node IDs and addresses before submission", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    await user.click(screen.getByRole("button", { name: "Add admin seed" }));
    await user.type(screen.getByLabelText("Seed 2 node ID"), "1");
    await user.type(screen.getByLabelText("Seed 2 AdminService address"), "127.0.0.1:52001");

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(await screen.findByText("Each admin seed must use a unique node ID.")).toBeVisible();
    expect(screen.getByText("Each admin seed must use a unique AdminService address.")).toBeVisible();
    expect(screen.getByLabelText("Seed 2 node ID")).toHaveFocus();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("requires certificate and private key paths as a pair", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    await user.type(screen.getByLabelText("Admin certificate path"), "/etc/rr/client.pem");

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(await screen.findByText("Provide both the admin certificate and private key paths, or neither.")).toBeVisible();
    expect(screen.getByLabelText("Admin private key path")).toHaveFocus();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("passes the current connection ID when testing an edited candidate with Keep", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm connection={existing} keychainAvailable />);

    await user.click(screen.getByRole("button", { name: "Test connection" }));

    await waitFor(() => expect(mocks.testCandidate).toHaveBeenCalledWith(
      expect.not.objectContaining({ admin_token: expect.anything() }),
      "cluster-a",
    ));
  });

  it("refreshes persisted data but ignores navigation after the form unmounts", async () => {
    const user = userEvent.setup();
    const pending = deferred<ReturnType<typeof response<ConnectionSummary>>>();
    const onSaved = vi.fn();
    const onComplete = vi.fn();
    mocks.create.mockReturnValue(pending.promise);
    const view = render(<ConnectionForm keychainAvailable onSaved={onSaved} onComplete={onComplete} />);
    await fillRequiredFields(user);
    await user.click(screen.getByRole("button", { name: "Save & connect" }));
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(1));

    view.unmount();
    await act(async () => pending.resolve(response(existing)));

    expect(onSaved).toHaveBeenCalledWith(existing);
    expect(onComplete).not.toHaveBeenCalled();
    expect(mocks.connect).not.toHaveBeenCalled();
  });

  it("does not let a deferred A update complete a newly opened B form", async () => {
    const user = userEvent.setup();
    const pending = deferred<ReturnType<typeof response<ConnectionSummary>>>();
    const completeA = vi.fn();
    const completeB = vi.fn();
    const savedA = vi.fn();
    mocks.update.mockReturnValue(pending.promise);
    const view = render(<ConnectionForm key="a" connection={existing} keychainAvailable onSaved={savedA} onComplete={completeA} />);
    await user.click(screen.getByRole("button", { name: "Save connection" }));
    await waitFor(() => expect(mocks.update).toHaveBeenCalledWith("cluster-a", expect.any(Object)));

    view.rerender(<ConnectionForm key="b" connection={{ ...existing, id: "cluster-b", name: "Cluster B" }} keychainAvailable onComplete={completeB} />);
    await act(async () => pending.resolve(response(existing)));

    expect(savedA).toHaveBeenCalledWith(existing);
    expect(completeA).not.toHaveBeenCalled();
    expect(completeB).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Connection name")).toHaveValue("Cluster B");
  });

  it("moves focus to the remaining seed after removing a row", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    await user.click(screen.getByRole("button", { name: "Add admin seed" }));
    await user.type(screen.getByLabelText("Seed 2 node ID"), "2");
    await user.type(screen.getByLabelText("Seed 2 AdminService address"), "127.0.0.1:52002");

    await user.click(screen.getByRole("button", { name: "Remove seed 1" }));

    await waitFor(() => expect(screen.getByLabelText("Seed 1 node ID")).toHaveFocus());
    expect(screen.getByLabelText("Seed 1 node ID")).toHaveValue("2");
    await user.click(screen.getByRole("button", { name: "Remove seed 1" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Add admin seed" })).toHaveFocus());
  });

  it.each([
    "user:endpoint-secret@127.0.0.1:52001",
    "127.0.0.1:0",
    "127.0.0.1:65536",
  ])("rejects an unsafe or invalid admin seed address: %s", async (address) => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await user.type(screen.getByLabelText("Connection name"), "Production");
    await user.type(screen.getByLabelText("Seed 1 node ID"), "1");
    await user.type(screen.getByLabelText("Seed 1 AdminService address"), address);

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(await screen.findByText(/host:port without userinfo/)).toBeVisible();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("aligns discovery transport, username and userinfo validation with the BFF", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    await user.type(screen.getByLabelText("Discovery endpoints"), "etcd://127.0.0.1:2379");
    await user.type(screen.getByLabelText("Discovery cluster"), "production");

    await user.click(screen.getByRole("button", { name: "Save connection" }));
    expect(await screen.findByText("Provide a discovery CA or explicitly allow insecure transport.")).toBeVisible();

    await user.click(screen.getByRole("checkbox", { name: "Allow insecure discovery transport" }));
    await user.type(screen.getByLabelText("Discovery username"), "viewer");
    await user.click(screen.getByRole("button", { name: "Save connection" }));
    expect(await screen.findByText("Discovery username requires a discovery CA.")).toBeVisible();

    await user.type(screen.getByLabelText("Discovery CA path"), "/etc/rr/etcd-ca.pem");
    await user.click(screen.getByRole("button", { name: "Save connection" }));
    expect(await screen.findByText("Choose either a discovery CA or insecure transport, not both.")).toBeVisible();

    await user.clear(screen.getByLabelText("Discovery endpoints"));
    await user.type(screen.getByLabelText("Discovery endpoints"), "etcd://user:endpoint-secret@127.0.0.1:2379");
    await user.click(screen.getByRole("button", { name: "Save connection" }));
    expect(await screen.findByText(/discovery endpoints without userinfo/)).toBeVisible();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("requires CA-backed TLS identities and HTTP(S) Prometheus URLs", async () => {
    const user = userEvent.setup();
    render(<ConnectionForm keychainAvailable />);
    await fillRequiredFields(user);
    await user.type(screen.getByLabelText("Admin certificate path"), "/etc/rr/client.pem");
    await user.type(screen.getByLabelText("Admin private key path"), "/etc/rr/client.key");
    await user.type(screen.getByLabelText("Discovery domain"), "etcd.internal");
    await user.click(screen.getByText("Advanced settings"));
    await user.type(screen.getByLabelText("Prometheus URL"), "ftp://prometheus.internal");
    await user.type(screen.getByLabelText("Prometheus cluster matcher"), "production");

    await user.click(screen.getByRole("button", { name: "Save connection" }));

    expect(await screen.findByText("Admin certificate and domain settings require an admin CA.")).toBeVisible();
    expect(screen.getByText("Discovery certificate and domain settings require a discovery CA.")).toBeVisible();
    expect(screen.getByText("Enter an absolute HTTP or HTTPS Prometheus URL.")).toBeVisible();
    expect(mocks.create).not.toHaveBeenCalled();
  });
});
