import { makeMeta } from "@/test/fixtures";
import type { LoginResponse } from "@/lib/contract";
import {
  buildConsoleUrl,
  ConsoleApiError,
  consoleApi,
  getConsole,
} from "@/lib/api";

describe("same-origin Console API", () => {
  it("builds versioned relative URLs with bounded pagination", () => {
    const url = buildConsoleUrl("/nodes", { page_size: 500, filter: "node 1" });
    expect(url).toBe("/api/v1/console/nodes?page_size=200&filter=node+1");
    expect(url).not.toMatch(/^https?:/);
  });

  it("uses same-origin credentials and returns typed envelopes", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ meta: makeMeta(), data: { items: [], next_cursor: null } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await getConsole("/nodes", { page_size: 50 });
    expect(result.data.items).toEqual([]);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/console/nodes?page_size=50",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("keeps the login CSRF token in memory for reconcile", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: makeMeta(),
        csrf_token: "csrf-session-token",
        expires_at: "2026-11-16T08:15:00Z",
      }), { status: 200, headers: { "content-type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ meta: makeMeta(), data: { accepted: true } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
    vi.stubGlobal("fetch", fetchMock);

    await consoleApi.login("viewer-credential");
    await consoleApi.reconcile();
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/console/sources/reconcile",
      expect.objectContaining({ headers: expect.objectContaining({ "X-Console-CSRF": "csrf-session-token" }) }),
    );
  });

  it("restores CSRF in module memory after a full document reload before reconcile", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: makeMeta(),
        csrf_token: "login-csrf-token",
        expires_at: "2026-11-16T08:15:00Z",
      }), { status: 200, headers: { "content-type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: makeMeta(),
        csrf_token: "restored-csrf-token",
      }), { status: 200, headers: { "content-type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: makeMeta(),
        data: { accepted: true },
      }), { status: 200, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    vi.resetModules();
    const beforeReload = await import("@/lib/api");
    await beforeReload.consoleApi.login("viewer-credential");
    vi.resetModules();
    const afterReload = await import("@/lib/api");
    await afterReload.consoleApi.reconcile();

    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/console/auth/csrf",
      expect.objectContaining({ method: "GET", credentials: "same-origin" }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      "/api/v1/console/sources/reconcile",
      expect.objectContaining({ headers: expect.objectContaining({ "X-Console-CSRF": "restored-csrf-token" }) }),
    );
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(JSON.stringify({ local: Object.entries(localStorage), session: Object.entries(sessionStorage) })).not.toContain("restored-csrf-token");
  });

  it("caches the explicit no-CSRF capability when viewer auth is disabled", async () => {
    vi.resetModules();
    const afterReload = await import("@/lib/api");
    const disabled: LoginResponse = { meta: makeMeta(), csrf_token: null, expires_at: null };
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(disabled), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockImplementation(() => Promise.resolve(new Response(JSON.stringify({ meta: makeMeta(), data: { accepted: true } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      })));
    vi.stubGlobal("fetch", fetchMock);

    await afterReload.consoleApi.reconcile();
    await afterReload.consoleApi.reconcile();

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/v1/console/auth/csrf", expect.objectContaining({ method: "GET" }));
    for (const call of fetchMock.mock.calls.slice(1)) {
      expect(call[0]).toBe("/api/v1/console/sources/reconcile");
      expect(call[1]).toEqual(expect.objectContaining({ method: "POST" }));
      expect(call[1]?.headers).not.toHaveProperty("X-Console-CSRF");
    }
  });

  it("preserves reauthentication and does not retry when CSRF restoration is unauthorized", async () => {
    vi.resetModules();
    const afterReload = await import("@/lib/api");
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      meta: makeMeta({ correlation_id: "corr-csrf-401" }),
      error: { code: "UNAUTHENTICATED", message: "Viewer session required" },
    }), { status: 401, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(afterReload.consoleApi.reconcile()).rejects.toMatchObject({
      status: 401,
      correlationId: "corr-csrf-401",
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/console/auth/csrf",
      expect.objectContaining({ method: "GET", credentials: "same-origin" }),
    );
  });

  it("surfaces sanitized upstream errors with their correlation ID", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            meta: makeMeta({ correlation_id: "corr-unavailable" }),
            error: { code: "UPSTREAM_UNAVAILABLE", message: "token=secret upstream unavailable" },
          }),
          { status: 503, headers: { "content-type": "application/json" } },
        ),
      ),
    );

    const request = getConsole("/sources");
    await expect(request).rejects.toMatchObject({
      name: "ConsoleApiError",
      status: 503,
      correlationId: "corr-unavailable",
    });
    await request.catch((error: ConsoleApiError) => {
      expect(error.message).not.toContain("secret");
    });
  });
});
