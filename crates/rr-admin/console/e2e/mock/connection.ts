import { expect, type Page } from "@playwright/test";

export const CLUSTER_A = "cluster-a";
export const CLUSTER_B = "cluster-b";

export function clusterUrl(path: string, connectionId: string): string {
  const [pathname, search = ""] = path.split("?");
  const params = new URLSearchParams(search);
  params.set("connection", connectionId);
  return `${pathname}?${params.toString()}`;
}

export async function seedConnection(
  page: Page,
  id: string,
  name = id === CLUSTER_A ? "Cluster A" : "Cluster B",
  status: "CONNECTED" | "UNREACHABLE" | "DISCONNECTED" = "CONNECTED",
) {
  const response = await page.request.post("/__mock/connections/seed", {
    data: { id, name, status },
  });
  expect(response.status()).toBe(204);
}

export async function seedDefaultClusters(page: Page) {
  await seedConnection(page, CLUSTER_A);
  await seedConnection(page, CLUSTER_B);
}
