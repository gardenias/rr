import { RealCluster } from "./harness";

export default async function globalSetup() {
  const cluster = await RealCluster.start();
  return async () => cluster.stop();
}
