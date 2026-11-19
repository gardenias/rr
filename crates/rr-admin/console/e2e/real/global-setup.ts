import { RealHarness } from "./harness";

export default async function globalSetup() {
  const harness = await RealHarness.start();
  return async () => harness.stop();
}
