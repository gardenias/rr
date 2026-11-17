import { render, screen } from "@testing-library/react";
import { EmptyState, ErrorState } from "@/components/console/states";

describe("degraded and empty states", () => {
  it("does not equate empty observations with health", () => {
    render(<EmptyState title="No alerts observed" coverage={82} />);
    expect(screen.getByText("No alerts observed")).toBeVisible();
    expect(screen.getByText("Observation coverage: 82%")).toBeVisible();
  });

  it("makes recovery and correlation evidence available", () => {
    render(
      <ErrorState
        title="Sources unavailable"
        message="Two upstreams timed out"
        correlationId="corr-42"
        recovery="Open Sources to inspect the failing endpoints."
      />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("corr-42");
    expect(screen.getByText(/Open Sources/)).toBeVisible();
  });
});
