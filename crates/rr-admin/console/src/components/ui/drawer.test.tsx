import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Drawer } from "@/components/ui/drawer";

describe("drawer keyboard behavior", () => {
  it("contains focus, closes on Escape, and restores trigger focus", async () => {
    const user = userEvent.setup();
    render(
      <Drawer
        trigger={<button>Open node</button>}
        title="Node 1"
        description="Observed node evidence"
      >
        <button>First action</button>
        <button>Last action</button>
      </Drawer>,
    );

    const trigger = screen.getByRole("button", { name: "Open node" });
    await user.click(trigger);
    const dialog = screen.getByRole("dialog", { name: "Node 1" });
    expect(dialog).toBeVisible();

    for (let index = 0; index < 8; index += 1) {
      await user.tab();
      expect(dialog.contains(document.activeElement)).toBe(true);
    }

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });
});
