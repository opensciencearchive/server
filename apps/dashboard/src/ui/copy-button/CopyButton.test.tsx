import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { CopyButton } from "./CopyButton";

describe("CopyButton", () => {
  it("writes the value to the clipboard and confirms", async () => {
    const user = userEvent.setup();
    render(<CopyButton value="cortex-atlas.amacr.in" label="Copy" />);

    await user.click(screen.getByRole("button", { name: "Copy" }));

    const copied = await window.navigator.clipboard.readText();
    expect(copied).toBe("cortex-atlas.amacr.in");
    expect(await screen.findByText("Copied")).toBeInTheDocument();
  });
});
