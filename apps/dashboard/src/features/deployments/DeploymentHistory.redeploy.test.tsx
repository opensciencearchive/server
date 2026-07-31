import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { DeploymentHistory } from "./DeploymentHistory";
import { DeploymentPanel } from "./DeploymentPanel";

const ARCHIVE = "arch_a1p1n3c11m";

function bodyRowCount(table: HTMLElement): number {
  return within(table).getAllByRole("row").length - 1; // minus the header
}

describe("DeploymentHistory after a redeploy", () => {
  it("picks up the new deployment without waiting for an unrelated refetch", async () => {
    const user = userEvent.setup();
    renderWithProviders(
      <>
        <DeploymentPanel archiveId={ARCHIVE} />
        <DeploymentHistory archiveId={ARCHIVE} />
      </>,
    );

    const table = await screen.findByRole("table");
    expect(bodyRowCount(table)).toBe(1);

    await user.click(await screen.findByRole("button", { name: /redeploy/i }));

    await waitFor(() =>
      expect(bodyRowCount(screen.getByRole("table"))).toBe(2),
    );
  });
});
