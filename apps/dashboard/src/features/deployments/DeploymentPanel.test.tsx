import { screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { DeploymentPanel } from "./DeploymentPanel";

describe("DeploymentPanel", () => {
  it("shows Running with a redeploy button for a running archive", async () => {
    renderWithProviders(<DeploymentPanel archiveId="arch_c0r73xa71a" />);

    expect(await screen.findByText("Running")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /redeploy/i }),
    ).toBeInTheDocument();
    // "From" ref comes from Mocked deployment history → sample-data affordance.
    expect(await screen.findByText("geo-rnaseq-v2@4c1e77b")).toBeInTheDocument();
  });

  it("renders the provisioning timeline and the close-tab note while deploying", async () => {
    renderWithProviders(<DeploymentPanel archiveId="arch_tce11cr1sp" />);

    expect(await screen.findByText("Accepted")).toBeInTheDocument();
    expect(
      screen.getByText("Provisioning infrastructure"),
    ).toBeInTheDocument();
    expect(await screen.findByText(/you can close this tab/i)).toBeInTheDocument();
  });

  it("renders the verbatim error message and a redeploy button when failed", async () => {
    renderWithProviders(<DeploymentPanel archiveId="arch_pr073om1cs" />);

    expect(
      await screen.findByText(
        "provisioning failed: PersistentVolumeClaim pending after 600s — storage class 'gp3' quota exceeded in eu-west-1",
      ),
    ).toBeInTheDocument();
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: /redeploy/i }),
      ).toBeInTheDocument(),
    );
    expect(screen.getByText(/settings/i)).toBeInTheDocument();
  });
});
