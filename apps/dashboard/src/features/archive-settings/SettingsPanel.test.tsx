import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";
import { mockRouter } from "@/test/router-mock";

import { SettingsPanel } from "./SettingsPanel";

describe("SettingsPanel", () => {
  it("lets an Owner destroy: dialog gates on the typed slug, then navigates", async () => {
    const user = userEvent.setup();
    const services = makeTestServices();
    const destroySpy = vi.spyOn(services.amacrin, "destroyArchive");

    // arch_sky1mag3ry is in Personal (org_p3rs0na1aa), where the caller is Owner.
    renderWithProviders(<SettingsPanel archiveId="arch_sky1mag3ry" />, {
      services,
    });

    const destroyButton = await screen.findByRole("button", {
      name: /destroy archive/i,
    });
    expect(destroyButton).toBeEnabled();
    await user.click(destroyButton);

    const dialog = await screen.findByRole("dialog");
    expect(
      within(dialog).getByText(/Destroy Sky imagery scratchpad\?/i),
    ).toBeInTheDocument();

    const confirmButton = within(dialog).getByRole("button", {
      name: /destroy permanently/i,
    });
    expect(confirmButton).toBeDisabled();

    const confirmInput = within(dialog).getByLabelText(/sky-imagery-scratch/i);
    await user.type(confirmInput, "sky-imagery-wrong");
    expect(confirmButton).toBeDisabled();

    await user.clear(confirmInput);
    await user.type(confirmInput, "sky-imagery-scratch");
    expect(confirmButton).toBeEnabled();

    await user.click(confirmButton);

    await waitFor(() =>
      expect(destroySpy).toHaveBeenCalledWith("arch_sky1mag3ry"),
    );
    await waitFor(() =>
      expect(mockRouter.push).toHaveBeenCalledWith(
        "/organisations/org_p3rs0na1aa",
      ),
    );
  });

  it("disables destroy for a non-Owner with the owner-only reason", async () => {
    // arch_a1p1n3c11m is in Summit Lab (org_7f3k2mq9x1), where the caller is Admin.
    renderWithProviders(<SettingsPanel archiveId="arch_a1p1n3c11m" />);

    const destroyButton = await screen.findByRole("button", {
      name: /destroy archive/i,
    });
    expect(destroyButton).toBeDisabled();
    expect(
      screen.getByText(
        /Only an organisation Owner can destroy this archive\./i,
      ),
    ).toBeInTheDocument();
  });

  it("disables redeploy and destroy while deploying, with reasons", async () => {
    // arch_d0pp13rswp is deploying.
    renderWithProviders(<SettingsPanel archiveId="arch_d0pp13rswp" />);

    await screen.findByRole("button", { name: /destroy archive/i });

    const redeployButton = screen.getByRole("button", {
      name: /redeploy archive/i,
    });
    const rotateButton = screen.getByRole("button", {
      name: /rotate and redeploy/i,
    });
    const destroyButton = screen.getByRole("button", {
      name: /destroy archive/i,
    });

    expect(redeployButton).toBeDisabled();
    expect(rotateButton).toBeDisabled();
    expect(destroyButton).toBeDisabled();

    expect(
      screen.getAllByText(/A deployment is already in progress\./i).length,
    ).toBeGreaterThan(0);
  });

  it("rotation submits config auth with the ArchiveAuthInput shape", async () => {
    const user = userEvent.setup();
    const services = makeTestServices();
    const deploySpy = vi.spyOn(services.amacrin, "deploy");

    // arch_sky1mag3ry is running (rotation enabled) and owned by the caller.
    renderWithProviders(<SettingsPanel archiveId="arch_sky1mag3ry" />, {
      services,
    });

    await screen.findByRole("button", { name: /rotate and redeploy/i });

    await user.type(
      screen.getByLabelText(/ORCID client ID/i),
      "APP-ABC123DEF456",
    );
    await user.type(
      screen.getByLabelText(/ORCID client secret/i),
      "super-secret-value",
    );

    const admins = screen.getByLabelText(/Administrator ORCID iDs/i);
    await user.type(admins, "0000-0002-1825-0097{enter}");

    await user.click(
      screen.getByRole("button", { name: /rotate and redeploy/i }),
    );

    await waitFor(() =>
      expect(deploySpy).toHaveBeenCalledWith("arch_sky1mag3ry", {
        orcid: {
          clientId: "APP-ABC123DEF456",
          clientSecret: "super-secret-value",
        },
        adminOrcidIds: ["0000-0002-1825-0097"],
      }),
    );

    // Success confirmation appears and the form clears.
    expect(
      await screen.findByText(/redeploy started/i),
    ).toBeInTheDocument();
  });

  it("plain redeploy reuses stored credentials (no auth argument)", async () => {
    const user = userEvent.setup();
    const services = makeTestServices();
    const deploySpy = vi.spyOn(services.amacrin, "deploy");

    renderWithProviders(<SettingsPanel archiveId="arch_sky1mag3ry" />, {
      services,
    });

    await user.click(
      await screen.findByRole("button", { name: /redeploy archive/i }),
    );

    await waitFor(() =>
      expect(deploySpy).toHaveBeenCalledWith("arch_sky1mag3ry", undefined),
    );
  });
});
