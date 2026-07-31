import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";
import { mockRouter } from "@/test/router-mock";

import { CreateArchiveForm } from "./CreateArchiveForm";

/** Fill in every field except the subdomain, which each test drives itself. */
async function fillBaseFields(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText(/display name/i), "Alpine climate network");
  await user.type(screen.getByLabelText(/orcid client id/i), "APP-K91F2LQ8XZ40");
  await user.type(screen.getByLabelText(/orcid client secret/i), "s3cr3t-value");
}

describe("CreateArchiveForm", () => {
  it("lists only organisations where the caller is admin or owner", async () => {
    renderWithProviders(<CreateArchiveForm />);

    // Wait for the async session to populate the select options.
    await screen.findByRole("option", { name: "Summit Lab" });

    const select = screen.getByLabelText(/organisation/i) as HTMLSelectElement;
    const labels = within(select)
      .getAllByRole("option")
      .map((o) => o.textContent);

    expect(labels).toContain("Summit Lab"); // admin
    expect(labels).toContain("Personal"); // owner
    expect(labels).not.toContain("Radar Array Consortium"); // member — excluded
  });

  it("blocks submit and shows a field error for an invalid subdomain format", async () => {
    const user = userEvent.setup();
    const services = makeTestServices();
    renderWithProviders(<CreateArchiveForm />, { services });
    await screen.findByRole("option", { name: "Summit Lab" });
    const createSpy = vi.spyOn(services.amacrin, "createArchive");

    await fillBaseFields(user);
    // Uppercase + too short — fails SLUG_PATTERN.
    await user.type(screen.getByLabelText(/subdomain/i), "AB");
    await user.click(screen.getByRole("button", { name: /create and deploy/i }));

    // The field-level error (role=alert) — distinct from the always-on hint.
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent(/lowercase|a.?z/i);

    // Service never called; no navigation.
    expect(createSpy).not.toHaveBeenCalled();
    expect(mockRouter.push).not.toHaveBeenCalled();
  });

  it("shows the availability hint once the subdomain format is valid", async () => {
    const user = userEvent.setup();
    renderWithProviders(<CreateArchiveForm />);
    await screen.findByRole("option", { name: "Summit Lab" });

    await user.type(screen.getByLabelText(/subdomain/i), "new-atlas");

    expect(
      await screen.findByText(/looks available — confirmed when you create/i),
    ).toBeInTheDocument();
  });

  it("submits a valid form to the service and navigates to the new archive", async () => {
    const user = userEvent.setup();
    const services = makeTestServices();
    renderWithProviders(<CreateArchiveForm />, { services });
    await screen.findByRole("option", { name: "Summit Lab" });

    const createSpy = vi.spyOn(services.amacrin, "createArchive");

    await fillBaseFields(user);
    await user.type(screen.getByLabelText(/subdomain/i), "new-atlas");
    await user.click(screen.getByRole("button", { name: /create and deploy/i }));

    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(createSpy).toHaveBeenCalledWith(
      "org_7f3k2mq9x1",
      expect.objectContaining({
        name: "Alpine climate network",
        slug: "new-atlas",
        orcid: { clientId: "APP-K91F2LQ8XZ40", clientSecret: "s3cr3t-value" },
        adminOrcidIds: [],
      }),
    );

    await waitFor(() =>
      expect(mockRouter.push).toHaveBeenCalledWith(
        expect.stringMatching(/^\/deploying\/arch_/),
      ),
    );
  });

  it("surfaces a taken subdomain as a slug field error with suggestion chips", async () => {
    const user = userEvent.setup();
    renderWithProviders(<CreateArchiveForm />);
    await screen.findByRole("option", { name: "Summit Lab" });

    await fillBaseFields(user);
    await user.type(screen.getByLabelText(/subdomain/i), "alpine-climate");
    await user.click(screen.getByRole("button", { name: /create and deploy/i }));

    // Server 409 message rendered on the slug field.
    expect(
      await screen.findByText(/already taken/i),
    ).toBeInTheDocument();

    // Convenience suggestions offered as clickable chips.
    const suggestion = await screen.findByRole("button", {
      name: "alpine-climate-lab",
    });
    expect(suggestion).toBeInTheDocument();
    expect(mockRouter.push).not.toHaveBeenCalled();

    // Clicking a suggestion fills the field.
    await user.click(suggestion);
    expect(screen.getByLabelText(/subdomain/i)).toHaveValue("alpine-climate-lab");
  });

  it("preselects the organisation from defaultOrgId", async () => {
    renderWithProviders(<CreateArchiveForm defaultOrgId="org_p3rs0na1aa" />);

    await screen.findByRole("option", { name: "Personal" });
    const select = screen.getByLabelText(/organisation/i) as HTMLSelectElement;
    await waitFor(() => expect(select.value).toBe("org_p3rs0na1aa"));
  });
});
