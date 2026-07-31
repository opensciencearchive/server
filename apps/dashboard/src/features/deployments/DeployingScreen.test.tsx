import { screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MockAmacrinService } from "@/api/amacrin/mock";
import { ApiError } from "@/api/http/errors";
import type { Archive } from "@/domain/archive";
import type { Deployment, DeploymentStatus } from "@/domain/deployment";
import { makeTestServices, renderWithProviders } from "@/test/render";
import { mockRouter } from "@/test/router-mock";

import { DeployingScreen } from "./DeployingScreen";

const ARCHIVE: Archive = {
  id: "arch_1",
  organisationId: "org_1",
  name: "Alpine Lab",
  slug: "alpine",
  domain: "alpine.amacr.in",
  status: { kind: "deploying" },
  orcidAdmins: [],
  deploymentConfig: null,
  createdAt: new Date(0),
  updatedAt: new Date(0),
};

function deployment(status: DeploymentStatus): Deployment {
  return {
    id: "dep_1",
    archiveId: "arch_1",
    provider: "aws_eks",
    status,
    osaVersion: null,
    startedAt: new Date(0),
  };
}

const notFound = () =>
  new ApiError({ status: 404, code: "not_found", message: "archive not found" });

function servicesWith(opts: {
  archive?: Archive | ApiError;
  status: DeploymentStatus | ApiError;
}) {
  const amacrin = new MockAmacrinService();
  const archive = opts.archive ?? ARCHIVE;
  if (archive instanceof ApiError) {
    vi.spyOn(amacrin, "getArchive").mockRejectedValue(archive);
  } else {
    vi.spyOn(amacrin, "getArchive").mockResolvedValue(archive);
  }
  if (opts.status instanceof ApiError) {
    vi.spyOn(amacrin, "getDeploymentStatus").mockRejectedValue(opts.status);
  } else {
    vi.spyOn(amacrin, "getDeploymentStatus").mockResolvedValue(deployment(opts.status));
  }
  return makeTestServices({ amacrin });
}

describe("DeployingScreen", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("redirects to the archive dashboard once deployment succeeds", async () => {
    const services = servicesWith({
      status: { kind: "succeeded", url: null, completedAt: null },
    });
    renderWithProviders(<DeployingScreen archiveId="arch_1" />, { services });
    await waitFor(() =>
      expect(mockRouter.replace).toHaveBeenCalledWith("/archives/arch_1"),
    );
  });

  it("shows a failure notice linking to the archive, and does not redirect", async () => {
    const services = servicesWith({
      status: { kind: "failed", errorMessage: "quota exceeded", completedAt: null },
    });
    renderWithProviders(<DeployingScreen archiveId="arch_1" />, { services });

    expect(await screen.findByText("Deployment failed")).toBeInTheDocument();
    expect(screen.getByText("quota exceeded")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /view archive/i })).toHaveAttribute(
      "href",
      "/archives/arch_1",
    );
    expect(mockRouter.replace).not.toHaveBeenCalled();
  });

  it("shows the progress card while deploying", async () => {
    const services = servicesWith({ status: { kind: "in_progress" } });
    renderWithProviders(<DeployingScreen archiveId="arch_1" />, { services });

    expect(await screen.findByText("Deploying Alpine Lab")).toBeInTheDocument();
    expect(screen.getByText("alpine.amacr.in")).toBeInTheDocument();
  });

  it("shows a not-found notice for an unknown archive", async () => {
    const services = servicesWith({ archive: notFound(), status: notFound() });
    renderWithProviders(<DeployingScreen archiveId="ghost" />, { services });

    expect(await screen.findByText("Archive not found")).toBeInTheDocument();
  });
});
