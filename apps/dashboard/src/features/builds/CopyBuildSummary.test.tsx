import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { Build } from "@/domain/build";

import { CopyBuildSummary } from "./CopyBuildSummary";

const publishedBuild: Build = {
  id: "build_8f3a91c2e5",
  archiveId: "arch_a1p1n3c11m",
  conventionSlug: "geo-rnaseq-v2",
  conventionRef: "4c1e77b",
  status: { kind: "published", publishedAt: new Date("2026-07-25T14:05:12Z") },
  components: [
    {
      kind: "ingester",
      name: "ghcn-ingester",
      status: {
        kind: "succeeded",
        imageRef: "ecr/ghcn-ingester:build_8f3a91c2e5",
        digest: "sha256:aaaa1111bbbb2222cccc3333",
      },
      sourceRef: "ingesters/geo_ingester.py",
    },
  ],
  createdAt: new Date("2026-07-25T14:01:30Z"),
  updatedAt: new Date("2026-07-25T14:05:12Z"),
};

const failedBuild: Build = {
  ...publishedBuild,
  id: "build_f4a1e6d902",
  status: {
    kind: "build_failed",
    errorMessage: "ImportError: cannot import name 'SchemaRegistry'",
  },
  components: [
    {
      kind: "hook",
      name: "normalise-units",
      status: {
        kind: "failed",
        errorMessage: "ImportError: cannot import name 'SchemaRegistry'",
      },
      sourceRef: "hooks/normalise_counts.py",
    },
  ],
};

describe("CopyBuildSummary", () => {
  it("copies a plain-text summary with the build id, digest and archive id", async () => {
    const user = userEvent.setup();
    render(<CopyBuildSummary build={publishedBuild} />);

    await user.click(screen.getByRole("button"));

    const text = await window.navigator.clipboard.readText();
    expect(text).toContain("build_8f3a91c2e5");
    expect(text).toContain("arch_a1p1n3c11m");
    expect(text).toContain("geo-rnaseq-v2");
    expect(text).toContain("4c1e77b");
    expect(text).toContain("published");
    expect(text).toContain("sha256:aaaa1111bbbb2222cccc3333");
    expect(text).toContain("ghcn-ingester");
  });

  it("includes the verbatim parent error and per-component error for a failed build", async () => {
    const user = userEvent.setup();
    render(<CopyBuildSummary build={failedBuild} />);

    await user.click(screen.getByRole("button"));

    const text = await window.navigator.clipboard.readText();
    expect(text).toContain("build_failed");
    expect(text).toContain("ImportError: cannot import name 'SchemaRegistry'");
    expect(text).toContain("normalise-units");
  });
});
