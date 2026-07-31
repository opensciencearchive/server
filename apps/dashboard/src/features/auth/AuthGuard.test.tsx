import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { AuthGuard } from "./AuthGuard";

describe("AuthGuard", () => {
  it("renders children (session is enforced by middleware, not client-side)", () => {
    renderWithProviders(
      <AuthGuard>
        <p>protected content</p>
      </AuthGuard>,
    );
    expect(screen.getByText("protected content")).toBeInTheDocument();
  });
});
