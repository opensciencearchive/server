import { screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SessionRefresher } from "@/api/auth/refresher";
import { TokenStore } from "@/api/auth/token-store";
import { ApiError } from "@/api/http/errors";
import { makeTestServices, renderWithProviders } from "@/test/render";
import { mockRouter } from "@/test/router-mock";

import { AuthGuard } from "./AuthGuard";

describe("AuthGuard", () => {
  it("renders children once the bootstrap refresh restores a session", async () => {
    renderWithProviders(
      <AuthGuard>
        <p>protected content</p>
      </AuthGuard>,
    );
    expect(await screen.findByText("protected content")).toBeInTheDocument();
    expect(mockRouter.replace).not.toHaveBeenCalled();
  });

  it("redirects to sign-in when the refresh cookie is dead", async () => {
    const tokenStore = new TokenStore();
    const refresher = new SessionRefresher({
      store: tokenStore,
      refreshFn: () =>
        Promise.reject(
          new ApiError({
            status: 401,
            code: "unauthorized",
            message: "session expired",
          }),
        ),
    });
    renderWithProviders(
      <AuthGuard>
        <p>protected content</p>
      </AuthGuard>,
      { services: makeTestServices({ tokenStore, refresher }) },
    );

    await waitFor(() => {
      expect(mockRouter.replace).toHaveBeenCalledWith("/sign-in");
    });
    expect(screen.queryByText("protected content")).not.toBeInTheDocument();
  });
});
