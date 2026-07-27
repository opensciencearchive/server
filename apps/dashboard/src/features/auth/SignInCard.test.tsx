import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SignInCard } from "./SignInCard";

describe("SignInCard", () => {
  it("offers Google sign-in pointing at the API login route", () => {
    render(<SignInCard apiBaseUrl="https://api.test" error={null} />);
    const link = screen.getByRole("link", { name: /continue with google/i });
    const href = new URL(link.getAttribute("href")!);
    expect(href.origin).toBe("https://api.test");
    expect(href.pathname).toBe("/api/v1/auth/login");
    expect(href.searchParams.get("provider")).toBe("google");
    expect(href.searchParams.get("redirect_uri")).toBeTruthy();
  });

  it("renders the failure state with a retry path when an error code is present", () => {
    render(<SignInCard apiBaseUrl="https://api.test" error="provider_error" />);
    expect(screen.getByText(/sign-in didn't complete/i)).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: /try again/i }),
    ).toBeInTheDocument();
  });
});
