import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SignInCard } from "./SignInCard";

describe("SignInCard", () => {
  it("offers Google sign-in pointing at the BFF sign-in route", () => {
    render(<SignInCard error={null} />);
    const link = screen.getByRole("link", { name: /continue with google/i });
    expect(link.getAttribute("href")).toBe("/api/auth/sign-in");
  });

  it("renders the failure state with a retry path when an error code is present", () => {
    render(<SignInCard error="provider_error" />);
    expect(screen.getByText(/sign-in didn't complete/i)).toBeInTheDocument();
    const retry = screen.getByRole("link", { name: /try again/i });
    expect(retry.getAttribute("href")).toBe("/api/auth/sign-in");
  });
});
