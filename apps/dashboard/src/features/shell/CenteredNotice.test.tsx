import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { CenteredNotice } from "./CenteredNotice";

describe("CenteredNotice", () => {
  it("renders title, description and an action link", () => {
    render(
      <CenteredNotice
        title="Archive not found"
        description="It's gone."
        actionHref="/"
        actionLabel="Back to archives"
      />,
    );
    expect(screen.getByText("Archive not found")).toBeInTheDocument();
    expect(screen.getByText("It's gone.")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Back to archives" }),
    ).toHaveAttribute("href", "/");
  });

  it("shows a danger glyph for the danger tone", () => {
    render(<CenteredNotice title="Deployment failed" tone="danger" />);
    expect(screen.getByText("Deployment failed")).toBeInTheDocument();
    expect(screen.getByText("!")).toBeInTheDocument();
  });
});
