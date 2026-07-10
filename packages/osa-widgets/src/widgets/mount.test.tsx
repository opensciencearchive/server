/**
 * Standalone opening (no embedding host): window.parent === window, so the
 * bootstrap must explain itself instead of dispatching ui/initialize into an
 * echo loop that our own endpoint answers with -32601.
 */

import { act } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { mountWidget } from "./mount";

afterEach(() => {
  document.body.innerHTML = "";
});

it("renders the standalone notice and never connects when there is no parent frame", async () => {
  // jsdom's window.parent === window — exactly the standalone situation.
  const container = document.createElement("div");
  container.id = "root";
  document.body.appendChild(container);

  const render = vi.fn();
  await act(async () => {
    mountWidget(render);
  });

  expect(container.textContent).toContain("MCP Apps host");
  expect(container.textContent).toContain("/mcp");
  expect(render).not.toHaveBeenCalled();
});
