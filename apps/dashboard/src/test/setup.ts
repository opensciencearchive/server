import "@testing-library/jest-dom/vitest";

import { cleanup } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, vi } from "vitest";

import { server } from "@/mocks/server";

import { navigationModuleMock, resetRouterMock } from "./router-mock";

vi.mock("next/navigation", () => navigationModuleMock);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));

afterEach(() => {
  server.resetHandlers();
  resetRouterMock();
  cleanup();
});

afterAll(() => server.close());
