/** Typed WidgetHostApi test double shared by widget tests. */

import { vi, type Mock } from "vitest";

import type { WidgetHostApi } from "../protocol/host";

export interface MockHost {
  host: WidgetHostApi;
  callTool: Mock<(name: string, args: object) => Promise<unknown>>;
  updateModelContext: Mock<(text: string) => void>;
}

export function mockHost(): MockHost {
  const callTool = vi.fn<(name: string, args: object) => Promise<unknown>>();
  const updateModelContext = vi.fn<(text: string) => void>();
  const host: WidgetHostApi = {
    // The generic signature narrows per call site; the mock returns unknown.
    callTool: callTool as WidgetHostApi["callTool"],
    updateModelContext,
  };
  return { host, callTool, updateModelContext };
}
