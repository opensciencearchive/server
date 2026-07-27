/**
 * next/navigation mock for Vitest — installed globally in test/setup.ts.
 * Tests steer it with setMockPath/setMockParams and assert on mockRouter.
 */
import { vi } from "vitest";

export const mockRouter = {
  push: vi.fn(),
  replace: vi.fn(),
  back: vi.fn(),
  forward: vi.fn(),
  prefetch: vi.fn(),
  refresh: vi.fn(),
};

let currentPath = "/";
let currentSearch = new URLSearchParams();
let currentParams: Record<string, string> = {};

export function setMockPath(path: string, search = ""): void {
  currentPath = path;
  currentSearch = new URLSearchParams(search);
}

export function setMockParams(params: Record<string, string>): void {
  currentParams = params;
}

export function resetRouterMock(): void {
  mockRouter.push.mockReset();
  mockRouter.replace.mockReset();
  mockRouter.back.mockReset();
  mockRouter.forward.mockReset();
  mockRouter.prefetch.mockReset();
  mockRouter.refresh.mockReset();
  currentPath = "/";
  currentSearch = new URLSearchParams();
  currentParams = {};
}

export const navigationModuleMock = {
  useRouter: () => mockRouter,
  usePathname: () => currentPath,
  useSearchParams: () => currentSearch,
  useParams: () => currentParams,
  redirect: vi.fn(),
  notFound: vi.fn(),
};
