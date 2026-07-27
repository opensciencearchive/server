import { setupWorker } from "msw/browser";

import { handlers } from "./handlers";

/** MSW worker for `msw` mode in the browser (dev against fixtures). */
export const worker = setupWorker(...handlers);

let started: Promise<void> | null = null;

export function ensureMswStarted(): Promise<void> {
  started ??= worker
    .start({ onUnhandledRequest: "bypass" })
    .then(() => undefined);
  return started;
}
