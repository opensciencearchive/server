/**
 * Build lifecycle — the explicit state machine from the platform:
 *
 *   queued → building → publishing → published
 *               │            └─────► publish_failed
 *               └──────────────────► build_failed
 *      any non-terminal ───────────► cancelled
 *
 * Success is `published` — never inferred from image refs. Cancellation is
 * data, not failure: the cause rides `cancelledBy`/`cancelReason`.
 */
export type BuildStatus =
  | { kind: "queued" }
  | { kind: "building" }
  | { kind: "publishing" }
  | { kind: "published"; publishedAt: Date }
  | { kind: "build_failed"; errorMessage: string }
  | { kind: "publish_failed"; errorMessage: string }
  | {
      kind: "cancelled";
      cancelledBy: "user" | "system";
      cancelReason: string | null;
    };

export type BuildStatusKind = BuildStatus["kind"];

const TERMINAL_BUILD_KINDS: ReadonlySet<BuildStatusKind> = new Set([
  "published",
  "build_failed",
  "publish_failed",
  "cancelled",
]);

export function isBuildKindTerminal(kind: BuildStatusKind): boolean {
  return TERMINAL_BUILD_KINDS.has(kind);
}

export function isBuildTerminal(status: BuildStatus): boolean {
  return isBuildKindTerminal(status.kind);
}

export type ComponentKind = "hook" | "ingester";

/**
 * A component's image build. A succeeded component always has an image ref;
 * its content digest is the provenance record (immutable, copyable).
 */
export type ComponentBuildStatus =
  | { kind: "pending" }
  | { kind: "building" }
  | { kind: "succeeded"; imageRef: string; digest: string | null }
  | { kind: "failed"; errorMessage: string }
  | { kind: "cancelled" };

export interface ComponentBuild {
  kind: ComponentKind;
  name: string;
  status: ComponentBuildStatus;
  sourceRef: string;
}

/** One deploy attempt of a convention (parent aggregate). */
export interface Build {
  id: string;
  archiveId: string;
  conventionSlug: string;
  conventionRef: string | null;
  status: BuildStatus;
  components: ComponentBuild[];
  createdAt: Date;
  updatedAt: Date;
}
