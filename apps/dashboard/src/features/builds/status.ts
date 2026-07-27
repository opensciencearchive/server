import type { BuildStatusKind, ComponentBuildStatus } from "@/domain/build";
import type { Tone } from "@/ui";

export function buildStatusTone(kind: BuildStatusKind): Tone {
  switch (kind) {
    case "published":
      return "success";
    case "queued":
    case "building":
    case "publishing":
      return "info";
    case "build_failed":
    case "publish_failed":
      return "danger";
    case "cancelled":
      return "neutral";
  }
}

export function buildStatusInFlight(kind: BuildStatusKind): boolean {
  return kind === "queued" || kind === "building" || kind === "publishing";
}

export function componentStatusTone(status: ComponentBuildStatus): Tone {
  switch (status.kind) {
    case "succeeded":
      return "success";
    case "pending":
    case "building":
      return "info";
    case "failed":
      return "danger";
    case "cancelled":
      return "neutral";
  }
}
