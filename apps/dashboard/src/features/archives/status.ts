import type { ArchiveStatus, ArchiveStatusKind } from "@/domain/archive";
import type { Tone } from "@/ui";

/** Visual tone per archive status — the single mapping used everywhere. */
export function archiveStatusTone(kind: ArchiveStatusKind): Tone {
  switch (kind) {
    case "running":
      return "success";
    case "deploying":
      return "info";
    case "error":
      return "danger";
    case "destroying":
      return "warning";
    case "stopped":
      return "neutral";
  }
}

export function archiveStatusInFlight(status: ArchiveStatus): boolean {
  return status.kind === "deploying" || status.kind === "destroying";
}
