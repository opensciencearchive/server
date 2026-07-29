"use client";

import type { BuildStatus } from "@/domain/build";

import styles from "./BuildPipeline.module.css";

type StepState = "complete" | "active" | "pending" | "failed";

interface Step {
  id: "queued" | "building" | "publishing" | "published";
  label: string;
  state: StepState;
}

/**
 * Derive the 4-step pipeline from the status machine:
 *   queued → building → publishing → published
 * A build_failed marks Building failed; publish_failed marks Publishing failed.
 * (Cancellation is NOT rendered here — the caller shows a neutral banner.)
 */
function derivePipeline(status: BuildStatus, componentCount: number): Step[] {
  const buildingLabel = `Building (${componentCount} component${
    componentCount === 1 ? "" : "s"
  })`;
  const base: Array<Pick<Step, "id" | "label">> = [
    { id: "queued", label: "Queued" },
    { id: "building", label: buildingLabel },
    { id: "publishing", label: "Publishing" },
    { id: "published", label: "Published" },
  ];

  // Index of the step currently reached (for complete/active/pending).
  const stateFor = (id: Step["id"]): StepState => {
    switch (status.kind) {
      case "queued":
        return id === "queued" ? "active" : "pending";
      case "building":
        if (id === "queued") return "complete";
        if (id === "building") return "active";
        return "pending";
      case "publishing":
        if (id === "queued" || id === "building") return "complete";
        if (id === "publishing") return "active";
        return "pending";
      case "published":
        return "complete";
      case "build_failed":
        if (id === "queued") return "complete";
        if (id === "building") return "failed";
        return "pending";
      case "publish_failed":
        if (id === "queued" || id === "building") return "complete";
        if (id === "publishing") return "failed";
        return "pending";
      case "cancelled":
        // Not rendered (banner instead) — treat all as pending defensively.
        return "pending";
    }
  };

  return base.map((s) => ({ ...s, state: stateFor(s.id) }));
}

export function BuildPipeline({
  status,
  componentCount,
}: {
  status: BuildStatus;
  componentCount: number;
}) {
  const steps = derivePipeline(status, componentCount);

  return (
    <div className={styles.pipeline}>
      {steps.map((step, i) => (
        <div key={step.id} className={styles.segment}>
          <div
            className={styles.step}
            data-testid={`pipeline-step-${step.id}`}
            data-state={step.state}
          >
            <span className={[styles.dot, styles[step.state]].join(" ")} aria-hidden>
              {step.state === "complete" && step.id === "published" ? "✓" : ""}
              {step.state === "failed" ? "×" : ""}
            </span>
            <span className={[styles.label, styles[`text-${step.state}`]].join(" ")}>
              {step.state === "failed" ? `${step.label} — failed` : step.label}
            </span>
          </div>
          {i < steps.length - 1 && (
            <span
              className={[
                styles.connector,
                step.state === "complete"
                  ? styles.connectorComplete
                  : step.state === "failed"
                    ? styles.connectorFailed
                    : "",
              ].join(" ")}
              aria-hidden
            />
          )}
        </div>
      ))}
    </div>
  );
}
