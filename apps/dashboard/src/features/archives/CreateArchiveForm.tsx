"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { useRouter } from "next/navigation";
import { useEffect } from "react";
import { Controller, useForm, useWatch } from "react-hook-form";
import { z } from "zod";

import { ApiError, SlugTakenError } from "@/api/http/errors";
import { SLUG_PATTERN } from "@/domain/archive";
import { roleAtLeast } from "@/domain/organisation";
import { useSession } from "@/features/auth/useSession";
import { Badge, Button, Field, Input, Select, TagInput } from "@/ui";

import { useCreateArchive } from "./useCreateArchive";
import styles from "./CreateArchiveForm.module.css";

const schema = z.object({
  orgId: z.string().min(1, "Choose an organisation."),
  name: z.string().trim().min(1, "Give the archive a display name."),
  slug: z
    .string()
    .regex(
      SLUG_PATTERN,
      "3–63 chars, lowercase a–z, 0–9 and hyphens, not at the ends.",
    ),
  clientId: z.string().trim().min(1, "Paste the ORCID client ID."),
  clientSecret: z.string().min(1, "Paste the ORCID client secret."),
  adminOrcidIds: z.array(z.string()),
});

type FormValues = z.infer<typeof schema>;

/** Convenience-only alternatives — resubmission is the only proof. */
function slugSuggestions(slug: string): string[] {
  return [`${slug}-lab`, `${slug}-2026`, `${slug}-archive`];
}

export function CreateArchiveForm({
  defaultOrgId,
}: {
  defaultOrgId?: string;
}) {
  const router = useRouter();
  const session = useSession();
  const create = useCreateArchive();

  const eligibleOrgs = (session.data?.organisations ?? []).filter(
    (org) => org.role !== null && roleAtLeast(org.role, "admin"),
  );

  const resolvedDefaultOrg =
    defaultOrgId && eligibleOrgs.some((o) => o.id === defaultOrgId)
      ? defaultOrgId
      : (eligibleOrgs[0]?.id ?? "");

  const {
    control,
    register,
    handleSubmit,
    setValue,
    setError,
    clearErrors,
    getValues,
    formState: { errors },
  } = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: {
      orgId: "",
      name: "",
      slug: "",
      clientId: "",
      clientSecret: "",
      adminOrcidIds: [],
    },
  });

  // Once the session (and its eligible orgs) loads, seed the select — but
  // never clobber a choice the user has already made.
  useEffect(() => {
    if (resolvedDefaultOrg && getValues("orgId") === "") {
      setValue("orgId", resolvedDefaultOrg);
    }
  }, [resolvedDefaultOrg, getValues, setValue]);

  const watchedOrgId = useWatch({ control, name: "orgId" });
  const slug = useWatch({ control, name: "slug" });
  const selectedOrg = eligibleOrgs.find((o) => o.id === watchedOrgId);
  const slugFormatValid = SLUG_PATTERN.test(slug);

  const onSubmit = handleSubmit((values) => {
    create.reset();
    create.mutate(
      {
        orgId: values.orgId,
        input: {
          name: values.name.trim(),
          slug: values.slug,
          orcid: {
            clientId: values.clientId.trim(),
            clientSecret: values.clientSecret,
          },
          adminOrcidIds: values.adminOrcidIds,
        },
      },
      {
        onSuccess: ({ archive }) => {
          // The archive is created but still deploying — send the operator to a
          // centered progress screen that polls and opens the dashboard when it
          // succeeds, rather than a half-empty dashboard.
          router.push(`/deploying/${archive.id}`);
        },
        onError: (error) => {
          if (error instanceof SlugTakenError) {
            setError("slug", { type: "server", message: error.message });
          }
        },
      },
    );
  });

  const slugTaken =
    create.error instanceof SlugTakenError ? create.error.slug : null;

  // Generic (non-slug) API failure → form-level box.
  const formError =
    create.error && !(create.error instanceof SlugTakenError)
      ? create.error instanceof ApiError
        ? {
            message: create.error.message,
            requestId: create.error.requestId,
          }
        : { message: "Something went wrong — try again.", requestId: null }
      : null;

  const cancelHref = selectedOrg
    ? `/organisations/${selectedOrg.id}`
    : "/organisations";

  return (
    <form className={styles.layout} onSubmit={onSubmit} noValidate>
      <div className={styles.main}>
        <div className={styles.intro}>
          <h1>New archive</h1>
          <p className={styles.introText}>
            Creating deploys immediately. Expect roughly three minutes before
            the URL answers.
          </p>
        </div>

        <div className={styles.card}>
          <Field
            label="Organisation"
            help="Only organisations where you're Admin or Owner are listed."
            error={errors.orgId?.message}
          >
            {({ id, describedBy, invalid }) => (
              <div className={styles.pair} style={{ gridTemplateColumns: "1fr auto", alignItems: "center" }}>
                <Select
                  id={id}
                  aria-describedby={describedBy}
                  aria-invalid={invalid || undefined}
                  {...register("orgId")}
                >
                  {eligibleOrgs.map((org) => (
                    <option key={org.id} value={org.id}>
                      {org.name}
                    </option>
                  ))}
                </Select>
                {selectedOrg?.role && (
                  <Badge outline tone="accent">
                    {selectedOrg.role}
                  </Badge>
                )}
              </div>
            )}
          </Field>

          <Field
            label="Display name"
            help="Shown in the dashboard. Doesn't need to be unique — change it any time."
            error={errors.name?.message}
          >
            {({ id, describedBy, invalid }) => (
              <Input
                id={id}
                aria-describedby={describedBy}
                invalid={invalid}
                placeholder="e.g. Alpine climate network"
                {...register("name")}
              />
            )}
          </Field>

          <Field
            label="Subdomain"
            hint="3-63 chars · a-z 0-9 -"
            error={errors.slug?.message}
            help={
              slugFormatValid ? undefined : "Becomes {slug}.amacr.in — globally unique."
            }
          >
            {({ id, describedBy, invalid }) => (
              <>
                <div
                  className={[
                    styles.subdomain,
                    invalid ? styles.invalid : "",
                  ].join(" ")}
                >
                  <input
                    id={id}
                    className={styles.subdomainInput}
                    aria-describedby={describedBy}
                    aria-invalid={invalid || undefined}
                    autoCapitalize="off"
                    autoCorrect="off"
                    spellCheck={false}
                    placeholder="alpine-climate"
                    {...register("slug", {
                      onChange: () => {
                        if (errors.slug) clearErrors("slug");
                        if (create.error) create.reset();
                      },
                    })}
                  />
                  <span className={styles.suffix}>.amacr.in</span>
                </div>
                {slugFormatValid && !invalid && (
                  <p className={styles.available}>
                    ✓ Looks available — confirmed when you create.
                  </p>
                )}
              </>
            )}
          </Field>

          {slugTaken && (
            <div className={styles.suggestions}>
              <span className={styles.suggestionsLabel}>Try instead</span>
              <div className={styles.chips}>
                {slugSuggestions(slugTaken).map((candidate) => (
                  <button
                    key={candidate}
                    type="button"
                    className={styles.chip}
                    onClick={() => {
                      clearErrors("slug");
                      create.reset();
                      setValue("slug", candidate, {
                        shouldValidate: false,
                        shouldDirty: true,
                      });
                    }}
                  >
                    {candidate}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className={styles.card}>
          <div className={styles.sectionHead}>
            <h4>Archive sign-in</h4>
            <p className={styles.sectionText}>
              Depositors sign in to your archive with ORCID. Register an ORCID
              public API client, then paste its credentials here.
            </p>
          </div>

          <div className={styles.pair}>
            <Field label="ORCID client ID" error={errors.clientId?.message}>
              {({ id, describedBy, invalid }) => (
                <Input
                  id={id}
                  mono
                  aria-describedby={describedBy}
                  invalid={invalid}
                  placeholder="APP-…"
                  {...register("clientId")}
                />
              )}
            </Field>
            <Field
              label="ORCID client secret"
              error={errors.clientSecret?.message}
            >
              {({ id, describedBy, invalid }) => (
                <Input
                  id={id}
                  mono
                  type="password"
                  aria-describedby={describedBy}
                  invalid={invalid}
                  {...register("clientSecret")}
                />
              )}
            </Field>
          </div>

          <div className={styles.note}>
            <span className={styles.noteLabel}>NOTE</span>
            <span className={styles.noteText}>
              The secret is sealed on write and never shown again — not here, not
              in the API. To change it later, rotate it in archive settings.
            </span>
          </div>

          <Field
            label="Administrator ORCID iDs"
            optional
            help="Admins can curate records inside the archive. You can add them later."
          >
            {({ id, describedBy }) => (
              <Controller
                control={control}
                name="adminOrcidIds"
                render={({ field }) => (
                  <TagInput
                    id={id}
                    describedBy={describedBy}
                    label="Administrator ORCID iDs"
                    placeholder="Add an iD…"
                    values={field.value}
                    onChange={field.onChange}
                  />
                )}
              />
            )}
          </Field>
        </div>

        {formError && (
          <div className={styles.formError} role="alert">
            {formError.message}
            {formError.requestId && (
              <span className={styles.requestId}>
                Request ID: {formError.requestId}
              </span>
            )}
          </div>
        )}

        <div className={styles.actions}>
          <Button variant="primary" type="submit" disabled={create.isPending}>
            {create.isPending ? "Creating…" : "Create and deploy"}
          </Button>
          <Button
            variant="ghost"
            type="button"
            onClick={() => router.push(cancelHref)}
          >
            Cancel
          </Button>
        </div>
      </div>

      <aside className={styles.aside}>
        <div className={styles.asidePanel}>
          <span className={styles.asideLabel}>What happens next</span>
          <ol className={styles.steps}>
            <li>Compute, database and image registry are provisioned.</li>
            <li>The archive comes up at your subdomain.</li>
            <li>
              You push conventions and data with <code>osa deploy</code>.
            </li>
          </ol>
        </div>
        <div className={`${styles.asidePanel} ${styles.asidePanelMuted}`}>
          <span className={styles.asideLabel}>On subdomains</span>
          <p className={styles.asideText}>
            Subdomains are global and permanent while the archive exists. Custom
            domains aren&apos;t supported yet.
          </p>
        </div>
      </aside>
    </form>
  );
}
