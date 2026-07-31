"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

import { ApiError } from "@/api/http/errors";
import { Button, Dialog, Field, Input } from "@/ui";

import { useCreateOrganisation } from "./useCreateOrganisation";
import styles from "./CreateOrgDialog.module.css";

export function CreateOrgDialog({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const [name, setName] = useState("");
  const create = useCreateOrganisation();
  const router = useRouter();

  const close = () => {
    setName("");
    create.reset();
    onClose();
  };

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    create.mutate(
      { name: trimmed },
      {
        onSuccess: ({ created, sessionRefreshed }) => {
          close();
          // The org is committed either way. If the session refreshed, its new
          // claims let us open the org; if not, the session is dead — re-auth,
          // and the org surfaces on the way back in.
          if (sessionRefreshed) {
            router.push(`/organisations/${created.id}`);
          } else {
            router.replace("/sign-in");
          }
        },
      },
    );
  };

  const errorMessage =
    create.error instanceof ApiError
      ? create.error.message
      : create.error
        ? "Something went wrong — try again."
        : undefined;

  return (
    <Dialog open={open} onClose={close} title="New organisation">
      <form onSubmit={submit} className={styles.form}>
        <Field
          label="Name"
          help="You become the Owner. Members are added by Amacrin support in v1."
          error={errorMessage}
        >
          {({ id, describedBy, invalid }) => (
            <Input
              id={id}
              aria-describedby={describedBy}
              invalid={invalid}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Summit Lab"
              autoFocus
            />
          )}
        </Field>
        <div className={styles.actions}>
          <Button
            variant="primary"
            type="submit"
            disabled={create.isPending || name.trim() === ""}
          >
            {create.isPending ? "Creating…" : "Create organisation"}
          </Button>
          <Button variant="ghost" onClick={close}>
            Cancel
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
