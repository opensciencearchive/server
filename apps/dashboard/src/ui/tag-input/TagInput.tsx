"use client";

import { useState } from "react";

import styles from "./TagInput.module.css";

export interface TagInputProps {
  values: string[];
  onChange: (values: string[]) => void;
  label: string;
  placeholder?: string;
  id?: string;
  describedBy?: string;
  invalid?: boolean;
}

/** Chip input: Enter/comma adds, Backspace on empty removes the last. */
export function TagInput({
  values,
  onChange,
  label,
  placeholder,
  id,
  describedBy,
  invalid,
}: TagInputProps) {
  const [draft, setDraft] = useState("");

  const commit = () => {
    const value = draft.trim();
    setDraft("");
    if (value && !values.includes(value)) onChange([...values, value]);
  };

  const onKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter" || event.key === ",") {
      event.preventDefault();
      commit();
    } else if (event.key === "Backspace" && draft === "" && values.length > 0) {
      onChange(values.slice(0, -1));
    }
  };

  return (
    <div className={[styles.box, invalid ? styles.invalid : ""].join(" ")}>
      {values.map((value) => (
        <span key={value} className={styles.tag}>
          {value}
          <button
            type="button"
            className={styles.remove}
            aria-label={`Remove ${value}`}
            onClick={() => onChange(values.filter((v) => v !== value))}
          >
            ×
          </button>
        </span>
      ))}
      <input
        id={id}
        className={styles.input}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={onKeyDown}
        onBlur={commit}
        placeholder={values.length === 0 ? placeholder : undefined}
        aria-label={label}
        aria-describedby={describedBy}
        aria-invalid={invalid || undefined}
      />
    </div>
  );
}
