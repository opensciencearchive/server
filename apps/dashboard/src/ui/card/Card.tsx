import styles from "./Card.module.css";

export function Card({
  className,
  ...rest
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={[styles.card, className].filter(Boolean).join(" ")}
      {...rest}
    />
  );
}

/** Muted band at the bottom of a card (e.g. status rollups on org cards). */
export function CardFooter({
  className,
  ...rest
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={[styles.footer, className].filter(Boolean).join(" ")}
      {...rest}
    />
  );
}
