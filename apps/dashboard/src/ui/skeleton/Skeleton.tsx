import styles from "./Skeleton.module.css";

export function Skeleton({
  width,
  height = "1em",
  className,
}: {
  width?: string | number;
  height?: string | number;
  className?: string;
}) {
  return (
    <span
      className={[styles.skeleton, className].filter(Boolean).join(" ")}
      style={{ width, height }}
      aria-hidden
    />
  );
}
