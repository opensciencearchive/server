'use client';

import { useAuth } from '@/hooks/useAuth';
import styles from './LoginButton.module.css';

interface LoginButtonProps {
  className?: string;
}

export function LoginButton({ className }: LoginButtonProps) {
  const { login, isLoading } = useAuth();

  if (isLoading) {
    return (
      <button className={`${styles.button} ${className || ''}`} disabled>
        Loading...
      </button>
    );
  }

  return (
    <button className={`${styles.button} ${className || ''}`} onClick={login}>
      <OrcidIcon />
      Sign in with ORCiD
    </button>
  );
}

function OrcidIcon() {
  return (
    <svg
      className={styles.icon}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <circle cx="12" cy="12" r="11" stroke="currentColor" strokeWidth="2" />
      <text
        x="12"
        y="17"
        textAnchor="middle"
        fontSize="14"
        fontWeight="bold"
        fill="currentColor"
      >
        iD
      </text>
    </svg>
  );
}
