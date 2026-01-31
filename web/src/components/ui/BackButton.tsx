'use client';

import { useRouter } from 'next/navigation';
import styles from './BackButton.module.css';

interface BackButtonProps {
  fallbackHref?: string;
  children: React.ReactNode;
}

export function BackButton({ fallbackHref = '/', children }: BackButtonProps) {
  const router = useRouter();

  const handleClick = () => {
    if (window.history.length > 1) {
      router.back();
    } else {
      router.push(fallbackHref);
    }
  };

  return (
    <button onClick={handleClick} className={styles.button}>
      {children}
    </button>
  );
}
