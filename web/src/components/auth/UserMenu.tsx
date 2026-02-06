'use client';

import { useState } from 'react';
import { useAuth } from '@/hooks/useAuth';
import styles from './UserMenu.module.css';

interface UserMenuProps {
  className?: string;
}

export function UserMenu({ className }: UserMenuProps) {
  const { user, logout, isLoading } = useAuth();
  const [isOpen, setIsOpen] = useState(false);
  const [isLoggingOut, setIsLoggingOut] = useState(false);

  if (isLoading || !user) {
    return null;
  }

  const handleLogout = async () => {
    setIsLoggingOut(true);
    try {
      await logout();
    } finally {
      setIsLoggingOut(false);
      setIsOpen(false);
    }
  };

  return (
    <div className={`${styles.container} ${className || ''}`}>
      <button
        className={styles.trigger}
        onClick={() => setIsOpen(!isOpen)}
        aria-expanded={isOpen}
        aria-haspopup="menu"
      >
        <span className={styles.avatar}>
          {user.displayName?.[0]?.toUpperCase() || 'U'}
        </span>
        <span className={styles.name}>{user.displayName || 'User'}</span>
        <ChevronIcon className={isOpen ? styles.chevronOpen : ''} />
      </button>

      {isOpen && (
        <div className={styles.dropdown} role="menu">
          <div className={styles.userInfo}>
            <span className={styles.displayName}>{user.displayName}</span>
            <span className={styles.orcidId}>
              <OrcidIcon />
              {user.externalId}
            </span>
          </div>
          <hr className={styles.divider} />
          <button
            className={styles.menuItem}
            onClick={handleLogout}
            disabled={isLoggingOut}
            role="menuitem"
          >
            {isLoggingOut ? 'Signing out...' : 'Sign out'}
          </button>
        </div>
      )}
    </div>
  );
}

function ChevronIcon({ className }: { className?: string }) {
  return (
    <svg
      className={`${styles.chevron} ${className || ''}`}
      viewBox="0 0 20 20"
      fill="currentColor"
    >
      <path
        fillRule="evenodd"
        d="M5.23 7.21a.75.75 0 011.06.02L10 11.168l3.71-3.938a.75.75 0 111.08 1.04l-4.25 4.5a.75.75 0 01-1.08 0l-4.25-4.5a.75.75 0 01.02-1.06z"
        clipRule="evenodd"
      />
    </svg>
  );
}

function OrcidIcon() {
  return (
    <svg className={styles.orcidIcon} viewBox="0 0 24 24" fill="currentColor">
      <circle cx="12" cy="12" r="10" fill="#a6ce39" />
      <text
        x="12"
        y="16"
        textAnchor="middle"
        fontSize="11"
        fontWeight="bold"
        fill="white"
      >
        iD
      </text>
    </svg>
  );
}
