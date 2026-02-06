'use client';

import { useAuth } from '@/hooks/useAuth';
import { LoginButton } from '@/components/auth/LoginButton';
import { UserMenu } from '@/components/auth/UserMenu';

export function AuthButtons() {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return null; // Don't flash login button while loading
  }

  if (isAuthenticated) {
    return <UserMenu />;
  }

  return <LoginButton />;
}
