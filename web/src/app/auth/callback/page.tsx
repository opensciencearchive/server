'use client';

import { Suspense, useEffect, useRef } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import Link from 'next/link';
import { osa, parseAuthCallback } from '@/lib/sdk';

function AuthCallbackContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const processedRef = useRef(false);

  // Check for error in URL search params
  const urlError = searchParams.get('error');
  const errorDescription = searchParams.get('error_description');
  const error = urlError ? (errorDescription || urlError) : null;

  useEffect(() => {
    // Prevent double processing in strict mode
    if (processedRef.current || error) return;

    // Check for auth data in hash
    const hash = window.location.hash;
    if (!hash || !hash.includes('auth=')) {
      return;
    }

    // Parse and store auth data
    const params = parseAuthCallback(hash);
    if (!params) {
      return;
    }

    processedRef.current = true;

    // Store via SDK singleton
    osa.auth.handleCallback(hash);

    // Redirect to home (or wherever user came from)
    router.push('/');
  }, [router, error]);

  if (error) {
    return (
      <main style={{ padding: '2rem', textAlign: 'center' }}>
        <h1>Authentication Error</h1>
        <p style={{ color: '#dc2626', marginTop: '1rem' }}>{error}</p>
        <Link
          href="/"
          style={{
            display: 'inline-block',
            marginTop: '1rem',
            padding: '0.5rem 1rem',
            background: '#3b82f6',
            color: 'white',
            borderRadius: '0.375rem',
            textDecoration: 'none',
          }}
        >
          Return to Home
        </Link>
      </main>
    );
  }

  return (
    <main style={{ padding: '2rem', textAlign: 'center' }}>
      <p>Completing sign in...</p>
    </main>
  );
}

export default function AuthCallbackPage() {
  return (
    <Suspense
      fallback={
        <main style={{ padding: '2rem', textAlign: 'center' }}>
          <p>Loading...</p>
        </main>
      }
    >
      <AuthCallbackContent />
    </Suspense>
  );
}
