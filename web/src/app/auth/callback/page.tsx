'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { OSAClient, parseAuthCallback } from '@/lib/sdk';

export default function AuthCallbackPage() {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Check for error in URL search params
    const searchParams = new URLSearchParams(window.location.search);
    const urlError = searchParams.get('error');
    const errorDescription = searchParams.get('error_description');

    if (urlError) {
      setError(errorDescription || urlError);
      return;
    }

    // Check for auth data in hash
    const hash = window.location.hash;
    if (!hash || !hash.includes('auth=')) {
      setError('No authentication data received');
      return;
    }

    // Parse and store auth data
    const params = parseAuthCallback(hash);
    if (!params) {
      setError('Failed to parse authentication data');
      return;
    }

    // Store in client
    const client = new OSAClient({ baseUrl: '/api/v1' });
    client.handleAuthCallback(hash);

    // Redirect to home (or wherever user came from)
    router.push('/');
  }, [router]);

  if (error) {
    return (
      <main style={{ padding: '2rem', textAlign: 'center' }}>
        <h1>Authentication Error</h1>
        <p style={{ color: '#dc2626', marginTop: '1rem' }}>{error}</p>
        <a
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
        </a>
      </main>
    );
  }

  return (
    <main style={{ padding: '2rem', textAlign: 'center' }}>
      <p>Completing sign in...</p>
    </main>
  );
}
