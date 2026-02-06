import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';
import { AuthProvider } from '@/components/auth/AuthProvider';
import { Header } from '@/components/layout/Header';
import { Footer } from '@/components/layout/Footer';

const geist = Geist({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-body',
});

const geistMono = Geist_Mono({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-mono',
});

export const metadata: Metadata = {
  title: 'Lingual Bio — Search Biological Databases',
  description: 'Semantic search over biological databases from Open Science Archive. Discover research datasets using natural language.',
  keywords: ['biology', 'genomics', 'GEO', 'research', 'scientific data', 'semantic search'],
};

// API URL: use env var for dev (different port), relative path for prod (reverse proxy)
const apiBaseUrl = process.env.NEXT_PUBLIC_API_URL || '/api/v1';

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${geist.variable} ${geistMono.variable}`}>
      <body className={geist.className}>
        <AuthProvider baseUrl={apiBaseUrl}>
          <Header />
          {children}
          <Footer />
        </AuthProvider>
      </body>
    </html>
  );
}
