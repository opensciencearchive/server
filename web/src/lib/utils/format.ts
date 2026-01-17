/**
 * Formatting utilities for display
 */

/**
 * Extract UUID from SRN for display.
 * @example extractSrnId("urn:osa:localhost:dep:ee1a36b4-...") → "ee1a36b4-..."
 */
export function extractSrnId(srn: string): string {
  const parts = srn.split(':');
  return parts[parts.length - 1] || srn;
}

/**
 * Get truncated SRN ID for compact display (first 8 characters of UUID).
 * @example getDisplaySrn("urn:osa:localhost:dep:ee1a36b4-5c2d-4a8f") → "ee1a36b4"
 */
export function getDisplaySrn(srn: string): string {
  const id = extractSrnId(srn);
  return id.substring(0, 8);
}

/**
 * Format score as percentage string.
 * @example formatScore(0.45) → "45%"
 */
export function formatScore(score: number): string {
  return `${Math.round(score * 100)}%`;
}

/**
 * Format date string for display.
 * Converts YYYY/MM/DD to a more readable format.
 * @example formatDate("2026/01/02") → "Jan 2, 2026"
 */
export function formatDate(date: string | null | undefined): string {
  if (!date) return 'Unknown date';

  try {
    // Parse YYYY/MM/DD format
    const [year, month, day] = date.split('/').map(Number);
    const dateObj = new Date(year, month - 1, day);

    return dateObj.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  } catch {
    return date;
  }
}

/**
 * Get display-safe organism string.
 * @example formatOrganism(null) → "Unknown organism"
 */
export function formatOrganism(organism: string | null | undefined): string {
  return organism || 'Unknown organism';
}

/**
 * Parse sample count string to display format.
 * @example formatSampleCount("189") → "189 samples"
 */
export function formatSampleCount(count: string | null | undefined): string {
  if (!count) return 'Unknown samples';
  const num = parseInt(count, 10);
  if (isNaN(num)) return count;
  return `${num.toLocaleString()} sample${num === 1 ? '' : 's'}`;
}

/**
 * Truncate text with ellipsis.
 */
export function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength).trim() + '…';
}
