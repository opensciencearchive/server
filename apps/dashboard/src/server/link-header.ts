/** Extract the `rel="next"` URL from an RFC 5988 Link header, or null. */
export function nextPageUrl(link: string | null, base: string): string | null {
  if (!link) return null;
  for (const part of link.split(",")) {
    if (!/rel="next"/.test(part)) continue;
    const start = part.indexOf("<");
    const end = part.indexOf(">", start);
    if (start === -1 || end === -1) return null;
    const url = part.slice(start + 1, end);
    return url.startsWith("http") ? url : `${base}${url}`;
  }
  return null;
}
