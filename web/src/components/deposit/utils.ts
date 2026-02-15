export function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function getFileExtension(name: string): string {
  return name.split('.').pop()?.toLowerCase() ?? '';
}

export function getFileIconType(name: string): 'spreadsheet' | 'data' | 'generic' {
  const ext = getFileExtension(name);
  if (['xlsx', 'xls', 'csv', 'tsv'].includes(ext)) return 'spreadsheet';
  if (['fastq', 'fasta', 'bam', 'sam', 'vcf', 'h5', 'h5ad', 'gz', 'tar', 'zip', 'hdf5', 'parquet', 'json', 'mzML', 'raw'].includes(ext)) return 'data';
  return 'generic';
}
