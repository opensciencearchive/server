"""Storage layout — single source of truth for directory structure.

Composable path methods that define where data lives on disk/S3.
Storage adapters and runners consume this instead of hardcoding paths.

See #106 for the full consolidation plan. Currently covers ingest paths only;
deposition paths will be migrated here in a follow-up.
"""

from pathlib import Path


def _safe_srn(srn: str) -> str:
    """Convert an SRN to a filesystem-safe string."""
    return srn.replace(":", "_").replace("@", "_")


class StorageLayout:
    """Computes storage paths relative to a data root.

    All methods return Path objects. Storage adapters prefix with their
    own root (filesystem base_path or S3 key prefix).
    """

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir

    # ── Ingest paths ─────────────────────────────────────────────────

    def ingest_run_dir(self, ingest_run_srn: str) -> Path:
        """Root directory for an ingest run."""
        return self._data_dir / "ingests" / _safe_srn(ingest_run_srn)

    def ingest_batch_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        """Directory for a specific batch within an ingest run."""
        return self.ingest_run_dir(ingest_run_srn) / "batches" / str(batch_index)

    def ingest_batch_ingester_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        """Ingester output directory (records.jsonl, files/) for a batch."""
        return self.ingest_batch_dir(ingest_run_srn, batch_index) / "ingester"

    def ingest_batch_hook_dir(self, ingest_run_srn: str, batch_index: int, hook_name: str) -> Path:
        """Hook output directory for a batch."""
        return self.ingest_batch_dir(ingest_run_srn, batch_index) / "hooks" / hook_name

    def ingest_session_file(self, ingest_run_srn: str) -> Path:
        """Session state file for ingester continuation."""
        return self.ingest_run_dir(ingest_run_srn) / "session.json"
