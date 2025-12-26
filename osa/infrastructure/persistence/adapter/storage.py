import shutil
from pathlib import Path
from typing import Any

from osa.domain.deposition.port.storage import StoragePort
from osa.domain.shared.model.srn import DepositionSRN


class LocalStorageAdapter(StoragePort):
    """Local filesystem implementation of StoragePort."""

    def __init__(self, base_path: str = "/tmp/osa_storage"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_dep_path(self, deposition_id: DepositionSRN) -> Path:
        # Structure: /tmp/osa_storage/{domain}/{id}/
        # Using safe string representation
        safe_id = f"{deposition_id.domain.root}_{deposition_id.id.root}"
        return self.base_path / safe_id

    def delete_files_for_deposition(self, deposition_id: DepositionSRN) -> None:
        target_dir = self._get_dep_path(deposition_id)
        if target_dir.exists():
            shutil.rmtree(target_dir)

    def save_file(self, deposition_id: DepositionSRN, filename: str, stream: Any) -> None:
        # Helper for UploadFile command
        target_dir = self._get_dep_path(deposition_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        
        target_file = target_dir / filename
        
        # Assuming stream is file-like open in binary mode
        # If stream is from httpx/spooledtempfile
        with open(target_file, "wb") as f:
            # If stream supports read/write
            if hasattr(stream, "read"):
                if hasattr(stream, "seek"):
                    stream.seek(0)
                shutil.copyfileobj(stream, f)
            else:
                # Fallback if just bytes
                f.write(stream)
