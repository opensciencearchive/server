import httpx
from urllib.parse import urlparse

from osa.domain.shadow.port.ingestion import IngestionPort, IngestionResult


class HttpIngestionAdapter(IngestionPort):
    def ingest(self, url: str) -> IngestionResult:
        # TODO: This is a synchronous wrapper around async httpx, or we should make the port async.
        # For now, assuming sync for simplicity or using httpx.Client.

        # Simple filename extraction
        path = urlparse(url).path
        filename = path.split("/")[-1] or "download.dat"

        # We need a stream.
        # If the caller expects to read it, we might need to keep the response open.
        # Ideally, we'd use a context manager or pass the response object.
        # For this prototype, let's fetch it to memory (bad for large files) or use a temp file.
        # BETTER: Return a generator or stream.

        # But since I can't change the caller's async nature easily without knowing if it's async...
        # The command handler will be async?

        # Let's use a temporary file for safety and "stream" from it.
        import tempfile

        with httpx.Client() as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                spooled_file = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
                for chunk in response.iter_bytes():
                    spooled_file.write(chunk)
                spooled_file.seek(0)

        return IngestionResult(
            metadata={"source_url": url}, filename=filename, stream=spooled_file
        )
