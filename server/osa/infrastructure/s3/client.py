"""Thin async wrapper around aioboto3 for S3 operations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from osa.domain.shared.error import InfrastructureError

logger = logging.getLogger(__name__)


class S3Client:
    """Async S3 client with bucket baked in.

    Uses aioboto3's context-managed client pattern — each operation gets a
    short-lived client that resolves credentials via the default chain
    (env vars, IRSA, Pod Identity, instance profile, etc.).

    The aioboto3 Session is the long-lived object; clients are ephemeral.
    """

    def __init__(
        self,
        bucket: str,
        endpoint_url: str | None = None,
    ) -> None:
        import aioboto3

        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._session = aioboto3.Session()

    @asynccontextmanager
    async def _client(self):
        """Yield a short-lived S3 client with fresh credentials."""
        kwargs: dict[str, str] = {}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        async with self._session.client("s3", **kwargs) as client:
            yield client

    async def put_object(self, key: str, body: str | bytes) -> None:
        """Upload an object."""
        data = body.encode() if isinstance(body, str) else body
        async with self._client() as client:
            await client.put_object(Bucket=self._bucket, Key=key, Body=data)

    async def get_object(self, key: str) -> bytes:
        """Download an object as bytes."""
        async with self._client() as client:
            resp = await client.get_object(Bucket=self._bucket, Key=key)
            return await resp["Body"].read()

    async def get_object_stream(self, key: str, chunk_size: int = 8192) -> AsyncIterator[bytes]:
        """Stream an object in chunks."""
        async with self._client() as client:
            resp = await client.get_object(Bucket=self._bucket, Key=key)
            async with resp["Body"] as stream:
                while chunk := await stream.read(chunk_size):
                    yield chunk

    async def delete_object(self, key: str) -> None:
        """Delete a single object."""
        async with self._client() as client:
            await client.delete_object(Bucket=self._bucket, Key=key)

    async def delete_objects(self, prefix: str) -> None:
        """Delete all objects under a prefix."""
        keys = await self.list_objects(prefix)
        if not keys:
            return
        async with self._client() as client:
            for i in range(0, len(keys), 1000):
                batch = keys[i : i + 1000]
                resp = await client.delete_objects(
                    Bucket=self._bucket,
                    Delete={"Objects": [{"Key": k} for k in batch]},
                )
                errors = resp.get("Errors", [])
                if errors:
                    failed_keys = [e.get("Key", "?") for e in errors]
                    raise InfrastructureError(
                        f"S3 batch delete failed for {len(errors)} object(s): {failed_keys}"
                    )

    async def copy_object(self, source_key: str, dest_key: str) -> None:
        """Server-side copy within the same bucket."""
        async with self._client() as client:
            await client.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": source_key},
                Key=dest_key,
            )

    async def list_objects(self, prefix: str) -> list[str]:
        """List all object keys under a prefix."""
        async with self._client() as client:
            keys: list[str] = []
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return keys

    async def head_object(self, key: str) -> bool:
        """Check if an object exists."""
        from botocore.exceptions import ClientError

        async with self._client() as client:
            try:
                await client.head_object(Bucket=self._bucket, Key=key)
                return True
            except ClientError as exc:
                if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                    return False
                raise
