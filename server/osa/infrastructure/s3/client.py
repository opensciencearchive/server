"""Thin async wrapper around aioboto3 for S3 operations."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from osa.domain.shared.error import InfrastructureError

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AioS3Client

logger = logging.getLogger(__name__)


class S3Client:
    """Async S3 client with bucket baked in.

    Callers pass only the object key — bucket is set once at construction.
    All methods translate S3 errors into domain-friendly exceptions.
    """

    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._endpoint_url = endpoint_url
        self._session: object | None = None
        self._client: AioS3Client | None = None
        self._init_lock = asyncio.Lock()

    async def _get_client(self) -> AioS3Client:
        if self._client is None:
            async with self._init_lock:
                if self._client is None:
                    import aioboto3

                    self._session = aioboto3.Session()
                    self._client = await self._session.client(
                        "s3",
                        region_name=self._region,
                        endpoint_url=self._endpoint_url,
                    ).__aenter__()
        client = self._client
        if client is None:
            raise RuntimeError("S3 client initialization failed")
        return client

    async def put_object(self, key: str, body: str | bytes) -> None:
        """Upload an object."""
        client = await self._get_client()
        data = body.encode() if isinstance(body, str) else body
        await client.put_object(Bucket=self._bucket, Key=key, Body=data)

    async def get_object(self, key: str) -> bytes:
        """Download an object as bytes."""
        client = await self._get_client()
        resp = await client.get_object(Bucket=self._bucket, Key=key)
        return await resp["Body"].read()

    async def get_object_stream(self, key: str, chunk_size: int = 8192) -> AsyncIterator[bytes]:
        """Stream an object in chunks."""
        client = await self._get_client()
        resp = await client.get_object(Bucket=self._bucket, Key=key)
        async with resp["Body"] as stream:
            while chunk := await stream.read(chunk_size):
                yield chunk

    async def delete_object(self, key: str) -> None:
        """Delete a single object."""
        client = await self._get_client()
        await client.delete_object(Bucket=self._bucket, Key=key)

    async def delete_objects(self, prefix: str) -> None:
        """Delete all objects under a prefix."""
        keys = await self.list_objects(prefix)
        if not keys:
            return
        client = await self._get_client()
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
        client = await self._get_client()
        await client.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": source_key},
            Key=dest_key,
        )

    async def list_objects(self, prefix: str) -> list[str]:
        """List all object keys under a prefix."""
        client = await self._get_client()
        keys: list[str] = []
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    async def head_object(self, key: str) -> bool:
        """Check if an object exists."""
        from botocore.exceptions import ClientError

        client = await self._get_client()
        try:
            await client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    async def close(self) -> None:
        """Clean up the underlying client."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
            self._session = None
