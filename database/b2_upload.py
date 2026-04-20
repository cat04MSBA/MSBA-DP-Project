"""
database/b2_upload.py
=====================
Thin wrapper around boto3 for Backblaze B2 file transfer.

RESPONSIBILITY:
    Upload bytes to B2. Download bytes from B2. List keys
    under a prefix. Nothing else.
    This file does not compute checksums, does not verify data
    integrity, and does not know what the bytes represent.
    Checksums belong to quality_checks.py. Orchestration
    belongs to base_ingestor.py and base_transformer.py.

WHY A CLASS:
    Credentials and bucket configuration are read from env once
    at instantiation. All subsequent upload, download, and list
    calls reuse the same boto3 client without re-reading env or
    re-authenticating. Cleaner than passing credentials to every
    function call and avoids repeated env reads across potentially
    thousands of batch uploads per run.

WHY BOTO3 OVER B2SDK:
    Backblaze B2 exposes an S3-compatible API. boto3 works
    against this API directly with no translation layer.
    b2sdk adds a dependency for no benefit at this scale.

WHY FORMAT-AGNOSTIC (bytes only):
    b2_upload.py does not know or care whether it is uploading
    JSON, CSV, XLSX, or Stata files. The caller serializes to
    bytes before calling upload(). The caller deserializes after
    calling download(). Single responsibility — this file only
    moves bytes.

WHY ADAPTIVE RETRY:
    Backblaze B2 occasionally returns transient 503 errors
    under load. boto3's default retry config is tuned for AWS
    S3 and does not handle B2's specific retry patterns well.
    Setting mode='adaptive' tells boto3 to back off
    automatically on 503s and retry up to 3 times before
    raising ClientError. This handles transient B2 failures
    without any retry logic needed in the caller.

WHY list() METHOD EXISTS:
    The Oxford ingestion script needs to discover which year
    files have already been uploaded to B2 before deciding
    which years to process. Without list(), it would need to
    attempt a download for each possible year and catch the
    error for missing files — expensive and fragile. list()
    allows one efficient prefix scan to determine exactly
    which files exist, then the script processes only those.
    This pattern also applies to any future manual file source
    that uploads files one at a time over multiple runs.

WHY NO DELETE METHOD:
    Bronze files are never deleted — this is a core design
    principle of the bronze layer. retire_ops.py migrates ops
    table data from Supabase to B2 but never deletes from B2.
    A delete method here would violate that principle.

ENV VARS REQUIRED:
    B2_KEY_ID           Backblaze application key ID
    B2_APPLICATION_KEY  Backblaze application key secret
    B2_BUCKET_NAME      Target bucket name (msba-dp-project)
    B2_BUCKET_REGION    Bucket region (e.g. us-east-005)
"""

import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


class B2Client:
    """
    Thin boto3 wrapper for Backblaze B2 file transfer.
    Instantiate once per script run and reuse across all
    upload, download, and list calls.
    """

    def __init__(self):
        key_id      = os.getenv("B2_KEY_ID")
        app_key     = os.getenv("B2_APPLICATION_KEY")
        region      = os.getenv("B2_BUCKET_REGION")
        self.bucket = os.getenv("B2_BUCKET_NAME")

        # Validate all required env vars are present before
        # attempting any connection. Fail fast with a clear
        # message rather than a cryptic boto3 auth error.
        missing = [
            name for name, val in [
                ("B2_KEY_ID",          key_id),
                ("B2_APPLICATION_KEY", app_key),
                ("B2_BUCKET_REGION",   region),
                ("B2_BUCKET_NAME",     self.bucket),
            ]
            if not val
        ]
        if missing:
            raise ValueError(
                f"Missing required env vars: {', '.join(missing)}. "
                "Copy .env.example to .env and fill in credentials."
            )

        self.client = boto3.client(
            service_name          = "s3",
            endpoint_url          = f"https://s3.{region}.backblazeb2.com",
            aws_access_key_id     = key_id,
            aws_secret_access_key = app_key,
            region_name           = region,
            config = Config(
                retries = {
                    'max_attempts': 3,
                    'mode': 'adaptive',
                    # adaptive mode backs off automatically on
                    # 503s — handles transient B2 load spikes
                    # without any retry logic in the caller.
                }
            )
        )

    # ─────────────────────────────────────────────────────────
    # UPLOAD
    # ─────────────────────────────────────────────────────────
    def upload(self, key: str, data: bytes) -> None:
        """
        Upload bytes to B2 at the given key (path within bucket).

        Args:
            key:  Full path within the bucket.
                  Example: 'bronze/world_bank/NY.GDP.PCAP.CD/
                            2020_2026-04-20.json'
            data: Raw bytes to upload. Caller is responsible
                  for serializing to bytes before calling this.

        Raises:
            ClientError: if the upload fails after 3 retries.
                         Caller (base_ingestor.py) handles retry
                         logic at the task level.
        """
        self.client.put_object(
            Bucket = self.bucket,
            Key    = key,
            Body   = data,
        )

    # ─────────────────────────────────────────────────────────
    # DOWNLOAD
    # ─────────────────────────────────────────────────────────
    def download(self, key: str) -> bytes:
        """
        Download bytes from B2 at the given key.

        Args:
            key: Full path within the bucket.

        Returns:
            Raw bytes. Caller is responsible for deserializing.

        Raises:
            ClientError: if the key does not exist or download
                         fails after 3 retries.
        """
        response = self.client.get_object(
            Bucket = self.bucket,
            Key    = key,
        )
        return response["Body"].read()

    # ─────────────────────────────────────────────────────────
    # LIST
    # ─────────────────────────────────────────────────────────
    def list(self, prefix: str) -> list:
        """
        List all object keys in the bucket under a given prefix.

        WHY THIS METHOD EXISTS:
            Manual file sources (Oxford, WIPO, PWT) upload files
            one at a time via upload_to_b2.py. The ingestion script
            needs to discover which files exist on B2 before
            deciding which batch units to process. Without list(),
            the script would need to attempt a download for each
            possible year and catch the error for missing files —
            expensive and fragile. One prefix scan is more
            efficient and explicit.

        WHY PAGINATED:
            The S3 list API returns at most 1000 keys per call.
            For most prefixes (e.g. 'bronze/oxford/') the number
            of files is small (7 years = 7 files) so pagination
            rarely triggers. But implementing pagination correctly
            means this method is safe for any prefix size and
            will not silently truncate results.

        Args:
            prefix: B2 path prefix to list under.
                    Example: 'bronze/oxford/'

        Returns:
            List of full key strings under the prefix.
            Empty list if no keys match the prefix.

        Raises:
            ClientError: if the list operation fails after
                         3 boto3 retries.
        """
        keys = []

        # Use paginator to handle buckets with >1000 objects
        # under the prefix. paginate() handles the continuation
        # token automatically — no manual loop needed.
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket = self.bucket,
            Prefix = prefix,
        )

        for page in pages:
            # 'Contents' key is absent if no objects match.
            # Checking with .get() avoids a KeyError on empty
            # prefixes — returns empty list cleanly.
            for obj in page.get('Contents', []):
                keys.append(obj['Key'])

        return keys
