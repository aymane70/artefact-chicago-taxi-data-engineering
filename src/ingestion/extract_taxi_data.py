import argparse
import hashlib
import json
import logging
import os
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError


# --------------------------------------------------
# Configuration
# --------------------------------------------------

URL = "https://data.cityofchicago.org/resource/wrvz-psew.json"

BATCH_SIZE = 10_000
MAX_RETRIES = 5

S3_ENDPOINT_URL = os.getenv("RUSTFS_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY")
S3_BUCKET = os.getenv("RUSTFS_BUCKET")
S3_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BRONZE_PREFIX = "bronze"

MONTHS = ["2023-09", "2023-10", "2023-11", "2023-12"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# S3 / RustFS client
# --------------------------------------------------

def get_s3_client():
    """Create an S3 client configured for the RustFS endpoint."""

    required_settings = {
        "RUSTFS_ENDPOINT_URL": S3_ENDPOINT_URL,
        "RUSTFS_ACCESS_KEY": S3_ACCESS_KEY,
        "RUSTFS_SECRET_KEY": S3_SECRET_KEY,
        "RUSTFS_BUCKET": S3_BUCKET,
    }

    missing = [
        name for name, value in required_settings.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
        )

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def ensure_bucket_exists(s3):
    """Fail early if the configured bucket is unavailable."""

    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as error:
        raise RuntimeError(
            f"Cannot access S3 bucket '{S3_BUCKET}'. "
            "Check the bucket name and RustFS credentials."
        ) from error


def get_month_prefix(month: str) -> str:
    year, month_number = map(int, month.split("-"))

    return (
        f"{S3_BRONZE_PREFIX}/"
        f"year={year}/month={month_number:02d}/"
    )


def get_batch_key(month: str, batch_number: int) -> str:
    return (
        f"{get_month_prefix(month)}"
        f"batch_{batch_number:05d}.jsonl"
    )


def get_manifest_key(month: str) -> str:
    return f"{get_month_prefix(month)}_manifest.json"


def object_exists(s3, key: str) -> bool:
    """Check whether an S3 object exists without downloading it."""

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = error.response.get("Error", {}).get("Code")

        if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def download_s3_object(s3, key: str, local_path: Path):
    """Download an S3 object to a local temporary file."""

    response = s3.get_object(Bucket=S3_BUCKET, Key=key)

    with open(local_path, "wb") as file:
        for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)


def upload_s3_object(s3, local_path: Path, key: str, metadata: dict):
    """Upload a validated batch and verify its stored metadata and size."""

    s3.upload_file(
        str(local_path),
        S3_BUCKET,
        key,
        ExtraArgs={
            "Metadata": {
                "sha256": metadata["sha256"],
                "record-count": str(metadata["record_count"]),
            }
        },
    )

    stored = s3.head_object(Bucket=S3_BUCKET, Key=key)

    if stored["ContentLength"] != local_path.stat().st_size:
        raise RuntimeError(
            f"Uploaded object size mismatch for s3://{S3_BUCKET}/{key}"
        )

    stored_sha256 = stored.get("Metadata", {}).get("sha256")

    if stored_sha256 != metadata["sha256"]:
        raise RuntimeError(
            f"Uploaded object checksum metadata mismatch for "
            f"s3://{S3_BUCKET}/{key}"
        )


# --------------------------------------------------
# Date utilities
# --------------------------------------------------

def get_month_boundaries(month: str):
    year, month_number = map(int, month.split("-"))

    start = date(year, month_number, 1)

    if month_number == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month_number + 1, 1)

    return start, end


# --------------------------------------------------
# API requests with retry
# --------------------------------------------------

def request_with_retry(session, params):

    for attempt in range(MAX_RETRIES):

        try:
            response = session.get(
                URL,
                params=params,
                timeout=(10, 180),
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")

                try:
                    wait = (
                        float(retry_after)
                        if retry_after
                        else 2 ** attempt
                    )
                except ValueError:
                    wait = 2 ** attempt

                if attempt == MAX_RETRIES - 1:
                    response.raise_for_status()

                logger.warning(
                    "Rate limited. Waiting %s seconds.",
                    wait,
                )

                time.sleep(wait)
                continue

            response.raise_for_status()

            return response.json()

        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.HTTPError,
            ValueError,
        ) as error:

            if isinstance(error, requests.HTTPError):
                status = error.response.status_code

                if status not in (429, 500, 502, 503, 504):
                    raise

            if attempt == MAX_RETRIES - 1:
                raise

            wait = 2 ** attempt

            logger.warning(
                "Request failed: %s. Retrying in %s seconds.",
                error,
                wait,
            )

            time.sleep(wait)

    raise RuntimeError("Maximum retries exceeded")


# --------------------------------------------------
# Source metadata
# --------------------------------------------------

def get_month_count(session, start, end):

    params = {
        "$select": "COUNT(*) AS total",
        "$where": (
            f"trip_start_timestamp >= '{start}T00:00:00' "
            f"AND trip_start_timestamp < '{end}T00:00:00'"
        ),
    }

    result = request_with_retry(session, params)

    return int(result[0]["total"])


# --------------------------------------------------
# Record validation
# --------------------------------------------------

def validate_record(record):

    if not isinstance(record, dict):
        return False

    trip_id = record.get("trip_id")

    return (
        isinstance(trip_id, str)
        and bool(trip_id.strip())
    )


# --------------------------------------------------
# Local temporary batch serialization and validation
# --------------------------------------------------

def save_batch(records, file_path: Path):

    temp_path = file_path.with_suffix(
        file_path.suffix + ".tmp"
    )

    seen_ids = set()

    try:
        with open(temp_path, "w", encoding="utf-8") as file:

            for record in records:

                if not validate_record(record):
                    raise ValueError(
                        "Invalid record: missing or invalid trip_id"
                    )

                trip_id = record["trip_id"]

                if trip_id in seen_ids:
                    raise ValueError(
                        f"Duplicate trip_id found: {trip_id}"
                    )

                seen_ids.add(trip_id)

                file.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )

        os.replace(temp_path, file_path)

    finally:
        if temp_path.exists():
            temp_path.unlink()


def validate_batch(file_path: Path, expected_count):

    if not file_path.exists():
        return None

    seen_ids = set()
    actual_count = 0
    checksum = hashlib.sha256()

    try:
        with open(file_path, "rb") as file:

            for raw_line in file:

                checksum.update(raw_line)

                line = raw_line.decode("utf-8")
                record = json.loads(line)

                if not validate_record(record):
                    return None

                trip_id = record["trip_id"]

                if trip_id in seen_ids:
                    return None

                seen_ids.add(trip_id)
                actual_count += 1

        if actual_count != expected_count:
            return None

        return {
            "record_count": actual_count,
            "sha256": checksum.hexdigest(),
        }

    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        OSError,
    ):
        logger.warning("Invalid batch file: %s", file_path)
        return None


def validate_s3_batch(s3, key: str, local_path: Path, expected_count):
    """Download an existing S3 batch and validate its actual contents."""

    try:
        download_s3_object(s3, key, local_path)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = error.response.get("Error", {}).get("Code")

        if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
            return None

        raise

    metadata = validate_batch(local_path, expected_count)

    if metadata is None:
        return None

    stored = s3.head_object(Bucket=S3_BUCKET, Key=key)
    stored_metadata = stored.get("Metadata", {})

    if (
        stored_metadata.get("sha256") != metadata["sha256"]
        or stored_metadata.get("record-count") != str(expected_count)
    ):
        logger.warning(
            "S3 object checksum or record-count metadata mismatch: %s",
            key,
        )
        return None

    return metadata


# --------------------------------------------------
# Manifest management
# --------------------------------------------------

def write_manifest(s3, manifest_key: str, manifest: dict):

    body = json.dumps(
        manifest,
        indent=4,
        ensure_ascii=False,
    ).encode("utf-8")

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=body,
        ContentType="application/json",
    )


def load_or_create_manifest(s3, month: str, total_records: int):
    manifest_key = get_manifest_key(month)

    try:
        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
        )
        existing = json.loads(response["Body"].read().decode("utf-8"))

        if (
            existing.get("month") == month
            and existing.get("source_count") == total_records
        ):
            # Rebuild batch metadata from objects actually validated in
            # this run. The objects themselves remain available for resume.
            existing["status"] = "in_progress"
            existing["batches"] = {}
            existing.pop("completed_at", None)
            existing.pop("total_records", None)
            existing.pop("unique_trip_ids", None)
            existing.pop("total_batches", None)
            existing["updated_at"] = datetime.now(
                timezone.utc
            ).isoformat()
            return existing

        logger.warning(
            "Existing manifest does not match current source count. "
            "Creating a new manifest for %s.",
            month,
        )

    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = error.response.get("Error", {}).get("Code")

        if not (
            status == 404
            or code in ("404", "NoSuchKey", "NotFound")
        ):
            raise
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Existing manifest is invalid. Recreating it.")

    now = datetime.now(timezone.utc).isoformat()

    return {
        "month": month,
        "source": URL,
        "source_count": total_records,
        "batch_size": BATCH_SIZE,
        "status": "in_progress",
        "batches": {},
        "created_at": now,
        "updated_at": now,
    }


def update_manifest(
    manifest,
    batch_number,
    offset,
    batch_key,
    metadata,
):

    manifest["batches"][batch_key] = {
        "batch_number": batch_number,
        "offset": offset,
        "record_count": metadata["record_count"],
        "sha256": metadata["sha256"],
    }

    manifest["updated_at"] = datetime.now(
        timezone.utc
    ).isoformat()


# --------------------------------------------------
# S3 object listing
# --------------------------------------------------

def list_batch_objects(s3, month: str):
    prefix = get_month_prefix(month)
    paginator = s3.get_paginator("list_objects_v2")

    keys = set()

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            filename = key.rsplit("/", 1)[-1]

            if filename.startswith("batch_") and filename.endswith(".jsonl"):
                keys.add(key)

    return keys


# --------------------------------------------------
# Monthly reconciliation
# --------------------------------------------------

def reconcile_month(
    s3,
    month,
    total_records,
    manifest,
    temp_dir: Path,
):

    logger.info("Starting reconciliation for %s", month)

    expected_batches = 0
    total_extracted = 0
    seen_ids = set()

    for offset in range(0, total_records, BATCH_SIZE):

        batch_number = offset // BATCH_SIZE

        expected_count = min(
            BATCH_SIZE,
            total_records - offset,
        )

        batch_key = get_batch_key(month, batch_number)
        local_path = temp_dir / f"reconcile_{batch_number:05d}.jsonl"

        metadata = validate_s3_batch(
            s3,
            batch_key,
            local_path,
            expected_count,
        )

        if metadata is None:
            raise RuntimeError(
                f"Reconciliation failed: invalid or missing "
                f"batch {batch_number} for {month}"
            )

        # Check duplicates across different batches.
        with open(local_path, "r", encoding="utf-8") as file:
            for line in file:
                record = json.loads(line)
                trip_id = record["trip_id"]

                if trip_id in seen_ids:
                    raise RuntimeError(
                        f"Duplicate trip_id across batches: {trip_id}"
                    )

                seen_ids.add(trip_id)

        update_manifest(
            manifest,
            batch_number,
            offset,
            batch_key,
            metadata,
        )

        total_extracted += metadata["record_count"]
        expected_batches += 1

    # Detect unexpected extra batch objects.
    actual_batch_keys = list_batch_objects(s3, month)

    expected_batch_keys = {
        get_batch_key(month, batch_number)
        for batch_number in range(expected_batches)
    }

    extra_keys = actual_batch_keys - expected_batch_keys

    if extra_keys:
        raise RuntimeError(
            f"Unexpected batch objects found: {sorted(extra_keys)}"
        )

    if total_extracted != total_records:
        raise RuntimeError(
            f"Record count mismatch for {month}: "
            f"expected {total_records}, got {total_extracted}"
        )

    if len(seen_ids) != total_records:
        raise RuntimeError(
            f"Unique record count mismatch for {month}"
        )

    manifest["status"] = "complete"
    manifest["total_records"] = total_extracted
    manifest["unique_trip_ids"] = len(seen_ids)
    manifest["total_batches"] = expected_batches
    manifest["completed_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    manifest["updated_at"] = manifest["completed_at"]

    write_manifest(
        s3,
        get_manifest_key(month),
        manifest,
    )

    logger.info(
        "Reconciliation successful | %s | "
        "%s records | %s unique IDs | %s batches",
        month,
        total_extracted,
        len(seen_ids),
        expected_batches,
    )


# --------------------------------------------------
# Monthly extraction
# --------------------------------------------------

def extract_month(s3, session, month, max_batches=None):

    start, end = get_month_boundaries(month)

    total_records = get_month_count(
        session,
        start,
        end,
    )

    logger.info(
        "%s: %s records available",
        month,
        total_records,
    )

    manifest = load_or_create_manifest(
        s3,
        month,
        total_records,
    )

    manifest_key = get_manifest_key(month)
    write_manifest(s3, manifest_key, manifest)

    with tempfile.TemporaryDirectory(
        prefix=f"chicago_taxi_{month}_"
    ) as temp_directory:

        temp_dir = Path(temp_directory)
        batch_number = 0

        for offset in range(0, total_records, BATCH_SIZE):

            # Limit the number of batch positions examined.
            if (
                max_batches is not None
                and batch_number >= max_batches
            ):
                logger.info(
                    "Batch limit reached. Stopping test extraction."
                )
                break

            expected_count = min(
                BATCH_SIZE,
                total_records - offset,
            )

            batch_key = get_batch_key(month, batch_number)
            local_path = temp_dir / f"batch_{batch_number:05d}.jsonl"

            metadata = validate_s3_batch(
                s3,
                batch_key,
                local_path,
                expected_count,
            )

            if metadata is not None:
                logger.info(
                    "Skipping existing valid S3 batch %s",
                    batch_number,
                )

            else:
                params = {
                    "$limit": BATCH_SIZE,
                    "$offset": offset,
                    "$order": "trip_start_timestamp ASC, trip_id ASC",
                    "$where": (
                        f"trip_start_timestamp >= '{start}T00:00:00' "
                        f"AND trip_start_timestamp < '{end}T00:00:00'"
                    ),
                }

                records = request_with_retry(
                    session,
                    params,
                )

                if len(records) != expected_count:
                    raise RuntimeError(
                        f"Unexpected batch size for {month}, "
                        f"offset {offset}: expected {expected_count}, "
                        f"received {len(records)}"
                    )

                save_batch(records, local_path)

                metadata = validate_batch(
                    local_path,
                    expected_count,
                )

                if metadata is None:
                    raise RuntimeError(
                        f"Serialized batch failed validation: {local_path}"
                    )

                upload_s3_object(
                    s3,
                    local_path,
                    batch_key,
                    metadata,
                )

                logger.info(
                    "%s | Batch %s | %s records | Progress: %s/%s",
                    month,
                    batch_number,
                    len(records),
                    min(offset + len(records), total_records),
                    total_records,
                )

            update_manifest(
                manifest,
                batch_number,
                offset,
                batch_key,
                metadata,
            )

            write_manifest(
                s3,
                manifest_key,
                manifest,
            )

            batch_number += 1

        # Reconcile only after a complete extraction.
        if max_batches is None:
            reconcile_month(
                s3,
                month,
                total_records,
                manifest,
                temp_dir,
            )

    logger.info("Finished extraction for %s", month)


# --------------------------------------------------
# Main ingestion entry point
# --------------------------------------------------

def extract_all_months(months=None, max_batches=None):

    months = months or MONTHS

    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    with requests.Session() as session:
        for month in months:
            extract_month(
                s3,
                session,
                month,
                max_batches,
            )


# --------------------------------------------------
# CLI
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Chicago Taxi Trips data ingestion to RustFS"
    )

    parser.add_argument(
        "--month",
        help="Month to extract, for example 2023-09",
    )

    parser.add_argument(
        "--max-batches",
        type=int,
        help="Maximum number of batch positions to process per month",
    )

    args = parser.parse_args()

    months = (
        [args.month]
        if args.month
        else MONTHS
    )

    extract_all_months(
        months=months,
        max_batches=args.max_batches,
    )


if __name__ == "__main__":
    main()
