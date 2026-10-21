"""Small S3 helper for listing, uploading, and downloading OTT project files."""

from __future__ import annotations

import argparse
import mimetypes
import os
import sys
from pathlib import Path, PurePosixPath
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

DEFAULT_BUCKET = "ott-media-engagement-piyush-2026"
DEFAULT_REGION = "ap-southeast-2"
DEFAULT_PREFIX = "raw/"


def list_objects(s3: Any, bucket: str, prefix: str) -> int:
    paginator = s3.get_paginator("list_objects_v2")
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            print(f"{item['Key']}\t{item['Size']} bytes")
            count += 1

    if count == 0:
        print(f"No objects found under s3://{bucket}/{prefix}")
    return count


def upload_file(s3: Any, bucket: str, source: Path, key: str) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"Upload source is not a file: {source}")

    content_type, _ = mimetypes.guess_type(source.name)
    extra_args = {"ContentType": content_type} if content_type else {}
    s3.upload_file(
        str(source),
        bucket,
        key,
        ExtraArgs=extra_args,
    )
    print(f"Uploaded {source} to s3://{bucket}/{key}")


def download_file(s3: Any, bucket: str, key: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(destination))
    print(f"Downloaded s3://{bucket}/{key} to {destination}")


def _relative_key(value: str) -> str:
    key = PurePosixPath(value)
    if key.is_absolute() or not value or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("The object key must be a non-empty relative path without '..'.")
    return key.as_posix()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket",
        default=os.environ.get("OTT_S3_BUCKET", DEFAULT_BUCKET),
        help="S3 bucket name (default: OTT_S3_BUCKET or project bucket)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", DEFAULT_REGION),
        help="AWS region (default: AWS_REGION or ap-southeast-2)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    list_command = commands.add_parser("list", help="List objects under a prefix")
    list_command.add_argument("--prefix", default=DEFAULT_PREFIX)

    upload_command = commands.add_parser("upload", help="Upload a local file")
    upload_command.add_argument("source", type=Path)
    upload_command.add_argument(
        "--key",
        help="Object key relative to --prefix (default: source filename)",
    )
    upload_command.add_argument("--prefix", default=DEFAULT_PREFIX)

    download_command = commands.add_parser("download", help="Download an S3 object")
    download_command.add_argument("key", help="Full S3 object key, e.g. raw/content.csv")
    download_command.add_argument("destination", type=Path)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    s3 = boto3.Session(region_name=args.region).client("s3")

    try:
        if args.command == "list":
            list_objects(s3, args.bucket, args.prefix)
        elif args.command == "upload":
            prefix = args.prefix.rstrip("/")
            relative_key = _relative_key(args.key or args.source.name)
            key = f"{prefix}/{relative_key}" if prefix else relative_key
            upload_file(s3, args.bucket, args.source, key)
        else:
            download_file(s3, args.bucket, args.key, args.destination)
    except NoCredentialsError:
        print(
            "AWS credentials were not found. Configure AWS SSO or a restricted "
            "IAM profile locally; do not put credentials in this project.",
            file=sys.stderr,
        )
        return 2
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "AWS error")
        message = error.get("Message", str(exc))
        print(f"S3 request failed ({code}): {message}", file=sys.stderr)
        return 2
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
