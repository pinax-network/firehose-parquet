#!/usr/bin/env python3
"""Run a loopback-only S3 server (moto) with one bucket, for the spike.

Usage: loopback_s3.py --port PORT [--bucket delta-spike]

Binds 127.0.0.1 only, creates the bucket, prints the endpoint URL and serves
until killed. Never contacts a real endpoint. moto accepts any credentials and
unsigned requests, which also exercises anonymous (public-read) reads.
"""
import argparse
import json
import sys
import time

import boto3
from moto.server import ThreadedMotoServer


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--bucket", default="delta-spike")
    args = ap.parse_args()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=args.port, verbose=False)
    server.start()
    endpoint = f"http://127.0.0.1:{args.port}"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="spike",
        aws_secret_access_key="spike",
    )
    s3.create_bucket(Bucket=args.bucket)
    # Public read, as the deployed per-network buckets are: anonymous GET and LIST.
    s3.put_bucket_policy(
        Bucket=args.bucket,
        Policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Resource": [
                            f"arn:aws:s3:::{args.bucket}",
                            f"arn:aws:s3:::{args.bucket}/*",
                        ],
                    }
                ],
            }
        ),
    )
    # Conditional create must be honored, or the spike's S3 results mean nothing.
    s3.put_object(Bucket=args.bucket, Key="probe", Body=b"1", IfNoneMatch="*")
    try:
        s3.put_object(Bucket=args.bucket, Key="probe", Body=b"2", IfNoneMatch="*")
    except s3.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] == "PreconditionFailed", e.response
    else:
        print("loopback S3 ignores If-None-Match", file=sys.stderr)
        return 1
    s3.delete_object(Bucket=args.bucket, Key="probe")
    print(endpoint, flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
