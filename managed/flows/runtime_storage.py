"""Storage resolved at run time, from injected credentials + env."""

import os

from prefect import flow, get_run_logger
from prefect_aws import AwsCredentials, S3Bucket


def _require_region() -> str:
    """The region for the S3 client, from whichever variable the runtime exports.

    `federated_identity.aws_region_name` is where the pool names the region, but
    which env var the managed runtime exports from it is not contractual. If
    neither is present, add AWS_REGION to the pool's `env`.
    """
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise ValueError(
            "No AWS region in the environment. Set AWS_REGION in the work pool's "
            "`env` job variable (federated_identity.aws_region_name configures the "
            "role assumption, not this process's environment)."
        )
    return region


@flow
def ingest(bucket: str | None = None, prefix: str | None = None) -> str:
    """Write to whichever bucket this run was pointed at.

    Precedence: explicit flow parameter > job_variables `env` > pool default.
    Credentials come from the role assumed by `federated_identity`, so this
    flow never holds a static key and cannot reach a bucket the role denies.
    """
    logger = get_run_logger()

    target = bucket or os.environ.get("DATA_BUCKET")
    if not target:
        raise ValueError(
            "No target bucket. Pass bucket=... or set DATA_BUCKET in `env` -- note "
            "that an `env` job variable replaces the pool default wholesale, so a "
            "partial override drops it."
        )
    key_prefix = prefix or os.environ.get("DATA_PREFIX", "")

    # No stored block needed: the managed worker injected short-lived creds into
    # the standard AWS env vars, so boto's default chain picks them up.
    store = S3Bucket(
        bucket_name=target,
        bucket_folder=key_prefix,
        credentials=AwsCredentials(region_name=_require_region()),
    )

    store.write_path("hello.txt", b"written by managed execution")
    logger.info("wrote to s3://%s/%s", target, key_prefix)
    return f"s3://{target}/{key_prefix}"
