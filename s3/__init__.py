"""
S3 utility functions.

This module is providing shortcuts for performing common tasks on S3, such as uploading, deleting, and generating signed URLs.
"""

import logging
import re

import boto3
import settings

_fileNameCleanerRe = re.compile(r"[^a-z0-9/_.-]+", re.I)
_BUCKETNAME = getattr(settings, "AWS_STORAGE_BUCKET_NAME", "fakebucket")


def get_s3_client():
    """
    Returning a new S3 connection.

    Returns:
        S3Connection: A new S3 connection object.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )


def delete_file(s3_file_path):
    """
    Deleting a file from S3.

    Args:
        s3_file_path (str): The path to the file in S3 to delete.
    """
    s3_client = get_s3_client()
    s3_client.delete_object(Bucket=_BUCKETNAME, Key=s3_file_path)


def upload_file(destination_file_path, data, content_type="plain/text", policy="private", cache_contraol=None):
    """
    Uploading a file to S3.

    Args:
        destination_file_path (str): The destination path in S3.
        data: The file data or file-like object.
        content_type (str): The content type of the file.
        policy (str): The S3 policy for the file.
        cache_contraol (str): The cache control header value.

    Returns:
        str: The signed URL of the uploaded file.
    """
    s3_client = get_s3_client()

    destination_file_path = _fileNameCleanerRe.sub("", destination_file_path)
    logging.info(f"Uploading fileName={destination_file_path} to S3 bucketName={_BUCKETNAME}")  # noqa

    if isinstance(data, str):
        body = data.encode("utf-8")
    elif hasattr(data, "read") and callable(data.read):
        body = data.read()
    else:
        body = data

    extra_args = {
        "ContentType": content_type,
    }
    if cache_contraol is not None:
        extra_args["CacheControl"] = cache_contraol

    s3_client.put_object(Bucket=_BUCKETNAME, Key=destination_file_path, Body=body, **extra_args)

    return get_signed_url(destination_file_path, True)


def get_signed_url(s3_file_path, secure=True, expires_in=60, include_signature=True):  # noqa
    """
    Generating a signed URL for an S3 file.

    Args:
        s3_file_path (str): The path to the file in S3.
        secure (bool): Whether to use HTTPS.
        expires_in (int): Expiry time in seconds.
        include_signature (bool): Whether to include the signature in the URL.

    Returns:
        str: The signed or unsigned URL for the S3 file.
    """
    signed_url = get_s3_client().generate_presigned_url(
        "get_object",
        Params={"Bucket": _BUCKETNAME, "Key": s3_file_path},
        ExpiresIn=expires_in,
        HttpMethod="GET",
    )
    if include_signature:
        return signed_url

    unsigned_url = signed_url[0 : signed_url.rindex("?")]
    logging.info(f"Unsigned url={unsigned_url}")
    return unsigned_url


__all__ = ["get_s3_client", "delete_file", "upload_file", "get_signed_url"]
