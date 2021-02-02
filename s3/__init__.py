"""
Shortcuts to perform common tasks on S3.
"""

# -*- coding: utf-8 -*-

import re
from io import BytesIO
import logging
import boto
import settings


_fileNameCleanerRe = re.compile(r'[^a-z0-9/_.-]+', re.I)
_BUCKETNAME = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'fakebucket')


def get_s3_client():
    """
    Returns a new S3 connection.
    """
    s3 = boto.s3.connection.S3Connection(settings.AWS_ACCESS_KEY_ID,
                                         settings.AWS_SECRET_ACCESS_KEY, is_secure=True)
    return s3


def delete_file(s3_file_path):
    """Delete a file from S3."""
    # S3 Client and Bucket.
    s3_client = get_s3_client()
    bucket = s3_client.get_bucket(_BUCKETNAME)
    # S3 Key.
    key = bucket.new_key(s3_file_path)
    key.delete()


def upload_file(destination_file_path, data, content_type='plain/text',
                policy='private', cache_contraol=None):
    """Upload a file to S3."""
    # S3 Client and Bucket.
    s3_client = get_s3_client()
    bucket = s3_client.get_bucket(_BUCKETNAME)

    # S3 Key.
    key = bucket.new_key(destination_file_path)

    # Get file contents, or assume that fileHandle is the raw data.
    if isinstance(data, str):
        method = key.set_contents_from_string
    else:
        method = key.set_contents_from_file
        data = BytesIO(data.read()) if hasattr(data, 'read') and \
                                       callable(data.read) else BytesIO(data)

    destination_file_path = _fileNameCleanerRe.sub('', destination_file_path)

    logging.info('Uploading fileName=%s to S3 bucketName=%s', destination_file_path, _BUCKETNAME)

    key.content_type = content_type
    key.cache_control = cache_contraol

    # Upload the file to S3.
    method(data, num_cb=0, policy=policy)

    return get_signed_url(destination_file_path, True)


def get_signed_url(s3_file_path, secure=True, expires_in=60, include_signature=True):
    """Generate a signed url for an S3 file."""
    signed_url = get_s3_client().generate_url(expires_in, 'GET',
                                              bucket=_BUCKETNAME,
                                              key=s3_file_path,
                                              force_http=not secure)
    if include_signature:
        return signed_url

    unsigned_url = signed_url[0:signed_url.rindex('?')]
    logging.info('Unsigned url=%s', unsigned_url)
    return unsigned_url


__all__ = ['get_s3_client', 'delete_file', 'upload_file', 'get_signed_url']
