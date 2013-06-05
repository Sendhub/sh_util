# -*- coding: utf-8 -*-

"""Shortcuts to perform common tasks on S3."""

import boto, re, settings
from io import BytesIO
import logging


_fileNameCleanerRe = re.compile(r'[^a-z0-9\/_\.-]+', re.I)

_bucketName = getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'fakebucket')

def getS3Client():
    """Returns a new S3 connection."""
    s3 = boto.s3.connection.S3Connection(settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY, is_secure=True)
    return s3


def deleteFile(s3FilePath):
    """Delete a file from S3."""
    # S3 Client and Bucket.
    s3Client = getS3Client()
    bucket = s3Client.get_bucket(_bucketName)
    # S3 Key.
    key = bucket.new_key(s3FilePath)
    key.delete()


def uploadFile(destinationFilePath, data, contentType='plain/text', policy='private', cacheContraol=None):
    """Upload a file to S3."""
    # S3 Client and Bucket.
    s3Client = getS3Client()
    bucket = s3Client.get_bucket(_bucketName)

    # S3 Key.
    key = bucket.new_key(destinationFilePath)

    # Get file contents, or assume that fileHandle is the raw data.
    if isinstance(data, (str, unicode)):
        method = key.set_contents_from_string
    else:
        method = key.set_contents_from_file
        data = BytesIO(data.read()) if hasattr(data, 'read') and callable(data.read) else BytesIO(data)

    destinationFilePath = _fileNameCleanerRe.sub('', destinationFilePath)

    logging.info(u'Uploading fileName="{0}" to S3 bucketName="{1}"'.format(destinationFilePath, _bucketName))

    key.content_type = contentType
    key.cache_control = cacheContraol

    # Upload the file to S3.
    method(data, num_cb=0, policy=policy)

    return getSignedUrl(destinationFilePath, True)


def getSignedUrl(s3FilePath, secure=True, expiresIn=60, includeSignature=True):
    """Generate a signed url for an S3 file."""
    signedUrl = getS3Client().generate_url(expiresIn, 'GET', bucket=_bucketName, key=s3FilePath, force_http=not secure)
    if includeSignature:
        return signedUrl
    else:
        unsignedUrl = signedUrl[0:signedUrl.rindex('?')]
        logging.info(u'Unsigned url="{0}"'.format(unsignedUrl))
        return unsignedUrl

__all__ = ['getS3Client', 'deleteFile', 'uploadFile', 'getSignedUrl']

