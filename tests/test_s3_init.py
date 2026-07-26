"""Unit tests for ``sh_util.s3``.

All AWS calls go through a mocked ``boto3`` client — no real network/S3
access happens. ``settings.AWS_*`` values are monkeypatched since the host
app's ``settings`` shim doesn't define them (see conftest.py for how a bare
``import settings`` resolves in this test tree).
"""

from unittest.mock import MagicMock

import pytest

from sh_util import s3 as s3_module


@pytest.fixture(autouse=True)
def aws_settings(monkeypatch):
    monkeypatch.setattr(s3_module.settings, "AWS_ACCESS_KEY_ID", "test-key", raising=False)
    monkeypatch.setattr(s3_module.settings, "AWS_SECRET_ACCESS_KEY", "test-secret", raising=False)
    monkeypatch.setattr(s3_module, "_BUCKETNAME", "test-bucket")


@pytest.fixture
def fake_client(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(s3_module.boto3, "client", MagicMock(return_value=client))
    return client


class TestGetS3Client:
    def test_creates_a_client_with_configured_credentials(self, monkeypatch):
        mock_boto3_client = MagicMock(return_value=MagicMock())
        monkeypatch.setattr(s3_module.boto3, "client", mock_boto3_client)

        s3_module.get_s3_client()

        mock_boto3_client.assert_called_once_with(
            "s3",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
        )


class TestDeleteFile:
    def test_deletes_the_object_from_the_configured_bucket(self, fake_client):
        s3_module.delete_file("path/to/file.txt")

        fake_client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="path/to/file.txt")


class TestUploadFile:
    def test_string_data_is_encoded_to_bytes(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))

        s3_module.upload_file("dest/file.txt", "hello world")

        _, kwargs = fake_client.put_object.call_args
        assert kwargs["Body"] == b"hello world"

    def test_file_like_data_is_read(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))
        data = MagicMock()
        data.read.return_value = b"raw-bytes"

        s3_module.upload_file("dest/file.txt", data)

        _, kwargs = fake_client.put_object.call_args
        assert kwargs["Body"] == b"raw-bytes"

    def test_other_data_is_passed_through_unchanged(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))

        s3_module.upload_file("dest/file.txt", b"already-bytes")

        _, kwargs = fake_client.put_object.call_args
        assert kwargs["Body"] == b"already-bytes"

    def test_destination_path_is_sanitized(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))

        s3_module.upload_file("dest/ file!.txt", "data")

        _, kwargs = fake_client.put_object.call_args
        assert kwargs["Key"] == "dest/file.txt"

    def test_content_type_defaults_to_plain_text(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))

        s3_module.upload_file("dest/file.txt", "data")

        _, kwargs = fake_client.put_object.call_args
        assert kwargs["ContentType"] == "plain/text"

    def test_cache_control_is_omitted_when_not_provided(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))

        s3_module.upload_file("dest/file.txt", "data")

        _, kwargs = fake_client.put_object.call_args
        assert "CacheControl" not in kwargs

    def test_cache_control_is_included_when_provided(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="signed-url"))

        s3_module.upload_file("dest/file.txt", "data", cache_contraol="max-age=60")

        _, kwargs = fake_client.put_object.call_args
        assert kwargs["CacheControl"] == "max-age=60"

    def test_returns_the_signed_url(self, fake_client, monkeypatch):
        monkeypatch.setattr(s3_module, "get_signed_url", MagicMock(return_value="https://signed"))

        assert s3_module.upload_file("dest/file.txt", "data") == "https://signed"


class TestGetSignedUrl:
    def test_returns_the_presigned_url_including_signature_by_default(self, fake_client):
        fake_client.generate_presigned_url.return_value = "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc"

        result = s3_module.get_signed_url("path/to/file.txt")

        assert result == "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc"
        fake_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "path/to/file.txt"},
            ExpiresIn=60,
            HttpMethod="GET",
        )

    def test_strips_the_query_string_when_signature_not_requested(self, fake_client):
        fake_client.generate_presigned_url.return_value = "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc"

        result = s3_module.get_signed_url("path/to/file.txt", include_signature=False)

        assert result == "https://bucket.s3.amazonaws.com/key"

    def test_respects_custom_expiry(self, fake_client):
        fake_client.generate_presigned_url.return_value = "https://x?sig"

        s3_module.get_signed_url("path/to/file.txt", expires_in=3600)

        _, kwargs = fake_client.generate_presigned_url.call_args
        assert kwargs["ExpiresIn"] == 3600


class TestDunderAll:
    def test_all_exports_the_public_functions(self):
        assert s3_module.__all__ == ["get_s3_client", "delete_file", "upload_file", "get_signed_url"]
