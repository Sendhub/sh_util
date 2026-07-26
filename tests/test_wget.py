"""Unit tests for ``sh_util.sh_http.wget``.

No real network calls are made. GET requests are exercised by monkeypatching
``urllib.request.build_opener`` to return a fake opener whose ``open()``
returns a fake response object; non-GET requests are exercised by
monkeypatching ``http.client.HTTPSConnection``/``HTTPConnection`` (imported
locally inside ``wget()``, so patching the real ``http.client`` module
attributes -- rather than faking ``sys.modules`` -- is enough, since the
local import just reads whatever is currently on that module).
"""

import gzip
import http.client
import sys
import urllib.error
import urllib.request
from unittest import mock

import pytest

from sh_util.sh_http.wget import WgetError, normalize_url, wget, wget_opener

# `sh_util.sh_http.__init__` does `from .wget import wget`, which rebinds
# the package's `wget` attribute to the *function* -- so `import
# sh_util.sh_http.wget as x` (an attribute-chain lookup under the hood)
# would bind `x` to that function, not the submodule. Pulling the module
# straight out of `sys.modules` sidesteps the shadowing.
wget_module = sys.modules["sh_util.sh_http.wget"]


class TestNormalizeUrl:
    def test_plain_url_is_unchanged(self):
        assert normalize_url("https://example.com/path") == "https://example.com/path"

    def test_special_characters_in_the_path_are_encoded(self):
        # normalize_url uses quote_plus, so a space becomes `+`, not `%20`.
        assert normalize_url("https://example.com/a b") == "https://example.com/a+b"

    def test_query_string_is_encoded(self):
        assert normalize_url("https://example.com/?q=a b") == "https://example.com/?q=a+b"


class TestWgetOpener:
    def test_default_referer_is_used(self):
        opener = wget_opener()
        assert ("Referer", "http://www.google.com/GOBBLEGOBBLEGOBBLE") in opener.addheaders

    def test_custom_referer_is_used(self):
        opener = wget_opener(referer="http://example.com")
        assert ("Referer", "http://example.com") in opener.addheaders

    def test_user_agent_header_is_always_set(self):
        opener = wget_opener()
        headers = dict(opener.addheaders)
        assert headers["User-agent"] == wget_module.USER_AGENT


class _FakeResponse:
    def __init__(self, data, code=200, url="https://example.com/"):
        self._data = data
        self.code = code
        self._url = url

    def read(self):
        return self._data

    def info(self):
        return {"Content-Type": "text/plain"}

    def geturl(self):
        return self._url


@pytest.fixture
def fake_opener(monkeypatch):
    opener = mock.Mock(name="opener")
    opener.addheaders = []
    monkeypatch.setattr(wget_module.urllib.request, "build_opener", mock.Mock(return_value=opener))
    return opener


class TestWgetArgumentValidation:
    def test_zero_tries_raises_immediately(self):
        with pytest.raises(WgetError, match="0 tries left"):
            wget("https://example.com", num_tries=0)

    def test_negative_tries_raises_immediately(self):
        with pytest.raises(WgetError, match="0 tries left"):
            wget("https://example.com", num_tries=-1)


class TestWgetGet:
    def test_returns_the_raw_body_by_default(self, fake_opener):
        fake_opener.open.return_value = _FakeResponse(b"hello world")

        assert wget("https://example.com") == b"hello world"

    def test_as_dict_on_a_get_returns_a_dict_with_the_decompressed_body(self, fake_opener):
        fake_opener.open.return_value = _FakeResponse(b"hello", code=201, url="https://example.com/final")

        result = wget("https://example.com", as_dict=True)

        assert result["body"] == b"hello"
        assert result["code"] == 201
        assert result["url"] == "https://example.com/final"

    def test_gzip_encoded_body_is_transparently_decompressed(self, fake_opener):
        fake_opener.open.return_value = _FakeResponse(gzip.compress(b"hello world"))

        assert wget("https://example.com") == b"hello world"

    def test_headers_combine_accept_encoding_user_agent_and_referer(self, fake_opener):
        fake_opener.open.return_value = _FakeResponse(b"ok")

        wget("https://example.com", accept_encoding="gzip", user_agent="custom-agent", referer="http://ref.example.com")

        headers = dict(fake_opener.addheaders)
        assert headers["Accept-Encoding"] == "gzip"
        assert headers["User-Agent"] == "custom-agent"
        assert headers["Referer"] == "http://ref.example.com"

    def test_none_user_agent_omits_the_header(self, fake_opener):
        fake_opener.open.return_value = _FakeResponse(b"ok")

        wget("https://example.com", user_agent=None)

        headers = dict(fake_opener.addheaders)
        assert "User-Agent" not in headers

    def test_url_error_is_retried_until_success(self, fake_opener):
        fake_opener.open.side_effect = [urllib.error.URLError("boom"), _FakeResponse(b"ok")]

        assert wget("https://example.com", num_tries=2) == b"ok"
        assert fake_opener.open.call_count == 2

    def test_url_error_is_raised_as_wgeterror_once_retries_are_exhausted(self, fake_opener):
        fake_opener.open.side_effect = urllib.error.URLError("boom")

        with pytest.raises(WgetError, match="failed"):
            wget("https://example.com", num_tries=2)

        assert fake_opener.open.call_count == 2

    def test_single_try_raises_immediately_on_url_error_without_retrying(self, fake_opener):
        fake_opener.open.side_effect = urllib.error.URLError("boom")

        with pytest.raises(WgetError, match="failed"):
            wget("https://example.com")

        assert fake_opener.open.call_count == 1

    def test_retry_after_url_error_preserves_a_custom_user_agent(self, fake_opener):
        fake_opener.open.side_effect = [urllib.error.URLError("boom"), _FakeResponse(b"ok")]

        wget("https://example.com", user_agent="custom-agent", num_tries=2)

        headers = dict(fake_opener.addheaders)
        assert headers["User-Agent"] == "custom-agent"


class _FakeHttpResponse:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class TestWgetNonGet:
    def test_as_dict_is_rejected_for_non_get_requests(self):
        with pytest.raises(WgetError, match="as_dict can only be True for GETs"):
            wget("https://example.com/path", request_type="POST", as_dict=True)

    def test_invalid_hostname_raises(self):
        with pytest.raises(WgetError, match="Invalid hostname"):
            wget("not-a-url", request_type="POST")

    def test_https_url_uses_https_connection_on_port_443_by_default(self, monkeypatch):
        fake_conn = mock.Mock()
        fake_conn.getresponse.return_value = _FakeHttpResponse(b"created")
        https_cls = mock.Mock(return_value=fake_conn)
        monkeypatch.setattr(http.client, "HTTPSConnection", https_cls)

        result = wget("https://example.com/path", request_type="POST", body="payload")

        https_cls.assert_called_once_with("example.com", port=443, timeout=mock.ANY)
        fake_conn.request.assert_called_once_with("POST", "/path", "payload", {"User-Agent": wget_module.USER_AGENT})
        assert result == b"created"

    def test_http_url_uses_http_connection_on_port_80_by_default(self, monkeypatch):
        fake_conn = mock.Mock()
        fake_conn.getresponse.return_value = _FakeHttpResponse(b"created")
        http_cls = mock.Mock(return_value=fake_conn)
        monkeypatch.setattr(http.client, "HTTPConnection", http_cls)

        wget("http://example.com/path", request_type="PUT")

        http_cls.assert_called_once_with("example.com", port=80, timeout=mock.ANY)

    def test_explicit_port_in_the_url_is_parsed_and_used(self, monkeypatch):
        fake_conn = mock.Mock()
        fake_conn.getresponse.return_value = _FakeHttpResponse(b"created")
        http_cls = mock.Mock(return_value=fake_conn)
        monkeypatch.setattr(http.client, "HTTPConnection", http_cls)

        result = wget("http://example.com:8080/path", request_type="DELETE")

        http_cls.assert_called_once_with("example.com", port=8080, timeout=mock.ANY)
        assert result == b"created"
