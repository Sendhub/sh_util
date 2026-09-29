"""
Unit tests for the behaviour ported into bw_util from inforeach: release circuit breaker,
country/inventory resolution, messaging error-code lookup, number-info cache, pagination
link parsing and order polling.
"""

import itertools
from unittest.mock import MagicMock, patch

import pytest

from .. import bw_util, release_breaker
from ..bw_util import BandwidthReleaseCircuitOpenError, SHBandwidthClient
from .test_bw import bw_client, mock_settings  # noqa: F401  (shared fixtures)


class FakeRedis:
    def __init__(self):
        self.store = {}

    def exists(self, key):
        return 1 if key in self.store else 0

    def delete(self, key):
        self.store.pop(key, None)

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    def set(self, key, value, ex=None):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)

    def ttl(self, key):
        return 100 if key in self.store else -2


class TestReleaseBreakerWithoutRedis:
    def test_is_never_open_and_calls_are_noops(self):
        assert release_breaker.is_breaker_open("bandwidth") is False
        assert release_breaker.record_failure("bandwidth") is None
        assert release_breaker.record_success("bandwidth") is None
        assert release_breaker.breaker_ttl("bandwidth") is None
        assert release_breaker.failure_count("bandwidth") == 0
        assert release_breaker.reset_breaker("bandwidth") is False


class TestReleaseBreakerWithRedis:
    @pytest.fixture
    def redis(self, monkeypatch):
        fake = FakeRedis()
        monkeypatch.setattr(release_breaker.settings, "REDIS", fake, raising=False)
        return fake

    def test_opens_after_threshold_consecutive_failures(self, redis):
        for _ in range(release_breaker.CONSECUTIVE_FAILURE_THRESHOLD - 1):
            release_breaker.record_failure("bandwidth")
        assert release_breaker.is_breaker_open("bandwidth") is False

        release_breaker.record_failure("bandwidth")

        assert release_breaker.is_breaker_open("bandwidth") is True
        assert release_breaker.breaker_ttl("bandwidth") == 100

    def test_success_clears_failure_count(self, redis):
        release_breaker.record_failure("bandwidth")
        assert release_breaker.failure_count("bandwidth") == 1

        release_breaker.record_success("bandwidth")

        assert release_breaker.failure_count("bandwidth") == 0

    def test_reset_closes_an_open_breaker(self, redis):
        for _ in range(release_breaker.CONSECUTIVE_FAILURE_THRESHOLD):
            release_breaker.record_failure("bandwidth")

        assert release_breaker.reset_breaker("bandwidth") is True
        assert release_breaker.is_breaker_open("bandwidth") is False

    def test_redis_errors_fail_open(self, monkeypatch):
        broken = MagicMock()
        broken.exists.side_effect = ConnectionError("redis down")
        broken.incr.side_effect = ConnectionError("redis down")
        monkeypatch.setattr(release_breaker.settings, "REDIS", broken, raising=False)

        assert release_breaker.is_breaker_open("bandwidth") is False
        assert release_breaker.record_failure("bandwidth") is None


class TestReleaseUsesBreaker:
    def test_open_breaker_skips_the_carrier_call(self, bw_client):  # noqa: F811
        with patch.object(bw_util, "is_breaker_open", return_value=True), patch("requests.post") as mock_post:
            with pytest.raises(BandwidthReleaseCircuitOpenError):
                bw_client.release_phone_number("+14155551234")

        mock_post.assert_not_called()

    @patch("requests.get")
    @patch("requests.post")
    def test_failed_release_is_recorded(self, mock_post, mock_get, bw_client):  # noqa: F811
        mock_get.return_value = MagicMock(status_code=200, text="<Site><Id>100200</Id></Site>")
        mock_post.return_value = MagicMock(status_code=500, text="boom")

        with patch.object(bw_util, "record_failure") as record_failure, patch.object(bw_util, "record_success") as record_success:
            with pytest.raises(RuntimeError):
                bw_client.release_phone_number("+14155551234")

        record_failure.assert_called_once_with("bandwidth")
        record_success.assert_not_called()

    @patch("requests.get")
    @patch("requests.post")
    def test_successful_release_is_recorded(self, mock_post, mock_get, bw_client):  # noqa: F811
        mock_get.return_value = MagicMock(status_code=200, text="<Site><Id>100200</Id></Site>")
        mock_post.return_value = MagicMock(status_code=200, text="<DisconnectTelephoneNumberOrderResponse></DisconnectTelephoneNumberOrderResponse>")

        with patch.object(bw_util, "record_failure") as record_failure, patch.object(bw_util, "record_success") as record_success:
            bw_client.release_phone_number("+14155551234")

        record_success.assert_called_once_with("bandwidth")
        record_failure.assert_not_called()


class TestInventoryCountryResolution:
    def test_service_without_supported_countries_allows_every_wired_region(self):
        with patch.object(bw_util, "settings", MagicMock(spec=[])):
            assert bw_util.resolve_bandwidth_inventory_country("us") == "US"
            assert bw_util.resolve_bandwidth_inventory_country("AU") == "AU"

    def test_configured_supported_countries_restrict_the_regions(self):
        with patch.object(bw_util, "settings", MagicMock(SUPPORTED_COUNTRIES=("US", "CA"))):
            assert bw_util.resolve_bandwidth_inventory_country("CA") == "CA"
            with pytest.raises(ValueError, match="not enabled"):
                bw_util.resolve_bandwidth_inventory_country("AU")

    def test_unwired_region_is_rejected_even_when_configured(self):
        with patch.object(bw_util, "settings", MagicMock(SUPPORTED_COUNTRIES=("US", "GB"))):
            with pytest.raises(ValueError, match="not integrated"):
                bw_util.resolve_bandwidth_inventory_country("GB")

    def test_country_code_is_required(self):
        with patch.object(bw_util, "settings", MagicMock(spec=[])):
            with pytest.raises(ValueError, match="required"):
                bw_util.resolve_bandwidth_inventory_country(None)


class TestMessagingErrorCodes:
    def test_reference_file_ships_with_sh_util(self):
        assert bw_util._load_messaging_error_codes()

    def test_unset_or_unknown_codes_return_none(self):
        assert bw_util.lookup_messaging_error_code(None) is None
        assert bw_util.lookup_messaging_error_code("") is None
        assert bw_util.lookup_messaging_error_code("not-a-code") is None

    def test_known_code_is_described(self):
        reference = bw_util._load_messaging_error_codes()
        category, entries = next((c, e) for c, e in reference.items() if not c.startswith("_") and isinstance(e, dict))
        code = next(iter(entries))

        info = bw_util.lookup_messaging_error_code(code)

        assert info["code"] == code
        assert info["category"] == category


class TestNumberInfoCache:
    @patch("requests.get")
    def test_second_lookup_is_served_from_cache(self, mock_get, bw_client):  # noqa: F811
        mock_get.return_value = MagicMock(status_code=200, text="<TelephoneNumberResponse><Status>Active</Status></TelephoneNumberResponse>")

        first = bw_client.get_number_info("+14155551234")
        second = bw_client.get_number_info("+14155551234")

        assert first == second
        assert mock_get.call_count == 1

    @patch("requests.get")
    def test_use_cache_false_and_clear_force_a_refetch(self, mock_get, bw_client):  # noqa: F811
        mock_get.return_value = MagicMock(status_code=200, text="<TelephoneNumberResponse><Status>Active</Status></TelephoneNumberResponse>")

        bw_client.get_number_info("+14155551234")
        bw_client.get_number_info("+14155551234", use_cache=False)
        bw_client.clear_number_info_cache()
        bw_client.get_number_info("+14155551234")

        assert mock_get.call_count == 3


class TestParseLinkHeader:
    def test_extracts_query_string(self, bw_client):  # noqa: F811
        assert bw_client._parse_link_header("<https://x.test/inserviceNumbers?page=2&size=3>") == "?page=2&size=3"

    @pytest.mark.parametrize("value", [None, "", {}, "<https://x.test/inserviceNumbers>"])
    def test_returns_none_without_a_usable_link(self, bw_client, value):  # noqa: F811
        assert bw_client._parse_link_header(value) is None


class TestPollOrderStatus:
    def test_returns_number_when_order_completes(self, bw_client):  # noqa: F811
        with patch.object(bw_client, "fetch_placed_purchased_order_details", return_value="+14155551234"), patch("time.sleep"):
            assert bw_client._poll_order_status("order_1") == "+14155551234"

    def test_keeps_polling_while_order_is_pending(self, bw_client):  # noqa: F811
        pending = {"code": "5019", "description": "pending"}
        with (
            patch.object(bw_client, "fetch_placed_purchased_order_details", side_effect=[pending, pending, "+14155551234"]) as fetch,
            patch("time.sleep") as sleep,
        ):
            assert bw_client._poll_order_status("order_1") == "+14155551234"

        assert fetch.call_count == 3
        assert sleep.call_count == 2

    def test_returns_error_details_for_a_failed_order(self, bw_client):  # noqa: F811
        failed = {"code": "1234", "description": "rejected"}
        with patch.object(bw_client, "fetch_placed_purchased_order_details", return_value=failed), patch("time.sleep"):
            assert bw_client._poll_order_status("order_1") == failed

    def test_gives_up_after_timeout(self, bw_client):  # noqa: F811
        clock = itertools.count(0, 10)  # every time.time() call (logging included) advances the clock 10s
        with patch.object(bw_client, "fetch_placed_purchased_order_details", return_value=None), patch("time.sleep"), patch("time.time", side_effect=lambda: next(clock)):
            assert bw_client._poll_order_status("order_1", timeout=30) is None

    def test_lookup_errors_end_polling(self, bw_client):  # noqa: F811
        with patch.object(bw_client, "fetch_placed_purchased_order_details", side_effect=Exception("boom")), patch("time.sleep"):
            assert bw_client._poll_order_status("order_1") is None


def test_client_class_is_exported():
    assert bw_util.SHBandwidthClient is SHBandwidthClient
