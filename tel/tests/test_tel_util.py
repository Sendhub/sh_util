"""
Unit tests for twilio_util module.

Test coverage includes:
- twilio_find_number_in_area_code: default/custom country code, only_list flag, exceptions
- twilio_buy_phone_number: area code purchase, specific number purchase, exceptions
- AreaCodeUnavailableError: custom exception behavior
"""

from unittest.mock import MagicMock, patch

import pytest

from .. import tel_util
from ..tel_util import (
    BuyPhoneNumberFromCarrier,
    FindPhoneNumberInAreaCode,
    ReleaseNumberSafely,
    SHBoughtNumberObject,
)
from ..twilio_util import (
    AreaCodeUnavailableError,
    twilio_buy_phone_number,
    twilio_find_number_in_area_code,
)
from ..bw_util import BWTollFreeUnavailableError


@pytest.fixture
def mock_twilio_client():
    client = MagicMock()
    client.username = "AC_test_account"
    return client


@pytest.fixture
def mock_phone_number():
    phone = MagicMock()
    phone.phone_number = "+14155551234"
    phone.sid = "PN1234567890abcdef1234567890abcdef"
    return phone


class TestTwilioFindNumberInAreaCode:
    def test_find_number_with_area_code_returns_results(self, mock_twilio_client, mock_phone_number):
        local_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.local.list
        local_list.return_value = [mock_phone_number]

        result = twilio_find_number_in_area_code(mock_twilio_client, "415", "US")

        assert result == [mock_phone_number]
        mock_twilio_client.api.v2010.accounts.assert_called_once_with(mock_twilio_client.username)
        mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.assert_called_once_with("US")
        local_list.assert_called_once_with(area_code="415", limit=6)

    def test_find_number_custom_country_code(self, mock_twilio_client, mock_phone_number):
        local_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.local.list
        local_list.return_value = [mock_phone_number]

        twilio_find_number_in_area_code(mock_twilio_client, "020", "GB")

        mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.assert_called_once_with("GB")

    def test_find_number_custom_max_limit(self, mock_twilio_client, mock_phone_number):
        local_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.local.list
        local_list.return_value = [mock_phone_number]

        twilio_find_number_in_area_code(mock_twilio_client, "415", "US", max_limit=10)

        local_list.assert_called_once_with(area_code="415", limit=10)

    def test_find_number_only_list_returns_phone_number_strings(self, mock_twilio_client, mock_phone_number):
        local_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.local.list
        local_list.return_value = [mock_phone_number]

        result = twilio_find_number_in_area_code(mock_twilio_client, "415", "US", only_list=True)

        assert result == [mock_phone_number.phone_number]

    def test_find_number_exception_raises_area_code_unavailable_error(self, mock_twilio_client):
        local_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.local.list
        local_list.side_effect = Exception("carrier error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_find_number_in_area_code(mock_twilio_client, "415", "US")

        assert "having problems finding phone numbers" in str(exc_info.value)


class TestTwilioBuyPhoneNumber:
    @patch("utils.sh_util.tel.twilio_util.twilio_find_number_in_area_code")
    def test_buy_number_with_area_code_success(self, mock_find_number, mock_twilio_client, mock_phone_number):
        mock_find_number.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilio_buy_phone_number(mock_twilio_client, "AP_app_sid", area_code="415")

        assert result == mock_phone_number
        mock_find_number.assert_called_once_with(mock_twilio_client, "415", country_code="US", max_limit=1)
        mock_twilio_client.incoming_phone_numbers.create.assert_called_once_with(
            phone_number=[mock_phone_number], sms_application_sid="AP_app_sid", voice_application_sid="AP_app_sid"
        )

    def test_buy_specific_phone_number_success(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilio_buy_phone_number(mock_twilio_client, "AP_app_sid", phone_number="+14155551234")

        assert result == mock_phone_number
        mock_twilio_client.incoming_phone_numbers.create.assert_called_once_with(
            phone_number="+14155551234", sms_application_sid="AP_app_sid", voice_application_sid="AP_app_sid"
        )

    @patch("utils.sh_util.tel.twilio_util.twilio_find_number_in_area_code")
    def test_buy_number_with_area_code_exception_raises_area_code_unavailable_error(self, mock_find_number, mock_twilio_client, mock_phone_number):
        mock_find_number.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.side_effect = Exception("carrier error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_phone_number(mock_twilio_client, "AP_app_sid", area_code="415")

        assert "having problems buying phone numbers" in str(exc_info.value)

    def test_buy_specific_phone_number_exception_raises_area_code_unavailable_error(self, mock_twilio_client):
        mock_twilio_client.incoming_phone_numbers.create.side_effect = Exception("carrier error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_phone_number(mock_twilio_client, "AP_app_sid", phone_number="+14155551234")

        assert "having problems buying phone numbers" in str(exc_info.value)

    def test_buy_number_no_area_code_no_phone_number_raises_error(self, mock_twilio_client):
        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_phone_number(mock_twilio_client, "AP_app_sid")

        assert "No available numbers left" in str(exc_info.value)

    @patch("utils.sh_util.tel.twilio_util.twilio_find_number_in_area_code")
    def test_buy_number_no_numbers_available_raises_error(self, mock_find_number, mock_twilio_client):
        mock_find_number.return_value = None

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_phone_number(mock_twilio_client, "AP_app_sid", area_code="999")

        assert "No available numbers left" in str(exc_info.value)


class TestAreaCodeUnavailableError:
    def test_custom_exception_can_be_raised(self):
        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            raise AreaCodeUnavailableError("Test message")

        assert str(exc_info.value) == "Test message"

    def test_custom_exception_inheritance(self):
        assert issubclass(AreaCodeUnavailableError, Exception)


# ---------------------------------------------------------------------------
# Tests for tel_util.py (SHBoughtNumberObject, ReleaseNumberSafely,
# BuyPhoneNumberFromCarrier, FindPhoneNumberInAreaCode) and tel/__init__.py
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_tel_settings():
    """Mocks the `settings` module as imported into tel_util.py."""
    with patch.object(tel_util, "settings") as mock_settings:
        mock_settings.SMS_GATEWAY_TWILIO = "twilio"
        mock_settings.SMS_GATEWAY_BANDWIDTH = "bandwidth"
        mock_settings.SUPPORTED_GATEWAYS = ["twilio", "bandwidth"]
        mock_settings.TWILIO_CLIENT = MagicMock()
        mock_settings.TWILIO_APP_SID = "APP_SID"
        mock_settings.TWILIO_APP_SID_STAGING = "APP_SID_STAGING"
        mock_settings.BW_SITE_ID = "site_123"
        yield mock_settings


class TestSHBoughtNumberObject:
    def test_initialization(self):
        obj = SHBoughtNumberObject("+14155551234", "sid_abc", "bandwidth")

        assert obj.phone_number == "+14155551234"
        assert obj.sid == "sid_abc"
        assert obj.gateway == "bandwidth"


class TestReleaseNumberSafelyCall:
    def test_twilio_gateway_routes_to_twilio_release(self, mock_tel_settings):
        releaser = ReleaseNumberSafely("+14155551234", "twilio", "sid_abc")
        with patch.object(releaser, "_twilio_safe_number_release", return_value=True) as mock_release:
            result = releaser()

        assert result is True
        mock_release.assert_called_once()

    def test_bandwidth_gateway_routes_to_bandwidth_release(self, mock_tel_settings):
        releaser = ReleaseNumberSafely("+14155551234", "bandwidth", "sid_abc")
        with patch.object(releaser, "_bandwidth_safe_number_release", return_value=True) as mock_release:
            result = releaser()

        assert result is True
        mock_release.assert_called_once()

    def test_invalid_gateway_returns_false(self, mock_tel_settings):
        releaser = ReleaseNumberSafely("+14155551234", "invalid_gateway", "sid_abc")

        result = releaser()

        assert result is False


class TestReleaseNumberSafelyTwilioRelease:
    def test_matching_app_sid_deletes_number(self, mock_tel_settings):
        nbr_object = MagicMock()
        nbr_object.voice_application_sid = "APP_SID"
        nbr_object.phone_number = "+14155551234"
        mock_tel_settings.TWILIO_CLIENT.phone_numbers.get.return_value = nbr_object

        releaser = ReleaseNumberSafely("+14155551234", "twilio", "sid_abc")
        result = releaser._twilio_safe_number_release()

        assert result is True
        nbr_object.delete.assert_called_once()

    def test_mismatched_app_sid_does_not_delete(self, mock_tel_settings):
        nbr_object = MagicMock()
        nbr_object.voice_application_sid = "SOME_OTHER_SID"
        mock_tel_settings.TWILIO_CLIENT.phone_numbers.get.return_value = nbr_object

        releaser = ReleaseNumberSafely("+14155551234", "twilio", "sid_abc")
        result = releaser._twilio_safe_number_release()

        assert result is False
        nbr_object.delete.assert_not_called()

    def test_exception_is_caught_and_returns_false(self, mock_tel_settings):
        mock_tel_settings.TWILIO_CLIENT.phone_numbers.get.side_effect = Exception("twilio down")

        releaser = ReleaseNumberSafely("+14155551234", "twilio", "sid_abc")
        result = releaser._twilio_safe_number_release()

        assert result is False


class TestReleaseNumberSafelyBandwidthRelease:
    def test_success_returns_true(self, mock_tel_settings):
        releaser = ReleaseNumberSafely("+14155551234", "bandwidth", "sid_abc")
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls:
            mock_client_cls.return_value.release_phone_number.return_value = None
            result = releaser._bandwidth_safe_number_release()

        assert result is True

    def test_exception_is_caught_and_returns_false(self, mock_tel_settings):
        releaser = ReleaseNumberSafely("+14155551234", "bandwidth", "sid_abc")
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls:
            mock_client_cls.return_value.release_phone_number.side_effect = Exception("bw down")
            result = releaser._bandwidth_safe_number_release()

        assert result is False


class TestBuyPhoneNumberFromCarrierSendhubBuyNumber:
    def test_twilio_gateway_routes_to_twilio_buy(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(buyer, "_twilio_buy_number", return_value="bought") as mock_buy:
            result = buyer._sendhub_buy_number("twilio", "sid_abc", "415", "US", None, False, None)

        assert result == "bought"
        mock_buy.assert_called_once()

    def test_bandwidth_gateway_routes_to_bandwidth_buy(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(buyer, "_bandwidth_buy_number", return_value="bought") as mock_buy:
            result = buyer._sendhub_buy_number("bandwidth", "sid_abc", "415", "US", None, False, "user1")

        assert result == "bought"
        mock_buy.assert_called_once()

    def test_invalid_gateway_returns_none(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()

        result = buyer._sendhub_buy_number("invalid_gateway", "sid_abc", "415", "US", None, False, None)

        assert result is None


class TestBuyPhoneNumberFromCarrierBandwidthBuyNumber:
    def test_toll_free_number_purchase_raises_unbound_local_error(self, mock_tel_settings):
        """
        PRE-EXISTING BUG (not fixed): the toll-free branch of
        _bandwidth_buy_number only assigns `number` (not `sid`), but the
        function unconditionally returns `BandwidthNumberObject(number, sid)`.
        Buying a toll-free Bandwidth number therefore always raises
        UnboundLocalError instead of returning a valid number object.
        """
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls, patch.object(tel_util, "BandwidthNumberObject") as mock_obj_cls:
            mock_client_cls.return_value.buy_toll_free_number.return_value = "+18005551234"
            mock_obj_cls.return_value = "bandwidth_number_object"

            with pytest.raises(UnboundLocalError):
                buyer._bandwidth_buy_number("415", "US", None, True, "user1")

        mock_client_cls.return_value.buy_toll_free_number.assert_called_once_with(quantity=1, pattern="415", site_id="site_123", user_id="user1")

    def test_regular_number_purchase(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls, patch.object(tel_util, "BandwidthNumberObject") as mock_obj_cls:
            mock_client_cls.return_value.buy_phone_number.return_value = ("+14155551234", "sid_xyz")
            mock_obj_cls.return_value = "bandwidth_number_object"

            result = buyer._bandwidth_buy_number("415", "US", None, False, "user1")

        assert result == "bandwidth_number_object"
        mock_obj_cls.assert_called_once_with("+14155551234", "sid_xyz")


class TestBuyPhoneNumberFromCarrierTwilioBuyNumber:
    def test_toll_free_delegates_to_twilio_buy_toll_free(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(tel_util, "twilio_buy_toll_free_phone_number", return_value="+18005551234") as mock_buy:
            result = buyer._twilio_buy_number("sid_abc", "415", "US", None, True)

        assert result == "+18005551234"
        mock_buy.assert_called_once_with(twilio_client=mock_tel_settings.TWILIO_CLIENT, app_sid="APP_SID_STAGING", pattern="415", country_code="US", phone_number=None)

    def test_regular_delegates_to_twilio_buy_phone_number(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(tel_util, "twilio_buy_phone_number", return_value="+14155551234") as mock_buy:
            result = buyer._twilio_buy_number("sid_abc", "415", "US", None, False)

        assert result == "+14155551234"
        mock_buy.assert_called_once_with(twilio_client=mock_tel_settings.TWILIO_CLIENT, app_sid="sid_abc", area_code="415", country_code="US", phone_number=None)


class TestBuyPhoneNumberFromCarrierCall:
    def test_invalid_gateway_raises_area_code_unavailable_error(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()

        with pytest.raises(AreaCodeUnavailableError):
            buyer("invalid_gateway", "sid_abc")

    def test_success_returns_sh_bought_number_object(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        bought = SHBoughtNumberObject("+14155551234", "sid_abc", "bandwidth")
        with patch.object(buyer, "_sendhub_buy_number", return_value=bought):
            result = buyer("bandwidth", "sid_abc", area_code="415")

        assert result is bought

    def test_no_alt_gateway_reraises_area_code_error(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(buyer, "_sendhub_buy_number", side_effect=AreaCodeUnavailableError("no numbers")):
            with pytest.raises(AreaCodeUnavailableError):
                buyer("bandwidth", "sid_abc", area_code="415", alt_gateway=False)

    def test_alt_gateway_success_on_fallback(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        bought = SHBoughtNumberObject("+14155551234", "sid_abc", "twilio")
        with patch.object(buyer, "_sendhub_buy_number", side_effect=[AreaCodeUnavailableError("no bw numbers"), bought]):
            result = buyer("bandwidth", "sid_abc", area_code="415", alt_gateway=True)

        assert result is bought

    def test_alt_gateway_all_fail_raises_area_code_error(self, mock_tel_settings):
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(
            buyer,
            "_sendhub_buy_number",
            side_effect=[
                AreaCodeUnavailableError("no bw numbers"),
                AreaCodeUnavailableError("no twilio numbers either"),
            ],
        ):
            with pytest.raises(AreaCodeUnavailableError):
                buyer("bandwidth", "sid_abc", area_code="415", alt_gateway=True)

    def test_no_exception_no_number_returns_none(self, mock_tel_settings):
        # _sendhub_buy_number returns None with no exception raised. The `try`
        # block's own `return nbr_obj` fires unconditionally in this case (it
        # is not gated on nbr_obj being truthy), so __call__ returns None here
        # rather than reaching the `else` clause's isinstance check or the
        # final `raise AreaCodeUnavailableError` below it - both of which are
        # unreachable dead code on the non-exception path.
        buyer = BuyPhoneNumberFromCarrier()
        with patch.object(buyer, "_sendhub_buy_number", return_value=None):
            result = buyer("bandwidth", "sid_abc", area_code="415")

        assert result is None


class TestFindPhoneNumberInAreaCodeCall:
    def test_twilio_regular_search_success(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "twilio_find_number_in_area_code", return_value=["num1", "num2"]) as mock_find:
            result = finder("twilio", area_code="415")

        assert result == ["num1", "num2"]
        mock_find.assert_called_once()

    def test_twilio_regular_search_exception_returns_empty_list(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "twilio_find_number_in_area_code", side_effect=AreaCodeUnavailableError("no numbers")):
            result = finder("twilio", area_code="415")

        assert result == []

    def test_twilio_toll_free_search_success(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "twilio_find_toll_free_number_in_area_code", return_value=["+18005551234"]) as mock_find:
            result = finder("twilio", toll_free=True, toll_free_area_code="800")

        assert result == ["+18005551234"]
        mock_find.assert_called_once()

    def test_twilio_toll_free_search_exception_returns_empty_list(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "twilio_find_toll_free_number_in_area_code", side_effect=AreaCodeUnavailableError("no toll free")):
            result = finder("twilio", toll_free=True, toll_free_area_code="800")

        assert result == []

    def test_bandwidth_regular_search_success(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls, patch.object(tel_util, "BandwidthAvailablePhoneNumber", side_effect=lambda n: f"wrapped:{n}"):
            mock_client_cls.return_value.find_number_in_area_code.return_value = ["+14155551234"]
            result = finder("bandwidth", area_code="415")

        assert result == ["wrapped:+14155551234"]

    def test_bandwidth_regular_search_non_list_result_is_wrapped(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls, patch.object(tel_util, "BandwidthAvailablePhoneNumber", side_effect=lambda n: f"wrapped:{n}"):
            mock_client_cls.return_value.find_number_in_area_code.return_value = "+14155551234"
            result = finder("bandwidth", area_code="415")

        assert result == ["wrapped:+14155551234"]

    def test_bandwidth_regular_search_exception_returns_empty_list(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls:
            mock_client_cls.return_value.find_number_in_area_code.side_effect = AreaCodeUnavailableError("no numbers")
            result = finder("bandwidth", area_code="415")

        assert result == []

    def test_bandwidth_toll_free_search_success(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls, patch.object(tel_util, "BandwidthAvailablePhoneNumber", side_effect=lambda n: f"wrapped:{n}"):
            mock_client_cls.return_value.search_available_toll_free_number.return_value = "+18005551234"
            result = finder("bandwidth", toll_free=True, toll_free_area_code="800")

        assert result == ["wrapped:+18005551234"]

    def test_bandwidth_toll_free_search_exception_returns_empty_list(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()
        with patch.object(tel_util, "SHBandwidthClient") as mock_client_cls:
            mock_client_cls.return_value.search_available_toll_free_number.side_effect = BWTollFreeUnavailableError("none")
            result = finder("bandwidth", toll_free=True, toll_free_area_code="800")

        assert result == []

    def test_invalid_gateway_returns_empty_list(self, mock_tel_settings):
        finder = FindPhoneNumberInAreaCode()

        result = finder("invalid_gateway")

        assert result == []


# ---------------------------------------------------------------------------
# Tests for tel/__init__.py: is_send_hub_number
# ---------------------------------------------------------------------------


class TestIsSendHubNumber:
    def test_invalid_input_returns_false_without_querying_db(self):
        import sh_util.tel as tel_pkg

        with patch.object(tel_pkg, "_db_query") as mock_query:
            result = tel_pkg.is_send_hub_number("not-a-number; DROP TABLE")

        assert result is False
        mock_query.assert_not_called()

    def test_number_found_in_db_returns_true(self):
        import sh_util.tel as tel_pkg

        with patch.object(tel_pkg, "_db_query") as mock_query:
            mock_query.return_value = [{"number": "+14155551234"}]
            result = tel_pkg.is_send_hub_number("+14155551234")

        assert result is True
        mock_query.assert_called_once()

    def test_number_not_found_in_db_returns_false(self):
        import sh_util.tel as tel_pkg

        with patch.object(tel_pkg, "_db_query") as mock_query:
            mock_query.return_value = []
            result = tel_pkg.is_send_hub_number("+14155551234")

        assert result is False
