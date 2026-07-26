"""
Unit tests for bw_util module.

Test coverage includes:
- phonenumber_as_e164: E.164 formatting, invalid inputs, country codes
- BandwidthAvailablePhoneNumber: initialization and attributes
- BandwidthNumberObject: initialization and attributes
- SHBandwidthClient: initialization, credentials, authentication
- SHBandwidthClient methods: SMS/MMS sending, number search/purchase/release
- Exception handling: all custom exceptions
- Input validation: E.164 format, recipient lists, tags, media URLs
- All public methods with happy paths and error cases
"""

import base64
from unittest.mock import MagicMock, patch

import pytest

from .. import bw_util
from ..bw_util import (
    BandwidthAvailablePhoneNumber,
    BandwidthNumberObject,
    BandwidthOrderPendingException,
    BWLengthOfMediaURLLimitExceededException,
    BWMessageCharacterLimitExceededException,
    BWNumberUnavailableError,
    BWTagCharacterLimitExceededException,
    BWTollFreeUnavailableError,
    SHBandwidthClient,
    phonenumber_as_e164,
)
from ..twilio_util import AreaCodeUnavailableError


@pytest.fixture
def mock_settings():
    with patch.object(bw_util, "settings") as mock_settings:
        mock_settings.BW_USER_ID = "test_user_id"
        mock_settings.BW_API_TOKEN = "test_token"
        mock_settings.BW_API_SECRET = "test_secret"
        mock_settings.BW_USERNAME = "test_username"
        mock_settings.BW_PASSWORD = "test_password"
        mock_settings.BW_APP_ID = "test_app_id"
        mock_settings.BW_USER_ID_AU = "test_user_id_au"
        mock_settings.BW_ACCOUNT_API_URL = "https://api.test.com"
        mock_settings.BW_ACCOUNT_API_URL_AU = "https://api.test.au"
        mock_settings.BW_SITE_ID = "test_site_id"
        mock_settings.BW_SITE_ID_AU = "test_site_id_au"
        mock_settings.SMS_GATEWAY_BANDWIDTH = "bandwidth"
        # SHBandwidthClient.__init__ uses getattr(settings, "BW_USE_OAUTH2", False);
        # since mock_settings is a MagicMock, unset attributes auto-vivify as truthy
        # Mocks instead of falling back to the getattr default, so this must be
        # set explicitly or __init__ silently takes the OAuth2 branch instead of
        # the intended username/password (Basic Auth) branch.
        mock_settings.BW_USE_OAUTH2 = False
        mock_settings.BW_CLIENT_ID = None
        mock_settings.BW_CLIENT_SECRET = None
        yield mock_settings


@pytest.fixture
def bw_client(mock_settings):
    with patch("bandwidth.Configuration"):
        client = SHBandwidthClient(userid="test_user", token="test_token", secret="test_secret", username="test_user", password="test_pass")
        return client


class TestPhoneNumberAsE164:
    @patch.object(bw_util, "validate_phone_number")
    def test_valid_us_number(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164("4155551234", "US")

        assert result == "+14155551234"
        mock_validate.assert_called_once_with("4155551234", False)

    @patch.object(bw_util, "validate_phone_number")
    def test_valid_number_with_plus(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164("+14155551234", "US")

        assert result == "+14155551234"

    @patch.object(bw_util, "validate_phone_number")
    def test_integer_number(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164(4155551234, "US")

        assert result == "+14155551234"

    @patch.object(bw_util, "validate_phone_number")
    def test_invalid_number_raises_error(self, mock_validate):
        mock_validate.return_value = False

        with pytest.raises(ValueError) as exc_info:
            phonenumber_as_e164("invalid", "US")

        assert "Invalid phone number" in str(exc_info.value)

    @patch.object(bw_util, "validate_phone_number")
    def test_bytes_number(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164(b"4155551234", "US")

        assert result == "+14155551234"

    @patch.object(bw_util, "validate_phone_number")
    def test_custom_country_code(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164("2079460123", "GB")

        assert result.startswith("+44")


class TestBandwidthAvailablePhoneNumber:
    @patch.object(bw_util, "display_number")
    def test_initialization(self, mock_display, mock_settings):
        mock_display.return_value = "(415) 555-1234"

        phone = BandwidthAvailablePhoneNumber("+14155551234")

        assert phone.phone_number == "+14155551234"
        assert phone.friendly_name == "(415) 555-1234"
        assert phone.gateway == "bandwidth"
        mock_display.assert_called_once_with("+14155551234")


class TestBandwidthNumberObject:
    def test_initialization(self):
        obj = BandwidthNumberObject("+14155551234", "sid_12345")

        assert obj.phone_number == "+14155551234"
        assert obj.sid == "sid_12345"


class TestSHBandwidthClientInit:
    @patch("bandwidth.Configuration")
    def test_init_with_all_params(self, mock_config, mock_settings):
        client = SHBandwidthClient(userid="user_id", token="token", secret="secret", username="username", password="password")

        assert client.token == "token"
        assert client.secret == "secret"
        assert client.username == "username"
        assert client.password == "password"
        assert client.user_id_na == "user_id"
        mock_config.assert_called_once_with(username="username", password="password")

    @patch("bandwidth.Configuration")
    def test_init_with_defaults_from_settings(self, mock_config, mock_settings):
        client = SHBandwidthClient()

        assert client.token == "test_token"
        assert client.secret == "test_secret"
        assert client.username == "test_username"
        assert client.password == "test_password"
        assert client.user_id_na == "test_user_id"
        assert client.bw_app_id == "test_app_id"

    @patch("bandwidth.Configuration")
    def test_init_sets_site_ids(self, mock_config, mock_settings):
        client = SHBandwidthClient()

        assert client.bw_site_id_na == "test_site_id"
        assert client.bw_site_id_au == "test_site_id_au"

    @patch("bandwidth.Configuration")
    def test_init_sets_api_urls(self, mock_config, mock_settings):
        client = SHBandwidthClient()

        assert client.bw_account_api_url_na == "https://api.test.com"
        assert client.bw_account_api_url_au == "https://api.test.au"


class TestSHBandwidthClientCredentials:
    def test_get_encoded_credentials(self, bw_client):
        result = bw_client._get_encoded_credentials()

        expected = base64.b64encode(b"test_user:test_pass").decode("utf-8")
        assert result == expected

    def test_get_common_auth_header(self, bw_client):
        result = bw_client._get_common_auth_header()

        assert "Authorization" in result
        assert result["Authorization"].startswith("Basic ")


class TestSHBandwidthClientAsE164:
    @patch.object(bw_util, "phonenumber_as_e164")
    def test_as_e164_calls_module_function(self, mock_e164):
        mock_e164.return_value = "+14155551234"

        result = SHBandwidthClient._as_e164("+14155551234", "US")

        assert result == "+14155551234"
        mock_e164.assert_called_once_with("+14155551234", "US")


class TestSHBandwidthClientE164Validation:
    def test_valid_e164_format(self, bw_client):
        assert bw_client.check_if_valid_e164_format("+14155551234") is True
        assert bw_client.check_if_valid_e164_format("+442079460123") is True
        assert bw_client.check_if_valid_e164_format("+61212345678") is True

    def test_invalid_e164_format_no_plus(self, bw_client):
        assert bw_client.check_if_valid_e164_format("14155551234") is False

    def test_invalid_e164_format_too_short(self, bw_client):
        assert bw_client.check_if_valid_e164_format("+1234567") is False

    def test_invalid_e164_format_too_long(self, bw_client):
        assert bw_client.check_if_valid_e164_format("+1234567890123456") is False

    def test_invalid_e164_format_starts_with_zero(self, bw_client):
        assert bw_client.check_if_valid_e164_format("+01234567890") is False

    def test_invalid_e164_format_contains_non_digits(self, bw_client):
        assert bw_client.check_if_valid_e164_format("+1415abc1234") is False

    def test_invalid_e164_format_not_string(self, bw_client):
        assert bw_client.check_if_valid_e164_format(4155551234) is False


class TestSHBandwidthClientCleanupAndReturnNumbers:
    @patch.object(bw_util, "phonenumber_as_e164")
    def test_single_number_quantity_one(self, mock_e164, bw_client):
        mock_e164.return_value = "+14155551234"

        result = bw_client._cleanup_and_return_numbers(["+14155551234"], 1, "US")

        assert result == "+14155551234"

    @patch.object(bw_util, "phonenumber_as_e164")
    def test_multiple_numbers_quantity_greater_than_one(self, mock_e164, bw_client):
        mock_e164.side_effect = ["+14155551234", "+14155555678"]

        result = bw_client._cleanup_and_return_numbers(["+14155551234", "+14155555678"], 2, "US")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result == ["+14155551234", "+14155555678"]

    @patch.object(bw_util, "phonenumber_as_e164")
    def test_invalid_number_raises_value_error(self, mock_e164, bw_client):
        # phonenumber_as_e164 raising ValueError propagates through _as_e164
        # (which re-raises a bare ValueError) and _cleanup_and_return_numbers
        # (which also re-raises a bare ValueError) - it is not swallowed.
        mock_e164.side_effect = ValueError("Invalid number")

        with pytest.raises(ValueError):
            bw_client._cleanup_and_return_numbers(["invalid"], 1, "US")


class TestSHBandwidthClientParseNumberToBWFormat:
    def test_parse_us_number(self, bw_client):
        result = bw_client._parse_number_to_bw_format("+14155551234", "US")

        assert result == "4155551234"

    def test_parse_number_without_plus(self, bw_client):
        result = bw_client._parse_number_to_bw_format("4155551234", "US")

        assert result == "4155551234"


class TestSHBandwidthClientSendHello:
    # send_hello simply delegates to send_sms and only catches ValueError, so
    # the cleanest way to exercise that contract without re-testing send_sms's
    # own internals (already covered separately) is to mock send_sms directly.
    def test_send_hello_valid_numbers(self, bw_client):
        with patch.object(bw_client, "send_sms") as mock_send_sms:
            mock_send_sms.return_value = "msg_123"

            result = bw_client.send_hello("+14155551234", "+14155555678")

            assert result == "msg_123"
            mock_send_sms.assert_called_once_with("+14155551234", "+14155555678", "Hello from Sendhub through Bandwidth!")

    def test_send_hello_invalid_number_catches_error(self, bw_client):
        with patch.object(bw_client, "send_sms") as mock_send_sms:
            mock_send_sms.side_effect = ValueError("Invalid")

            result = bw_client.send_hello("invalid", "+14155555678")

            assert result is None


class TestSHBandwidthClientCheckMsgStatus:
    def test_check_msg_status_calls_get_message_info(self, bw_client):
        with patch.object(bw_client, "get_message_info") as mock_get_info:
            mock_get_info.return_value = {"status": "delivered"}

            result = bw_client.check_msg_status("msg_123")

            assert result == {"status": "delivered"}
            mock_get_info.assert_called_once_with("msg_123")


class TestSHBandwidthClientCheckRecipientListValidity:
    def test_single_number_not_in_list_raises_type_error(self, bw_client):
        # Real code strictly requires isinstance(numbers, List) - a bare
        # string is not auto-wrapped, it raises TypeError.
        with pytest.raises(TypeError):
            bw_client.check_recipient_list_validity("+14155551234")

    def test_valid_list_of_numbers(self, bw_client):
        numbers = ["+14155551234", "+14155555678"]

        result = bw_client.check_recipient_list_validity(numbers)

        assert result == numbers

    def test_invalid_numbers_filtered_out(self, bw_client):
        numbers = ["+14155551234", "invalid", "+14155555678"]

        result = bw_client.check_recipient_list_validity(numbers)

        assert len(result) == 2
        assert "invalid" not in result

    def test_non_e164_numbers_converted(self, bw_client):
        numbers = ["4155551234"]

        result = bw_client.check_recipient_list_validity(numbers)

        assert result[0].startswith("+")

    def test_empty_list_returns_empty(self, bw_client):
        result = bw_client.check_recipient_list_validity([])

        assert result == []


class TestSHBandwidthClientSendSMS:
    # bw_util.py imports `bandwidth` locally inside each method (not at module
    # scope), so patch.object(module, "bandwidth") can never intercept those
    # calls. Patching the real bandwidth package's classes directly works
    # because the function-local `import bandwidth` resolves to the same
    # module object.
    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_sms_success(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_123"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms("+14155551234", "+14155555678", "Hello")

        assert result == "msg_123"
        mock_api.create_message.assert_called_once()

    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_sms_with_tag(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_123"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms("+14155551234", "+14155555678", "Hello", tag="test_tag")

        assert result == "msg_123"
        mock_message_request_cls.assert_called_once()
        assert mock_message_request_cls.call_args.kwargs["tag"] == "test_tag"

    def test_send_sms_tag_exceeds_limit_raises_exception(self, bw_client):
        long_tag = "x" * 2025

        with pytest.raises(BWTagCharacterLimitExceededException):
            bw_client.send_sms("+14155551234", "+14155555678", "Hello", tag=long_tag)

    def test_send_sms_message_exceeds_limit_raises_exception(self, bw_client):
        long_msg = "x" * 2049

        with pytest.raises(BWMessageCharacterLimitExceededException):
            bw_client.send_sms("+14155551234", "+14155555678", long_msg)

    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_sms_to_multiple_recipients(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_123"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms("+14155551234", ["+14155555678", "+14155559999"], "Hello")

        assert result == "msg_123"


class TestSHBandwidthClientSendMMS:
    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_mms_success(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_123"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_mms("+14155551234", "+14155555678", "Hello", ["http://example.com/image.jpg"])

        assert result == "msg_123"

    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_mms_with_tag(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_123"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_mms("+14155551234", "+14155555678", "Hello", ["http://example.com/image.jpg"], tag="test")

        assert result == "msg_123"

    def test_send_mms_tag_exceeds_limit_raises_exception(self, bw_client):
        long_tag = "x" * 2025

        with pytest.raises(BWTagCharacterLimitExceededException):
            bw_client.send_mms("+14155551234", "+14155555678", "Hello", ["http://example.com/image.jpg"], tag=long_tag)

    def test_send_mms_message_exceeds_limit_raises_exception(self, bw_client):
        long_msg = "x" * 2049

        with pytest.raises(BWMessageCharacterLimitExceededException):
            bw_client.send_mms("+14155551234", "+14155555678", long_msg, ["http://example.com/image.jpg"])

    def test_send_mms_media_url_exceeds_limit_raises_exception(self, bw_client):
        long_url = "http://example.com/" + "x" * 4100

        with pytest.raises(BWLengthOfMediaURLLimitExceededException):
            bw_client.send_mms("+14155551234", "+14155555678", "Hello", [long_url])


class TestSHBandwidthClientGetMessageInfo:
    # Real get_message_info() calls MessagesApi.list_messages(), not get_message().
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_get_message_info_success(self, mock_api_client_cls, mock_messages_api_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_123"
        mock_response.message_status = "DELIVERED"
        mock_api.list_messages.return_value = mock_response

        result = bw_client.get_message_info("msg_123")

        assert result.id == "msg_123"
        assert result.message_status == "DELIVERED"
        mock_api.list_messages.assert_called_once_with(account_id=bw_client.user_id_na, message_id="msg_123")


class TestSHBandwidthClientInService:
    # Real in_service() never touches the bandwidth SDK - it makes a plain
    # requests.get() against the Bandwidth account API and checks status_code.
    @patch("requests.get")
    def test_in_service_number_exists(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = bw_client.in_service("+14155551234")

        assert result is True

    @patch("requests.get")
    def test_in_service_number_not_found(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = bw_client.in_service("+14155551234")

        assert result is False


class TestSHBandwidthClientFindNumberInAreaCode:
    # Real find_number_in_area_code() uses requests.get() + xmltodict against
    # the account API, never the bandwidth SDK / a "client_module" (which
    # doesn't exist on the real bandwidth package).
    @patch("requests.get")
    def test_find_number_in_area_code_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<SearchResult><ResultCount>1</ResultCount>" "<TelephoneNumberList><TelephoneNumber>4155551234</TelephoneNumber></TelephoneNumberList>" "</SearchResult>"
        mock_get.return_value = mock_response

        result = bw_client.find_number_in_area_code("415", 1)

        assert result == "+14155551234"

    @patch("requests.get")
    def test_find_number_in_area_code_no_results_raises_error(self, mock_get, bw_client):
        # When the response has no <SearchResult> element, the real code's
        # dict-chain (.get("SearchResult").get(...)) hits an AttributeError
        # on None, which the broad except re-raises as AreaCodeUnavailableError.
        # BWNumberUnavailableError is raised elsewhere (buy_phone_number's
        # invalid-number path), but not by this method, so it cannot be the
        # real exception here.
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<Empty></Empty>"
        mock_get.return_value = mock_response

        with pytest.raises(AreaCodeUnavailableError):
            bw_client.find_number_in_area_code("999", 1)


class TestSHBandwidthClientSearchAvailableTollFreeNumber:
    @patch("requests.get")
    def test_search_toll_free_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<SearchResult><ResultCount>1</ResultCount>" "<TelephoneNumberList><TelephoneNumber>8005551234</TelephoneNumber></TelephoneNumberList>" "</SearchResult>"
        mock_get.return_value = mock_response

        result = bw_client.search_available_toll_free_number(quantity=1)

        assert result == "+18005551234"

    @patch("requests.get")
    def test_search_toll_free_no_results_returns_none(self, mock_get, bw_client):
        # Real code explicitly `return None` when <SearchResult> is absent -
        # it never raises BWTollFreeUnavailableError (defined but unused
        # anywhere in bw_util.py).
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<Empty></Empty>"
        mock_get.return_value = mock_response

        result = bw_client.search_available_toll_free_number(quantity=1)

        assert result is None


class TestSHBandwidthClientGetNumberInfo:
    @patch("requests.get")
    def test_get_number_info_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<TelephoneNumberResponse><TelephoneNumberDetails>" "<FullNumber>4155551234</FullNumber><Status>Active</Status>" "</TelephoneNumberDetails></TelephoneNumberResponse>"
        mock_get.return_value = mock_response

        result = bw_client.get_number_info("+14155551234")

        assert result is not None
        assert "TelephoneNumberResponse" in result


class TestSHBandwidthClientReleasePhoneNumber:
    # Real release_phone_number() calls requests.post() (not .delete()) and
    # has no explicit `return` statement in either branch, so it always
    # returns None - it never returns True/False.
    @patch("requests.post")
    def test_release_phone_number_success(self, mock_post, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<DisconnectTelephoneNumberOrderResponse></DisconnectTelephoneNumberOrderResponse>"
        mock_post.return_value = mock_response

        result = bw_client.release_phone_number("+14155551234")

        assert result is None
        mock_post.assert_called_once()

    @patch("requests.post")
    def test_release_phone_number_non_200_response(self, mock_post, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        result = bw_client.release_phone_number("+14155551234")

        assert result is None


class TestSHBandwidthClientGetActiveNumberCount:
    @patch("requests.get")
    def test_get_active_number_count_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        # xmltodict returns string values, not ints.
        mock_response.text = "<Quantity><Count>42</Count></Quantity>"
        mock_get.return_value = mock_response

        result = bw_client.get_active_number_count()

        assert result == "42"


class TestSHBandwidthClientListActiveNumbers:
    @patch("requests.get")
    def test_list_active_numbers_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "<TNs><TotalCount>2</TotalCount><Links><next></next></Links>"
            "<TelephoneNumbers>"
            "<TelephoneNumber>+14155551234</TelephoneNumber>"
            "<TelephoneNumber>+14155555678</TelephoneNumber>"
            "</TelephoneNumbers></TNs>"
        )
        mock_get.return_value = mock_response

        result = bw_client.list_active_numbers(size=10)

        assert len(result) == 2


class TestSHBandwidthClientGetSiteInfoForNumber:
    @patch("requests.get")
    def test_get_siteinfo_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<Site><Id>site_123</Id><Name>Test Site</Name></Site>"
        mock_get.return_value = mock_response

        result = bw_client.get_siteinfo_for_number("+14155551234")

        assert result is not None
        assert result["Id"] == "site_123"


class TestSHBandwidthClientBuyTollFreeNumber:
    # Real buy_toll_free_number() never constructs a BandwidthNumberObject
    # (that class is defined but unused anywhere in bw_util.py) - it returns
    # whatever _cleanup_and_return_numbers() produces, i.e. a plain number
    # string for quantity=1.
    def test_buy_toll_free_number_success(self, bw_client):
        mock_user = MagicMock(id="user_123")
        with patch.object(bw_client, "search_available_toll_free_number") as mock_search, patch("requests.post") as mock_post:
            mock_search.return_value = "+18005551234"
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus></OrderResponse>"
            mock_post.return_value = mock_response

            result = bw_client.buy_toll_free_number(quantity=1, user_id=mock_user)

            assert result == "+18005551234"

    def test_buy_toll_free_number_quantity_below_one_raises_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.buy_toll_free_number(quantity=0)

    def test_buy_toll_free_number_no_results_returns_none(self, bw_client):
        # search_available_toll_free_number() can legitimately return None
        # (see TestSHBandwidthClientSearchAvailableTollFreeNumber); when that
        # happens buy_toll_free_number() returns immediately without ever
        # raising BWTollFreeUnavailableError (unused anywhere in bw_util.py).
        with patch.object(bw_client, "search_available_toll_free_number") as mock_search:
            mock_search.return_value = None

            result = bw_client.buy_toll_free_number(quantity=1, user_id=MagicMock(id="user_123"))

            assert result is None


class TestSHBandwidthClientBuyPhoneNumber:
    # Real buy_phone_number() never constructs a BandwidthNumberObject either;
    # on a successfully completed order it returns a
    # (cleaned_number, order_id) tuple. It also unconditionally calls
    # time.sleep(10) after placing the order, which must be patched out.
    def test_buy_phone_number_with_area_code_success(self, bw_client):
        with patch("requests.post") as mock_post, patch("time.sleep") as mock_sleep, patch.object(bw_client, "fetch_placed_purchased_order_details") as mock_fetch:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus><Order><id>order_123</id></Order></OrderResponse>"
            mock_post.return_value = mock_response
            mock_fetch.return_value = "+14155551234"

            result = bw_client.buy_phone_number(area_code="415", user_id="user_1")

            assert result == ("+14155551234", "order_123")
            mock_sleep.assert_called_once_with(10)

    def test_buy_phone_number_with_specific_number_success(self, bw_client):
        with patch("requests.post") as mock_post, patch("time.sleep"), patch.object(bw_client, "fetch_placed_purchased_order_details") as mock_fetch:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus><Order><id>order_456</id></Order></OrderResponse>"
            mock_post.return_value = mock_response
            mock_fetch.return_value = "+14155551234"

            result = bw_client.buy_phone_number(phone_number="+14155551234", user_id="user_1")

            assert result == ("+14155551234", "order_456")

    def test_buy_phone_number_invalid_country_code_raises_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.buy_phone_number(area_code="415", country_code="MX")

    def test_buy_phone_number_failed_order_returns_false(self, bw_client):
        # Neither phone_number nor area_code is required by the real
        # implementation - a failed order response simply yields False.
        with patch("requests.post") as mock_post, patch("time.sleep") as mock_sleep:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_post.return_value = mock_response

            result = bw_client.buy_phone_number()

            assert result is False
            mock_sleep.assert_called_once_with(10)


class TestSHBandwidthClientFetchPlacedPurchasedOrderDetails:
    # Real fetch_placed_purchased_order_details() returns the completed
    # phone number (or an error message string, or None) - never the raw
    # response dict.
    @patch("requests.get")
    def test_fetch_order_details_complete_returns_number(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "<OrderResponse><OrderStatus>COMPLETE</OrderStatus>"
            "<CompletedNumbers><TelephoneNumber><FullNumber>+19197567242</FullNumber></TelephoneNumber></CompletedNumbers>"
            "</OrderResponse>"
        )
        mock_get.return_value = mock_response

        result = bw_client.fetch_placed_purchased_order_details(order_id="order_123")

        assert result == "+19197567242"

    @patch("requests.get")
    def test_fetch_order_details_received_returns_message(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "<OrderResponse><OrderStatus>RECEIVED</OrderStatus>"
            "<ErrorList><Error><Description>Order is pending.</Description></Error></ErrorList>"
            "</OrderResponse>"
        )
        mock_get.return_value = mock_response

        result = bw_client.fetch_placed_purchased_order_details(order_id="order_123")

        assert result == "Order is pending."

    def test_fetch_order_details_no_order_id_raises_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.fetch_placed_purchased_order_details(order_id=None)


class TestSHBandwidthClientGetMedia:
    @patch("requests.get")
    def test_get_media_success_raw_data(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"image_data"
        mock_get.return_value = mock_response

        result = bw_client.get_media("http://example.com/media.jpg", raw_data=True)

        assert result == b"image_data"

    @patch("requests.get")
    @patch("builtins.open", create=True)
    def test_get_media_save_to_file(self, mock_open, mock_get, bw_client, tmp_path):
        # os.path.dirname("test.jpg") == "" and os.path.isdir("") is False,
        # so a bare filename with no directory component makes the real code
        # raise ValueError before ever reaching the request - an absolute
        # path in a real (temp) directory is required here.
        out_file = str(tmp_path / "test.jpg")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"image_data"
        mock_get.return_value = mock_response
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        result = bw_client.get_media("http://example.com/media.jpg", out_filename=out_file)

        # Real code returns out_filename on success, not True.
        assert result == out_file
        mock_file.write.assert_called_once_with(b"image_data")

    @patch("requests.get")
    def test_get_media_failure(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = bw_client.get_media("http://example.com/media.jpg", raw_data=True)

        assert result is None


class TestCustomExceptions:
    def test_bandwidth_order_pending_exception(self):
        with pytest.raises(BandwidthOrderPendingException) as exc_info:
            raise BandwidthOrderPendingException("Order pending")

        assert str(exc_info.value) == "Order pending"

    def test_bw_number_unavailable_error(self):
        with pytest.raises(BWNumberUnavailableError) as exc_info:
            raise BWNumberUnavailableError("Number unavailable")

        assert str(exc_info.value) == "Number unavailable"

    def test_bw_toll_free_unavailable_error(self):
        with pytest.raises(BWTollFreeUnavailableError) as exc_info:
            raise BWTollFreeUnavailableError("Toll free unavailable")

        assert str(exc_info.value) == "Toll free unavailable"

    def test_bw_tag_limit_exceeded_exception(self):
        with pytest.raises(BWTagCharacterLimitExceededException) as exc_info:
            raise BWTagCharacterLimitExceededException("Tag limit exceeded")

        assert str(exc_info.value) == "Tag limit exceeded"

    def test_bw_message_limit_exceeded_exception(self):
        with pytest.raises(BWMessageCharacterLimitExceededException) as exc_info:
            raise BWMessageCharacterLimitExceededException("Message limit exceeded")

        assert str(exc_info.value) == "Message limit exceeded"

    def test_bw_media_url_limit_exceeded_exception(self):
        with pytest.raises(BWLengthOfMediaURLLimitExceededException) as exc_info:
            raise BWLengthOfMediaURLLimitExceededException("Media URL limit exceeded")

        assert str(exc_info.value) == "Media URL limit exceeded"

    def test_all_exceptions_are_exception_subclasses(self):
        assert issubclass(BandwidthOrderPendingException, Exception)
        assert issubclass(BWNumberUnavailableError, Exception)
        assert issubclass(BWTollFreeUnavailableError, Exception)
        assert issubclass(BWTagCharacterLimitExceededException, Exception)
        assert issubclass(BWMessageCharacterLimitExceededException, Exception)
        assert issubclass(BWLengthOfMediaURLLimitExceededException, Exception)


# NOTE on unreachable/dead code (documented, not covered - forcing coverage
# would require a source change, which is out of scope for a tests-only task):
#
# - phonenumber_as_e164() line "if isinstance(number, bytes): number =
#   number.decode('utf-8')" is unreachable: the preceding
#   "if not isinstance(number, str): number = str(number)" always converts
#   any non-str (including bytes) to str first, so `number` can never still
#   be bytes by the time the bytes-check runs.


class TestSHBandwidthClientInitOAuth2AndErrors:
    @patch("bandwidth.Configuration")
    def test_init_oauth2_success(self, mock_config, mock_settings):
        mock_settings.BW_CLIENT_ID = "test_client_id"
        mock_settings.BW_CLIENT_SECRET = "test_client_secret"

        client = SHBandwidthClient(use_oauth2=True)

        assert client.use_oauth2 is True
        assert client.client_id == "test_client_id"
        assert client.client_secret == "test_client_secret"
        mock_config.assert_called_once_with(client_id="test_client_id", client_secret="test_client_secret")

    @patch("bandwidth.Configuration")
    def test_init_oauth2_missing_client_credentials_raises(self, mock_config, mock_settings):
        # mock_settings already leaves BW_CLIENT_ID/BW_CLIENT_SECRET as None.
        with pytest.raises(ValueError):
            SHBandwidthClient(use_oauth2=True)

    @patch("bandwidth.Configuration")
    def test_init_missing_basic_auth_credentials_raises(self, mock_config, mock_settings):
        mock_settings.BW_USERNAME = None
        mock_settings.BW_PASSWORD = None

        with pytest.raises(ValueError):
            SHBandwidthClient(username=None, password=None, use_oauth2=False)


class TestSHBandwidthClientOAuthHeaders:
    @patch("bandwidth.Configuration")
    def test_get_oauth_bearer_header(self, mock_config, mock_settings):
        mock_settings.BW_CLIENT_ID = "cid"
        mock_settings.BW_CLIENT_SECRET = "csecret"
        client = SHBandwidthClient(use_oauth2=True)
        client.configuration.get_access_token.return_value = "tok123"

        result = client._get_oauth_bearer_header()

        assert result == {"Authorization": "Bearer tok123"}

    @patch("bandwidth.Configuration")
    def test_get_auth_header_uses_oauth_when_enabled(self, mock_config, mock_settings):
        mock_settings.BW_CLIENT_ID = "cid"
        mock_settings.BW_CLIENT_SECRET = "csecret"
        client = SHBandwidthClient(use_oauth2=True)
        client.configuration.get_access_token.return_value = "tok123"

        result = client._get_auth_header()

        assert result == {"Authorization": "Bearer tok123"}

    def test_get_auth_header_uses_basic_by_default(self, bw_client):
        result = bw_client._get_auth_header()

        assert result["Authorization"].startswith("Basic ")


class TestSHBandwidthClientCleanupAndReturnNumbersQuantityError:
    def test_quantity_below_one_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client._cleanup_and_return_numbers([], 0, "US")


class TestSHBandwidthClientSendSMSAuAndExceptions:
    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_sms_au_from_number_uses_au_account(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "msg_au"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms("+61491570156", "+14155555678", "Hello")

        assert result == "msg_au"

    def test_send_sms_inner_exception_returns_none(self, bw_client):
        with patch("bandwidth.ApiClient"), patch("bandwidth.MessagesApi") as mock_messages_api_cls, patch("bandwidth.MessageRequest"):
            mock_api = MagicMock()
            mock_messages_api_cls.return_value = mock_api
            mock_api.create_message.side_effect = Exception("create_message boom")

            result = bw_client.send_sms("+14155551234", "+14155555678", "Hello")

            assert result is None

    def test_send_sms_outer_exception_reraises(self, bw_client):
        with patch("bandwidth.ApiClient", side_effect=Exception("api client boom")):
            with pytest.raises(Exception):
                bw_client.send_sms("+14155551234", "+14155555678", "Hello")


class TestSHBandwidthClientSendMMSAuAndExceptions:
    @patch("bandwidth.MessageRequest")
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_send_mms_au_from_number_uses_au_account(self, mock_api_client_cls, mock_messages_api_cls, mock_message_request_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = "mms_au"
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_mms("+61491570156", "+14155555678", "Hello", ["http://example.com/image.jpg"])

        assert result == "mms_au"

    def test_send_mms_inner_exception_returns_none(self, bw_client):
        with patch("bandwidth.ApiClient"), patch("bandwidth.MessagesApi") as mock_messages_api_cls, patch("bandwidth.MessageRequest"):
            mock_api = MagicMock()
            mock_messages_api_cls.return_value = mock_api
            mock_api.create_message.side_effect = Exception("create_message boom")

            result = bw_client.send_mms("+14155551234", "+14155555678", "Hello", ["http://example.com/image.jpg"])

            assert result is None

    def test_send_mms_outer_exception_reraises(self, bw_client):
        with patch("bandwidth.ApiClient", side_effect=Exception("api client boom")):
            with pytest.raises(Exception):
                bw_client.send_mms("+14155551234", "+14155555678", "Hello", ["http://example.com/image.jpg"])


class TestSHBandwidthClientGetMessageInfoAuAndException:
    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_get_message_info_au_country_code(self, mock_api_client_cls, mock_messages_api_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_response = MagicMock()
        mock_api.list_messages.return_value = mock_response

        result = bw_client.get_message_info("msg_123", country_code="AU")

        assert result is mock_response
        mock_api.list_messages.assert_called_once_with(account_id=bw_client.user_id_au, message_id="msg_123")

    @patch("bandwidth.MessagesApi")
    @patch("bandwidth.ApiClient")
    def test_get_message_info_exception_reraises(self, mock_api_client_cls, mock_messages_api_cls, bw_client):
        mock_api = MagicMock()
        mock_messages_api_cls.return_value = mock_api
        mock_api.list_messages.side_effect = Exception("list_messages boom")

        with pytest.raises(Exception):
            bw_client.get_message_info("msg_123")


class TestSHBandwidthClientInServiceEdgeCases:
    def test_in_service_invalid_number_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.in_service("invalid")

    def test_in_service_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.in_service("+14155551234", country_code="MX")

    @patch("requests.get")
    def test_in_service_au_country_code(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = bw_client.in_service("+61491570156", country_code="AU")

        assert result is True

    @patch("requests.get")
    def test_in_service_unexpected_status_code(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = bw_client.in_service("+14155551234")

        assert result is False

    @patch("requests.get")
    def test_in_service_request_exception_raises_name_error(self, mock_get, bw_client):
        """
        PRE-EXISTING BUG (not fixed): in_service() never initializes `response`
        before its try block (unlike its sibling methods, which all do
        `response = None` first). When requests.get() itself raises, the
        except handler references `response.__dict__` before `response` was
        ever assigned, which raises NameError from inside the except block -
        masking the original exception entirely instead of logging it and
        returning False as the surrounding code implies it should.
        """
        mock_get.side_effect = Exception("network boom")

        with pytest.raises(NameError):
            bw_client.in_service("+14155551234")


class TestSHBandwidthClientFindNumberInAreaCodeEdgeCases:
    def test_quantity_below_one_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.find_number_in_area_code("415", quantity=0)

    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.find_number_in_area_code("415", quantity=1, country_code="MX")

    @patch("requests.get")
    def test_au_country_code_raises_value_error(self, mock_get, bw_client):
        """
        PRE-EXISTING BUG (not fixed): find_number_in_area_code() never
        forwards its `country_code` argument to
        `_cleanup_and_return_numbers(cleaned_numbers, quantity)` (called
        without a country_code, so it defaults to "US"). An AU number then
        fails E.164 formatting against the "US" country code and a bare
        ValueError propagates instead of the formatted number being
        returned.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<SearchResult><ResultCount>1</ResultCount>" "<TelephoneNumberList><TelephoneNumber>491570156</TelephoneNumber></TelephoneNumberList>" "</SearchResult>"
        mock_get.return_value = mock_response

        with pytest.raises(ValueError):
            bw_client.find_number_in_area_code("491", 1, country_code="AU")

    def test_unexpected_status_code_raises_index_error(self, bw_client):
        """
        PRE-EXISTING BUG (not fixed): on a non-200 response,
        find_number_in_area_code() just logs and leaves `cleaned_numbers`
        empty; the trailing `return self._cleanup_and_return_numbers(...)` is
        outside the try/except, so `numbers[0]` on the empty list raises an
        uncaught IndexError instead of raising AreaCodeUnavailableError like
        the genuine-failure branch does.
        """
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            with pytest.raises(IndexError):
                bw_client.find_number_in_area_code("415", 1)


class TestSHBandwidthClientSearchAvailableTollFreeNumberEdgeCases:
    def test_quantity_below_one_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.search_available_toll_free_number(quantity=0)

    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.search_available_toll_free_number(quantity=1, country_code="MX")

    @patch("requests.get")
    def test_au_country_code_raises_value_error(self, mock_get, bw_client):
        """
        PRE-EXISTING BUG (not fixed): same shape as
        find_number_in_area_code() - search_available_toll_free_number()
        never forwards its `country_code` argument to
        `_cleanup_and_return_numbers(cleaned_numbers, quantity)`, so an AU
        number is formatted against the default "US" country code and a bare
        ValueError propagates instead of the formatted number being
        returned.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<SearchResult><ResultCount>1</ResultCount>" "<TelephoneNumberList><TelephoneNumber>491570156</TelephoneNumber></TelephoneNumberList>" "</SearchResult>"
        mock_get.return_value = mock_response

        with pytest.raises(ValueError):
            bw_client.search_available_toll_free_number(quantity=1, country_code="AU")

    def test_unexpected_status_code_raises_index_error(self, bw_client):
        """
        PRE-EXISTING BUG (not fixed): same shape as
        find_number_in_area_code() - on a non-200 response the trailing
        `_cleanup_and_return_numbers()` call (outside the try/except) raises
        an uncaught IndexError on the empty `cleaned_numbers` list.
        """
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            with pytest.raises(IndexError):
                bw_client.search_available_toll_free_number(quantity=1)

    def test_request_exception_raises_attribute_error(self, bw_client):
        """
        PRE-EXISTING BUG (not fixed): the except handler logs
        `response.__dict__` while `response` is still None (it's only
        assigned inside the try block, before the request call raises), so
        the intended `AreaCodeUnavailableError` is masked by an
        AttributeError instead.
        """
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("network boom")

            with pytest.raises(AttributeError):
                bw_client.search_available_toll_free_number(quantity=1)


class TestSHBandwidthClientGetNumberInfoEdgeCases:
    def test_invalid_phone_number_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.get_number_info("invalid")

    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.get_number_info("+14155551234", country_code="MX")

    @patch("requests.get")
    def test_au_country_code_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<TelephoneNumberResponse><TelephoneNumberDetails>" "<FullNumber>491570156</FullNumber><Status>Active</Status>" "</TelephoneNumberDetails></TelephoneNumberResponse>"
        mock_get.return_value = mock_response

        result = bw_client.get_number_info("+61491570156", country_code="AU")

        assert result is not None

    @patch("requests.get")
    def test_unexpected_status_code_returns_none(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = bw_client.get_number_info("+14155551234")

        assert result is None

    @patch("requests.get")
    def test_request_exception_reraises(self, mock_get, bw_client):
        mock_get.side_effect = Exception("network boom")

        with pytest.raises(Exception):
            bw_client.get_number_info("+14155551234")


class TestSHBandwidthClientReleasePhoneNumberEdgeCases:
    def test_invalid_number_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.release_phone_number("invalid")

    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.release_phone_number("+14155551234", country_code="MX")

    @patch("requests.post")
    def test_au_country_code_success(self, mock_post, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<DisconnectTelephoneNumberOrderResponse></DisconnectTelephoneNumberOrderResponse>"
        mock_post.return_value = mock_response

        result = bw_client.release_phone_number("+61491570156", country_code="AU")

        assert result is None
        mock_post.assert_called_once()

    @patch("requests.post")
    def test_request_exception_reraises(self, mock_post, bw_client):
        mock_post.side_effect = Exception("network boom")

        with pytest.raises(Exception):
            bw_client.release_phone_number("+14155551234")


class TestSHBandwidthClientGetActiveNumberCountEdgeCases:
    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.get_active_number_count(country_code="MX")

    @patch("requests.get")
    def test_au_country_code_default_site_id(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<Quantity><Count>7</Count></Quantity>"
        mock_get.return_value = mock_response

        result = bw_client.get_active_number_count(country_code="AU")

        assert result == "7"

    @patch("requests.get")
    def test_unexpected_status_code_returns_zero(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = bw_client.get_active_number_count()

        assert result == 0

    @patch("requests.get")
    def test_request_exception_reraises(self, mock_get, bw_client):
        mock_get.side_effect = Exception("network boom")

        with pytest.raises(Exception):
            bw_client.get_active_number_count()


class TestSHBandwidthClientListActiveNumbersEdgeCases:
    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.list_active_numbers(country_code="MX")

    @patch("requests.get")
    def test_au_country_code_default_site_id_single_page(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "<TNs><TotalCount>2</TotalCount><Links><next></next></Links>"
            "<TelephoneNumbers>"
            "<TelephoneNumber>+61491570156</TelephoneNumber>"
            "<TelephoneNumber>+61491570157</TelephoneNumber>"
            "</TelephoneNumbers></TNs>"
        )
        mock_get.return_value = mock_response

        result = bw_client.list_active_numbers(country_code="AU")

        assert len(result) == 2

    @patch("requests.get")
    def test_pagination_follows_next_link_across_two_pages(self, mock_get, bw_client):
        page_one = MagicMock()
        page_one.status_code = 200
        page_one.text = (
            "<TNs><TotalCount>600</TotalCount>"
            "<Links><next>&lt;https://api.test.com/numbers?page=2&amp;x=1&gt;</next></Links>"
            "<TelephoneNumbers>"
            "<TelephoneNumber>+14155551111</TelephoneNumber>"
            "<TelephoneNumber>+14155552222</TelephoneNumber>"
            "</TelephoneNumbers></TNs>"
        )
        page_two = MagicMock()
        page_two.status_code = 200
        page_two.text = (
            "<TNs><TotalCount>600</TotalCount>"
            "<Links><next>&lt;https://api.test.com/numbers?page=3&amp;x=1&gt;</next></Links>"
            "<TelephoneNumbers>"
            "<TelephoneNumber>+14155553333</TelephoneNumber>"
            "<TelephoneNumber>+14155554444</TelephoneNumber>"
            "</TelephoneNumbers></TNs>"
        )
        mock_get.side_effect = [page_one, page_two]

        result = bw_client.list_active_numbers()

        assert len(result) == 4
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_pagination_reaches_end_without_next_link(self, mock_get, bw_client):
        page = MagicMock()
        page.status_code = 200
        page.text = (
            "<TNs><TotalCount>600</TotalCount><Links><next></next></Links>"
            "<TelephoneNumbers>"
            "<TelephoneNumber>+14155551111</TelephoneNumber>"
            "<TelephoneNumber>+14155552222</TelephoneNumber>"
            "</TelephoneNumbers></TNs>"
        )
        mock_get.side_effect = [page, page]

        result = bw_client.list_active_numbers()

        assert len(result) == 4
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_unexpected_status_code_breaks_and_returns_false(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = bw_client.list_active_numbers()

        assert result is False

    @patch("requests.get")
    def test_request_exception_reraises(self, mock_get, bw_client):
        mock_get.side_effect = Exception("network boom")

        with pytest.raises(Exception):
            bw_client.list_active_numbers()


class TestSHBandwidthClientGetSiteInfoForNumberEdgeCases:
    def test_invalid_phone_number_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.get_siteinfo_for_number("invalid")

    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.get_siteinfo_for_number("+14155551234", country_code="MX")

    @patch("requests.get")
    def test_au_country_code_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<Site><Id>site_au</Id><Name>AU Site</Name></Site>"
        mock_get.return_value = mock_response

        result = bw_client.get_siteinfo_for_number("+61491570156", country_code="AU")

        assert result["Id"] == "site_au"

    @patch("requests.get")
    def test_unexpected_status_code_raises_type_error(self, mock_get, bw_client):
        """
        PRE-EXISTING BUG (not fixed): unlike get_number_info() (which simply
        returns the None response_data on a non-200 response),
        get_siteinfo_for_number() unconditionally does
        `return response_data["Site"]` outside the try/except. On a non-200
        response response_data stays None, so this raises an uncaught
        TypeError ('NoneType' object is not subscriptable) instead of
        returning None.
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with pytest.raises(TypeError):
            bw_client.get_siteinfo_for_number("+14155551234")

    @patch("requests.get")
    def test_request_exception_reraises(self, mock_get, bw_client):
        mock_get.side_effect = Exception("network boom")

        with pytest.raises(Exception):
            bw_client.get_siteinfo_for_number("+14155551234")


class TestSHBandwidthClientBuyTollFreeNumberEdgeCases:
    def test_au_country_code_default_site_id_and_endpoint(self, bw_client):
        with patch.object(bw_client, "search_available_toll_free_number") as mock_search, patch("requests.post") as mock_post:
            mock_search.return_value = "+61491570156"
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus></OrderResponse>"
            mock_post.return_value = mock_response

            result = bw_client.buy_toll_free_number(quantity=1, user_id=MagicMock(id="user_1"), country_code="AU", site_id=None)

            assert result == "+61491570156"

    def test_invalid_country_code_raises_value_error(self, bw_client):
        with patch.object(bw_client, "search_available_toll_free_number") as mock_search:
            mock_search.return_value = "+18005551234"

            with pytest.raises(ValueError):
                bw_client.buy_toll_free_number(quantity=1, user_id=MagicMock(id="user_1"), country_code="MX")

    def test_unexpected_status_code_still_returns_number(self, bw_client):
        with patch.object(bw_client, "search_available_toll_free_number") as mock_search, patch("requests.post") as mock_post:
            mock_search.return_value = "+18005551234"
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_post.return_value = mock_response

            result = bw_client.buy_toll_free_number(quantity=1, user_id=MagicMock(id="user_1"))

            assert result == "+18005551234"

    def test_request_exception_raises_attribute_error(self, bw_client):
        """
        PRE-EXISTING BUG (not fixed): when requests.post() raises,
        buy_toll_free_number()'s except handler logs
        `response.__dict__`, but `response` is still None (it was
        initialized to None and never reassigned because the exception
        happened during the assignment itself). That raises a masking
        AttributeError from inside the except block instead of just logging
        the original exception and continuing.
        """
        with patch.object(bw_client, "search_available_toll_free_number") as mock_search, patch("requests.post") as mock_post:
            mock_search.return_value = "+18005551234"
            mock_post.side_effect = Exception("network boom")

            with pytest.raises(AttributeError):
                bw_client.buy_toll_free_number(quantity=1, user_id=MagicMock(id="user_1"))


class TestSHBandwidthClientBuyPhoneNumberEdgeCases:
    def test_au_country_code_success(self, bw_client):
        with patch("requests.post") as mock_post, patch("time.sleep"), patch.object(bw_client, "fetch_placed_purchased_order_details") as mock_fetch:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus><Order><id>order_au</id></Order></OrderResponse>"
            mock_post.return_value = mock_response
            mock_fetch.return_value = "+61491570156"

            result = bw_client.buy_phone_number(area_code="491", user_id="user_1", country_code="AU", site_id=None)

            assert result == ("+61491570156", "order_au")

    def test_ca_country_code_raises_attribute_error(self, bw_client):
        """
        PRE-EXISTING BUG (not fixed): buy_phone_number() only assigns the
        local `endpoint` variable inside the "US" and "AU" branches of the
        country_code if/elif chain - the "CA" branch only sets
        country_code_a3 and leaves `endpoint` unbound.
        requests.post(endpoint, ...) then raises UnboundLocalError (a
        NameError subclass) before a request is ever made, but the except
        handler immediately logs `response.__dict__` while `response` is
        still None, so that masking AttributeError is what actually
        propagates instead of the original UnboundLocalError.
        """
        with patch("time.sleep"):
            with pytest.raises(AttributeError):
                bw_client.buy_phone_number(area_code="416", user_id="user_1", country_code="CA")

    def test_request_exception_raises_attribute_error(self, bw_client):
        """
        PRE-EXISTING BUG (not fixed): same shape as
        buy_toll_free_number()'s request-exception bug - the except handler
        logs `response.__dict__` while `response` is still None, raising a
        masking AttributeError instead of logging the original exception.
        """
        with patch("requests.post") as mock_post, patch("time.sleep"):
            mock_post.side_effect = Exception("network boom")

            with pytest.raises(AttributeError):
                bw_client.buy_phone_number(area_code="415", user_id="user_1")

    def test_invalid_cleaned_number_raises_bw_number_unavailable_error(self, bw_client):
        with patch("requests.post") as mock_post, patch("time.sleep"), patch.object(bw_client, "fetch_placed_purchased_order_details") as mock_fetch, patch.object(bw_util, "validate_phone_number") as mock_validate:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus><Order><id>order_bad</id></Order></OrderResponse>"
            mock_post.return_value = mock_response
            mock_fetch.return_value = "+14155551234"
            mock_validate.return_value = False

            with pytest.raises(BWNumberUnavailableError):
                bw_client.buy_phone_number(area_code="415", user_id="user_1")

    def test_exception_while_processing_order_details_reraises(self, bw_client):
        with patch("requests.post") as mock_post, patch("time.sleep"), patch.object(bw_client, "fetch_placed_purchased_order_details") as mock_fetch:
            mock_response = MagicMock()
            mock_response.status_code = 201
            mock_response.text = "<OrderResponse><OrderStatus>RECEIVED</OrderStatus><Order><id>order_err</id></Order></OrderResponse>"
            mock_post.return_value = mock_response
            mock_fetch.side_effect = Exception("fetch boom")

            with pytest.raises(Exception):
                bw_client.buy_phone_number(area_code="415", user_id="user_1")


class TestSHBandwidthClientFetchPlacedPurchasedOrderDetailsEdgeCases:
    def test_invalid_country_code_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.fetch_placed_purchased_order_details(order_id="order_123", country_code="MX")

    @patch("requests.get")
    def test_au_country_code_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            "<OrderResponse><OrderStatus>COMPLETE</OrderStatus>"
            "<CompletedNumbers><TelephoneNumber><FullNumber>+61491570156</FullNumber></TelephoneNumber></CompletedNumbers>"
            "</OrderResponse>"
        )
        mock_get.return_value = mock_response

        result = bw_client.fetch_placed_purchased_order_details(order_id="order_au", country_code="AU")

        assert result == "+61491570156"

    @patch("requests.get")
    def test_unexpected_status_code_returns_none(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = bw_client.fetch_placed_purchased_order_details(order_id="order_123")

        assert result is None

    @patch("requests.get")
    def test_request_exception_reraises(self, mock_get, bw_client):
        mock_get.side_effect = Exception("network boom")

        with pytest.raises(Exception):
            bw_client.fetch_placed_purchased_order_details(order_id="order_123")

    @patch("xmltodict.parse")
    @patch("requests.get")
    def test_malformed_response_data_reraises(self, mock_get, mock_parse, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        # A non-dict parse result makes response_data.get(...) raise
        # AttributeError inside the second try block.
        mock_parse.return_value = ["not", "a", "dict"]

        with pytest.raises(AttributeError):
            bw_client.fetch_placed_purchased_order_details(order_id="order_123")


class TestSHBandwidthClientGetMediaEdgeCases:
    @patch("requests.get")
    def test_get_media_default_filename_uses_settings_directory(self, mock_get, bw_client, mock_settings, tmp_path):
        mock_settings.BW_MMS_DIRECTORY = str(tmp_path)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"image_data"
        mock_get.return_value = mock_response

        with patch("builtins.open", create=True) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            result = bw_client.get_media("http://example.com/media.jpg")

            assert result == str(tmp_path / "media.jpg")

    def test_get_media_invalid_output_directory_raises_value_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.get_media("http://example.com/media.jpg", out_filename="/definitely/does/not/exist/media.jpg")

    @patch("requests.get")
    def test_get_media_existing_file_is_overwritten(self, mock_get, bw_client, tmp_path):
        existing_file = tmp_path / "existing.jpg"
        existing_file.write_bytes(b"old_data")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"new_data"
        mock_get.return_value = mock_response

        result = bw_client.get_media("http://example.com/media.jpg", out_filename=str(existing_file))

        assert result == str(existing_file)
        assert existing_file.read_bytes() == b"new_data"

    @patch("bandwidth.Configuration")
    def test_get_media_oauth2_uses_bearer_header(self, mock_config, mock_settings):
        mock_settings.BW_CLIENT_ID = "cid"
        mock_settings.BW_CLIENT_SECRET = "csecret"
        client = SHBandwidthClient(use_oauth2=True)
        client.configuration.get_access_token.return_value = "tok123"

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b"image_data"
            mock_get.return_value = mock_response

            result = client.get_media("http://example.com/media.jpg", raw_data=True)

            assert result == b"image_data"
            _, kwargs = mock_get.call_args
            assert kwargs["headers"] == {"Authorization": "Bearer tok123"}

    @patch("requests.get")
    def test_get_media_request_exception_returns_none(self, mock_get, bw_client):
        import requests as requests_module

        mock_get.side_effect = requests_module.exceptions.RequestException("network boom")

        result = bw_client.get_media("http://example.com/media.jpg", raw_data=True)

        assert result is None

    @patch("requests.get")
    def test_get_media_write_failure_returns_none(self, mock_get, bw_client, tmp_path):
        out_file = str(tmp_path / "test.jpg")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"image_data"
        mock_get.return_value = mock_response

        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__.side_effect = OSError("disk full")

            result = bw_client.get_media("http://example.com/media.jpg", out_filename=out_file)

            assert result is None
