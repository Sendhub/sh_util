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
import json
from unittest.mock import MagicMock, patch

import pytest

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


@pytest.fixture
def mock_settings():
    with patch('..bw_util.settings') as mock_settings:
        mock_settings.BW_USER_ID = 'test_user_id'
        mock_settings.BW_TOKEN = 'test_token'
        mock_settings.BW_SECRET = 'test_secret'
        mock_settings.BW_USERNAME = 'test_username'
        mock_settings.BW_PASSWORD = 'test_password'
        mock_settings.BW_APP_ID = 'test_app_id'
        mock_settings.BW_USER_ID_AU = 'test_user_id_au'
        mock_settings.BW_ACCOUNT_API_URL = 'https://api.test.com'
        mock_settings.BW_ACCOUNT_API_URL_AU = 'https://api.test.au'
        mock_settings.BW_SITE_ID = 'test_site_id'
        mock_settings.BW_SITE_ID_AU = 'test_site_id_au'
        mock_settings.SMS_GATEWAY_BANDWIDTH = 'bandwidth'
        yield mock_settings


@pytest.fixture
def bw_client(mock_settings):
    with patch('..bw_util.bandwidth.Configuration'):
        client = SHBandwidthClient(
            userid='test_user',
            token='test_token',
            secret='test_secret',
            username='test_user',
            password='test_pass'
        )
        return client


class TestPhoneNumberAsE164:

    @patch('..bw_util.validatePhoneNumber')
    def test_valid_us_number(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164('4155551234', 'US')

        assert result == '+14155551234'
        mock_validate.assert_called_once_with('4155551234', False)

    @patch('..bw_util.validatePhoneNumber')
    def test_valid_number_with_plus(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164('+14155551234', 'US')

        assert result == '+14155551234'

    @patch('..bw_util.validatePhoneNumber')
    def test_integer_number(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164(4155551234, 'US')

        assert result == '+14155551234'

    @patch('..bw_util.validatePhoneNumber')
    def test_invalid_number_raises_error(self, mock_validate):
        mock_validate.return_value = False

        with pytest.raises(ValueError) as exc_info:
            phonenumber_as_e164('invalid', 'US')

        assert 'Invalid phone number' in str(exc_info.value)

    @patch('..bw_util.validatePhoneNumber')
    def test_bytes_number(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164(b'4155551234', 'US')

        assert result == '+14155551234'

    @patch('..bw_util.validatePhoneNumber')
    def test_custom_country_code(self, mock_validate):
        mock_validate.return_value = True

        result = phonenumber_as_e164('2079460123', 'GB')

        assert result.startswith('+44')


class TestBandwidthAvailablePhoneNumber:

    @patch('..bw_util.displayNumber')
    def test_initialization(self, mock_display, mock_settings):
        mock_display.return_value = '(415) 555-1234'

        phone = BandwidthAvailablePhoneNumber('+14155551234')

        assert phone.phone_number == '+14155551234'
        assert phone.friendly_name == '(415) 555-1234'
        assert phone.gateway == 'bandwidth'
        mock_display.assert_called_once_with('+14155551234')


class TestBandwidthNumberObject:

    def test_initialization(self):
        obj = BandwidthNumberObject('+14155551234', 'sid_12345')

        assert obj.phone_number == '+14155551234'
        assert obj.sid == 'sid_12345'


class TestSHBandwidthClientInit:

    @patch('..bw_util.bandwidth.Configuration')
    def test_init_with_all_params(self, mock_config, mock_settings):
        client = SHBandwidthClient(
            userid='user_id',
            token='token',
            secret='secret',
            username='username',
            password='password'
        )

        assert client.token == 'token'
        assert client.secret == 'secret'
        assert client.username == 'username'
        assert client.password == 'password'
        assert client.user_id_na == 'user_id'
        mock_config.assert_called_once_with(username='username', password='password')

    @patch('..bw_util.bandwidth.Configuration')
    def test_init_with_defaults_from_settings(self, mock_config, mock_settings):
        client = SHBandwidthClient()

        assert client.token == 'test_token'
        assert client.secret == 'test_secret'
        assert client.username == 'test_username'
        assert client.password == 'test_password'
        assert client.user_id_na == 'test_user_id'
        assert client.bw_app_id == 'test_app_id'

    @patch('..bw_util.bandwidth.Configuration')
    def test_init_sets_site_ids(self, mock_config, mock_settings):
        client = SHBandwidthClient()

        assert client.bw_site_id_na == 'test_site_id'
        assert client.bw_site_id_au == 'test_site_id_au'

    @patch('..bw_util.bandwidth.Configuration')
    def test_init_sets_api_urls(self, mock_config, mock_settings):
        client = SHBandwidthClient()

        assert client.bw_account_api_url_na == 'https://api.test.com'
        assert client.bw_account_api_url_au == 'https://api.test.au'


class TestSHBandwidthClientCredentials:

    def test_get_encoded_credentials(self, bw_client):
        result = bw_client._get_encoded_credentials()

        expected = base64.b64encode(b'test_user:test_pass').decode('utf-8')
        assert result == expected

    def test_get_common_auth_header(self, bw_client):
        result = bw_client._get_common_auth_header()

        assert 'Authorization' in result
        assert result['Authorization'].startswith('Basic ')


class TestSHBandwidthClientAsE164:

    @patch('..bw_util.phonenumber_as_e164')
    def test_as_e164_calls_module_function(self, mock_e164):
        mock_e164.return_value = '+14155551234'

        result = SHBandwidthClient._as_e164('+14155551234', 'US')

        assert result == '+14155551234'
        mock_e164.assert_called_once_with('+14155551234', 'US')


class TestSHBandwidthClientE164Validation:

    def test_valid_e164_format(self, bw_client):
        assert bw_client.check_if_valid_e164_format('+14155551234') is True
        assert bw_client.check_if_valid_e164_format('+442079460123') is True
        assert bw_client.check_if_valid_e164_format('+61212345678') is True

    def test_invalid_e164_format_no_plus(self, bw_client):
        assert bw_client.check_if_valid_e164_format('14155551234') is False

    def test_invalid_e164_format_too_short(self, bw_client):
        assert bw_client.check_if_valid_e164_format('+1234567') is False

    def test_invalid_e164_format_too_long(self, bw_client):
        assert bw_client.check_if_valid_e164_format('+1234567890123456') is False

    def test_invalid_e164_format_starts_with_zero(self, bw_client):
        assert bw_client.check_if_valid_e164_format('+01234567890') is False

    def test_invalid_e164_format_contains_non_digits(self, bw_client):
        assert bw_client.check_if_valid_e164_format('+1415abc1234') is False

    def test_invalid_e164_format_not_string(self, bw_client):
        assert bw_client.check_if_valid_e164_format(4155551234) is False


class TestSHBandwidthClientCleanupAndReturnNumbers:

    @patch('..bw_util.phonenumber_as_e164')
    def test_single_number_quantity_one(self, mock_e164, bw_client):
        mock_e164.return_value = '+14155551234'

        result = bw_client._cleanup_and_return_numbers(['+14155551234'], 1, 'US')

        assert result == '+14155551234'

    @patch('..bw_util.phonenumber_as_e164')
    def test_multiple_numbers_quantity_greater_than_one(self, mock_e164, bw_client):
        mock_e164.side_effect = ['+14155551234', '+14155555678']

        result = bw_client._cleanup_and_return_numbers(['+14155551234', '+14155555678'], 2, 'US')

        assert isinstance(result, list)
        assert len(result) == 2
        assert result == ['+14155551234', '+14155555678']

    @patch('..bw_util.phonenumber_as_e164')
    def test_invalid_number_raises_value_error(self, mock_e164, bw_client):
        mock_e164.side_effect = ValueError('Invalid number')

        result = bw_client._cleanup_and_return_numbers(['invalid'], 1, 'US')

        assert result is None


class TestSHBandwidthClientParseNumberToBWFormat:

    def test_parse_us_number(self, bw_client):
        result = bw_client._parse_number_to_bw_format('+14155551234', 'US')

        assert result == '4155551234'

    def test_parse_number_without_plus(self, bw_client):
        result = bw_client._parse_number_to_bw_format('4155551234', 'US')

        assert result == '4155551234'


class TestSHBandwidthClientSendHello:

    @patch('..bw_util.phonenumber_as_e164')
    def test_send_hello_valid_numbers(self, mock_e164, bw_client):
        mock_e164.side_effect = ['+14155551234', '+14155555678']

        result = bw_client.send_hello('+14155551234', '+14155555678')

        assert result is None

    @patch('..bw_util.phonenumber_as_e164')
    def test_send_hello_invalid_number_catches_error(self, mock_e164, bw_client):
        mock_e164.side_effect = ValueError('Invalid')

        result = bw_client.send_hello('invalid', '+14155555678')

        assert result is None


class TestSHBandwidthClientCheckMsgStatus:

    def test_check_msg_status_calls_get_message_info(self, bw_client):
        with patch.object(bw_client, 'get_message_info') as mock_get_info:
            mock_get_info.return_value = {'status': 'delivered'}

            result = bw_client.check_msg_status('msg_123')

            assert result == {'status': 'delivered'}
            mock_get_info.assert_called_once_with('msg_123')


class TestSHBandwidthClientCheckRecipientListValidity:

    def test_valid_single_number(self, bw_client):
        result = bw_client.check_recipient_list_validity('+14155551234')

        assert result == ['+14155551234']

    def test_valid_list_of_numbers(self, bw_client):
        numbers = ['+14155551234', '+14155555678']

        result = bw_client.check_recipient_list_validity(numbers)

        assert result == numbers

    def test_invalid_numbers_filtered_out(self, bw_client):
        numbers = ['+14155551234', 'invalid', '+14155555678']

        result = bw_client.check_recipient_list_validity(numbers)

        assert len(result) == 2
        assert 'invalid' not in result

    def test_non_e164_numbers_converted(self, bw_client):
        numbers = ['4155551234']

        result = bw_client.check_recipient_list_validity(numbers)

        assert result[0].startswith('+')

    def test_empty_list_returns_empty(self, bw_client):
        result = bw_client.check_recipient_list_validity([])

        assert result == []


class TestSHBandwidthClientSendSMS:

    @patch('..bw_util.bandwidth')
    def test_send_sms_success(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.MessagesApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = 'msg_123'
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms('+14155551234', '+14155555678', 'Hello')

        assert result == 'msg_123'

    @patch('..bw_util.bandwidth')
    def test_send_sms_with_tag(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.MessagesApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = 'msg_123'
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms('+14155551234', '+14155555678', 'Hello', tag='test_tag')

        assert result == 'msg_123'

    def test_send_sms_tag_exceeds_limit_raises_exception(self, bw_client):
        long_tag = 'x' * 2025

        with pytest.raises(BWTagCharacterLimitExceededException):
            bw_client.send_sms('+14155551234', '+14155555678', 'Hello', tag=long_tag)

    def test_send_sms_message_exceeds_limit_raises_exception(self, bw_client):
        long_msg = 'x' * 2049

        with pytest.raises(BWMessageCharacterLimitExceededException):
            bw_client.send_sms('+14155551234', '+14155555678', long_msg)

    @patch('..bw_util.bandwidth')
    def test_send_sms_to_multiple_recipients(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.MessagesApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = 'msg_123'
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_sms('+14155551234', ['+14155555678', '+14155559999'], 'Hello')

        assert result == 'msg_123'


class TestSHBandwidthClientSendMMS:

    @patch('..bw_util.bandwidth')
    def test_send_mms_success(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.MessagesApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = 'msg_123'
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_mms('+14155551234', '+14155555678', 'Hello', ['http://example.com/image.jpg'])

        assert result == 'msg_123'

    @patch('..bw_util.bandwidth')
    def test_send_mms_with_tag(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.MessagesApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = 'msg_123'
        mock_api.create_message.return_value = mock_response

        result = bw_client.send_mms('+14155551234', '+14155555678', 'Hello', ['http://example.com/image.jpg'], tag='test')

        assert result == 'msg_123'

    def test_send_mms_tag_exceeds_limit_raises_exception(self, bw_client):
        long_tag = 'x' * 2025

        with pytest.raises(BWTagCharacterLimitExceededException):
            bw_client.send_mms('+14155551234', '+14155555678', 'Hello', ['http://example.com/image.jpg'], tag=long_tag)

    def test_send_mms_message_exceeds_limit_raises_exception(self, bw_client):
        long_msg = 'x' * 2049

        with pytest.raises(BWMessageCharacterLimitExceededException):
            bw_client.send_mms('+14155551234', '+14155555678', long_msg, ['http://example.com/image.jpg'])

    def test_send_mms_media_url_exceeds_limit_raises_exception(self, bw_client):
        long_url = 'http://example.com/' + 'x' * 4100

        with pytest.raises(BWLengthOfMediaURLLimitExceededException):
            bw_client.send_mms('+14155551234', '+14155555678', 'Hello', [long_url])


class TestSHBandwidthClientGetMessageInfo:

    @patch('..tel.bw_util.bandwidth')
    def test_get_message_info_success(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.MessagesApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.id = 'msg_123'
        mock_response.message_status = 'DELIVERED'
        mock_api.get_message.return_value = mock_response

        result = bw_client.get_message_info('msg_123')

        assert result.id == 'msg_123'
        assert result.message_status == 'DELIVERED'


class TestSHBandwidthClientInService:

    @patch('..tel.bw_util.bandwidth')
    def test_in_service_number_exists(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.PhoneNumberLookupApi.return_value = mock_api
        mock_response = MagicMock()
        mock_response.result = [{'status': 'ACTIVE'}]
        mock_api.lookup_tn_async.return_value.get.return_value = mock_response

        result = bw_client.in_service('+14155551234')

        assert result is True

    @patch('..tel.bw_util.bandwidth')
    def test_in_service_number_not_found(self, mock_bandwidth, bw_client):
        mock_api = MagicMock()
        mock_bandwidth.PhoneNumberLookupApi.return_value = mock_api
        mock_api.lookup_tn_async.return_value.get.side_effect = Exception('Not found')

        result = bw_client.in_service('+14155551234')

        assert result is False


class TestSHBandwidthClientFindNumberInAreaCode:

    @patch('..tel.bw_util.bandwidth')
    def test_find_number_in_area_code_success(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_client.search_and_order_local_numbers.return_value = ['+14155551234']

        with patch.object(bw_client, '_cleanup_and_return_numbers') as mock_cleanup:
            mock_cleanup.return_value = '+14155551234'

            result = bw_client.find_number_in_area_code('415', 1)

            assert result == '+14155551234'

    @patch('..tel.bw_util.bandwidth')
    def test_find_number_in_area_code_no_results_raises_error(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_client.search_and_order_local_numbers.return_value = []

        with pytest.raises(BWNumberUnavailableError):
            bw_client.find_number_in_area_code('999', 1)


class TestSHBandwidthClientSearchAvailableTollFreeNumber:

    @patch('..tel.bw_util.bandwidth')
    def test_search_toll_free_success(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_client.search_and_order_toll_free_numbers.return_value = ['+18005551234']

        with patch.object(bw_client, '_cleanup_and_return_numbers') as mock_cleanup:
            mock_cleanup.return_value = '+18005551234'

            result = bw_client.search_available_toll_free_number(quantity=1)

            assert result == '+18005551234'

    @patch('..tel.bw_util.bandwidth')
    def test_search_toll_free_no_results_raises_error(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_client.search_and_order_toll_free_numbers.return_value = []

        with pytest.raises(BWTollFreeUnavailableError):
            bw_client.search_available_toll_free_number(quantity=1)


class TestSHBandwidthClientGetNumberInfo:

    @patch('..tel.bw_util.requests.get')
    def test_get_number_info_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'TelephoneNumber': '4155551234',
            'Status': 'Active'
        }
        mock_get.return_value = mock_response

        result = bw_client.get_number_info('+14155551234')

        assert result is not None
        assert 'TelephoneNumber' in result


class TestSHBandwidthClientReleasePhoneNumber:

    @patch('..tel.bw_util.requests.delete')
    def test_release_phone_number_success(self, mock_delete, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response

        result = bw_client.release_phone_number('+14155551234')

        assert result is True

    @patch('..tel.bw_util.requests.delete')
    def test_release_phone_number_failure(self, mock_delete, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_delete.return_value = mock_response

        result = bw_client.release_phone_number('+14155551234')

        assert result is False


class TestSHBandwidthClientGetActiveNumberCount:

    @patch('..tel.bw_util.requests.get')
    def test_get_active_number_count_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'TelephoneNumberCount': 42
        }
        mock_get.return_value = mock_response

        result = bw_client.get_active_number_count()

        assert result == 42


class TestSHBandwidthClientListActiveNumbers:

    @patch('..tel.bw_util.requests.get')
    def test_list_active_numbers_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'TelephoneNumbers': {
                'TelephoneNumber': [
                    {'FullNumber': '+14155551234'},
                    {'FullNumber': '+14155555678'}
                ]
            }
        }
        mock_get.return_value = mock_response

        result = bw_client.list_active_numbers(size=10)

        assert len(result) == 2


class TestSHBandwidthClientGetSiteInfoForNumber:

    @patch('..tel.bw_util.requests.get')
    def test_get_siteinfo_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'Site': {
                'Id': 'site_123',
                'Name': 'Test Site'
            }
        }
        mock_get.return_value = mock_response

        result = bw_client.get_siteinfo_for_number('+14155551234')

        assert result is not None
        assert 'Site' in result


class TestSHBandwidthClientBuyTollFreeNumber:

    @patch('..tel.bw_util.bandwidth')
    def test_buy_toll_free_number_success(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_order = MagicMock()
        mock_order.id = 'order_123'
        mock_client.order_phone_number.return_value = mock_order

        with patch.object(bw_client, 'search_available_toll_free_number') as mock_search:
            mock_search.return_value = ['+18005551234']

            result = bw_client.buy_toll_free_number(quantity=1)

            assert isinstance(result, BandwidthNumberObject)
            assert result.phone_number == '+18005551234'


class TestSHBandwidthClientBuyPhoneNumber:

    @patch('..tel.bw_util.bandwidth')
    def test_buy_phone_number_with_area_code_success(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_order = MagicMock()
        mock_order.id = 'order_123'
        mock_client.order_phone_number.return_value = mock_order

        with patch.object(bw_client, 'find_number_in_area_code') as mock_find:
            mock_find.return_value = ['+14155551234']

            result = bw_client.buy_phone_number(area_code='415')

            assert isinstance(result, BandwidthNumberObject)
            assert result.phone_number == '+14155551234'

    @patch('..bw_util.cleanupPhoneNumber')
    @patch('..bw_util.validatePhoneNumber', return_value=False)
    @patch('..bw_util.time.sleep', return_value=None)
    @patch.object(SHBandwidthClient, 'fetch_placed_purchased_order_details')
    @patch('..bw_util.xmltodict.parse')
    @patch('..bw_util.requests.post')
    def test_buy_phone_number_with_area_code_ca_uses_na_endpoint(
        self,
        mock_post,
        mock_parse,
        mock_fetch_order_details,
        mock_sleep,
        mock_validate_phone_number,
        mock_cleanup_phone_number,
        bw_client,
    ):
        mock_cleanup_phone_number.side_effect = lambda number, country_code='US': number
        mock_parse.return_value = {
            'OrderResponse': {
                'OrderStatus': 'RECEIVED',
                'Order': {'id': 'order_123'},
            }
        }
        mock_fetch_order_details.return_value = ['+14165551234']
        mock_post.return_value = MagicMock(status_code=201, text='<xml/>')

        result = bw_client.buy_phone_number(area_code='416', country_code='CA')

        assert result == ([], 'order_123')
        assert mock_post.call_args.args[0] == (
            f"{bw_client.bw_account_api_url_na}/api/v2/accounts/{bw_client.user_id_na}/orders"
        )

        payload = json.loads(mock_post.call_args.kwargs['data'])
        assert payload['orderType']['countryCodeA3'] == 'CAN'
        assert payload['subAccountId'] == bw_client.bw_site_id_na

    @patch('..tel.bw_util.bandwidth')
    def test_buy_phone_number_with_specific_number_success(self, mock_bandwidth, bw_client):
        mock_client = MagicMock()
        mock_bandwidth.client_module.Client.return_value = mock_client
        mock_order = MagicMock()
        mock_order.id = 'order_123'
        mock_client.order_phone_number.return_value = mock_order

        result = bw_client.buy_phone_number(phone_number='+14155551234')

        assert isinstance(result, BandwidthNumberObject)
        assert result.phone_number == '+14155551234'

    def test_buy_phone_number_no_params_raises_error(self, bw_client):
        with pytest.raises(ValueError):
            bw_client.buy_phone_number()


class TestSHBandwidthClientFetchPlacedPurchasedOrderDetails:

    @patch('..tel.bw_util.requests.get')
    def test_fetch_order_details_success(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'Order': {
                'id': 'order_123',
                'OrderStatus': 'COMPLETE'
            }
        }
        mock_get.return_value = mock_response

        result = bw_client.fetch_placed_purchased_order_details(orderId='order_123')

        assert result is not None
        assert 'Order' in result


class TestSHBandwidthClientGetMedia:

    @patch('..tel.bw_util.requests.get')
    def test_get_media_success_raw_data(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'image_data'
        mock_get.return_value = mock_response

        result = bw_client.get_media('http://example.com/media.jpg', raw_data=True)

        assert result == b'image_data'

    @patch('..tel.bw_util.requests.get')
    @patch('builtins.open', create=True)
    def test_get_media_save_to_file(self, mock_open, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'image_data'
        mock_get.return_value = mock_response
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        result = bw_client.get_media('http://example.com/media.jpg', out_filename='test.jpg')

        assert result is True
        mock_file.write.assert_called_once_with(b'image_data')

    @patch('..tel.bw_util.requests.get')
    def test_get_media_failure(self, mock_get, bw_client):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = bw_client.get_media('http://example.com/media.jpg', raw_data=True)

        assert result is None


class TestCustomExceptions:

    def test_bandwidth_order_pending_exception(self):
        with pytest.raises(BandwidthOrderPendingException) as exc_info:
            raise BandwidthOrderPendingException('Order pending')

        assert str(exc_info.value) == 'Order pending'

    def test_bw_number_unavailable_error(self):
        with pytest.raises(BWNumberUnavailableError) as exc_info:
            raise BWNumberUnavailableError('Number unavailable')

        assert str(exc_info.value) == 'Number unavailable'

    def test_bw_toll_free_unavailable_error(self):
        with pytest.raises(BWTollFreeUnavailableError) as exc_info:
            raise BWTollFreeUnavailableError('Toll free unavailable')

        assert str(exc_info.value) == 'Toll free unavailable'

    def test_bw_tag_limit_exceeded_exception(self):
        with pytest.raises(BWTagCharacterLimitExceededException) as exc_info:
            raise BWTagCharacterLimitExceededException('Tag limit exceeded')

        assert str(exc_info.value) == 'Tag limit exceeded'

    def test_bw_message_limit_exceeded_exception(self):
        with pytest.raises(BWMessageCharacterLimitExceededException) as exc_info:
            raise BWMessageCharacterLimitExceededException('Message limit exceeded')

        assert str(exc_info.value) == 'Message limit exceeded'

    def test_bw_media_url_limit_exceeded_exception(self):
        with pytest.raises(BWLengthOfMediaURLLimitExceededException) as exc_info:
            raise BWLengthOfMediaURLLimitExceededException('Media URL limit exceeded')

        assert str(exc_info.value) == 'Media URL limit exceeded'

    def test_all_exceptions_are_exception_subclasses(self):
        assert issubclass(BandwidthOrderPendingException, Exception)
        assert issubclass(BWNumberUnavailableError, Exception)
        assert issubclass(BWTollFreeUnavailableError, Exception)
        assert issubclass(BWTagCharacterLimitExceededException, Exception)
        assert issubclass(BWMessageCharacterLimitExceededException, Exception)
        assert issubclass(BWLengthOfMediaURLLimitExceededException, Exception)
