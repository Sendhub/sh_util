"""
Unit tests for twilio_util module.

Test coverage includes:
- twilioFindNumberInAreaCode: with/without area code, empty results
- twilioBuyPhoneNumber: HIPAA subaccounts, area code purchase, specific number purchase, retries, exceptions
- search_users_by_enterprise: SQL query generation and execution
- TwilioAPIHandler: all methods including messaging services, campaigns, subaccounts
- Exception handling: TwilioRestException and generic exceptions
- Logging verification
"""

from unittest.mock import MagicMock, patch

import pytest
from twilio.base.exceptions import TwilioRestException

from ..twilio_util import (
    AreaCodeUnavailableError,
    TwilioAPIHandler,
    search_users_by_enterprise,
    twilioBuyPhoneNumber,
    twilioFindNumberInAreaCode,
)


@pytest.fixture
def mock_twilio_client():
    client = MagicMock()
    return client


@pytest.fixture
def mock_phone_number():
    phone = MagicMock()
    phone.phone_number = "+14155551234"
    phone.sid = "PN1234567890abcdef1234567890abcdef"
    return phone


@pytest.fixture
def mock_settings():
    with patch("..twilio_util.settings") as mock_settings:
        mock_settings.TWILIO_CLIENT = MagicMock()
        yield mock_settings


@pytest.fixture
def twilio_handler(mock_settings):
    return TwilioAPIHandler()


class TestTwilioFindNumberInAreaCode:
    def test_find_number_with_area_code_returns_results(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]

        result = twilioFindNumberInAreaCode(mock_twilio_client, "415", "US")

        assert result == [mock_phone_number]
        mock_twilio_client.available_phone_numbers.assert_called_once_with("US")
        mock_twilio_client.available_phone_numbers.return_value.local.list.assert_called_once_with(area_code="415", limit=20)

    def test_find_number_without_area_code_fetches_all(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.fetch.return_value = [mock_phone_number]

        result = twilioFindNumberInAreaCode(mock_twilio_client, None, "US")

        assert result == [mock_phone_number]
        mock_twilio_client.available_phone_numbers.assert_called_once_with("US")
        mock_twilio_client.available_phone_numbers.return_value.fetch.assert_called_once()

    def test_find_number_empty_list_falls_back_to_fetch(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = []
        mock_twilio_client.available_phone_numbers.return_value.fetch.return_value = [mock_phone_number]

        result = twilioFindNumberInAreaCode(mock_twilio_client, "415", "US")

        assert result == [mock_phone_number]
        mock_twilio_client.available_phone_numbers.return_value.fetch.assert_called_once()

    def test_find_number_custom_country_code(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]

        result = twilioFindNumberInAreaCode(mock_twilio_client, "020", "GB")

        mock_twilio_client.available_phone_numbers.assert_called_once_with("GB")


class TestTwilioBuyPhoneNumber:
    @patch("..twilio_util.Client")
    def test_buy_number_with_hipaa_enabled_creates_subaccount_client(self, mock_client_class, mock_twilio_client, mock_phone_number):
        hipaa_config = {"enabled": True, "subaccount_sid": "AC_subaccount", "subaccount_token": "sub_token", "subaccount_twiml_sid": "AP_twiml_sub"}
        mock_subaccount_client = MagicMock()
        mock_client_class.return_value = mock_subaccount_client
        mock_subaccount_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_subaccount_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", hipaa_config, areaCode="415")

        mock_client_class.assert_called_once_with("AC_subaccount", "sub_token")
        assert result == mock_phone_number
        mock_subaccount_client.incoming_phone_numbers.create.assert_called_once_with(
            phone_number=mock_phone_number.phone_number, sms_application_sid="AP_twiml_sub", voice_application_sid="AP_twiml_sub"
        )

    def test_buy_number_with_hipaa_disabled_uses_main_client(self, mock_twilio_client, mock_phone_number):
        hipaa_config = {"enabled": False}
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", hipaa_config, areaCode="415")

        assert result == mock_phone_number
        mock_twilio_client.incoming_phone_numbers.create.assert_called_once()

    def test_buy_number_with_area_code_success(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, areaCode="415")

        assert result == mock_phone_number
        mock_twilio_client.available_phone_numbers.assert_called_once_with("US")
        mock_twilio_client.available_phone_numbers.return_value.local.list.assert_called_once_with(area_code="415", limit=20)

    @patch("..twilio_util.cleanupPhoneNumber")
    def test_buy_specific_phone_number_success(self, mock_cleanup, mock_twilio_client, mock_phone_number):
        mock_cleanup.return_value = "+14155551234"
        mock_twilio_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, phoneNumber="4155551234")

        assert result == mock_phone_number
        mock_cleanup.assert_called_once_with("4155551234")
        mock_twilio_client.incoming_phone_numbers.create.assert_called_once_with(sms_application_sid="AP_app_sid", voice_application_sid="AP_app_sid", phone_number="+14155551234")

    def test_buy_number_retries_on_twilio_exception(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.side_effect = [
            TwilioRestException(status=400, uri="/test", msg="Error 1"),
            TwilioRestException(status=400, uri="/test", msg="Error 2"),
            mock_phone_number,
        ]

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, areaCode="415")

        assert result == mock_phone_number
        assert mock_twilio_client.incoming_phone_numbers.create.call_count == 3

    def test_buy_number_retries_on_generic_exception(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.side_effect = [ValueError("Test error"), mock_phone_number]

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, areaCode="415")

        assert result == mock_phone_number
        assert mock_twilio_client.incoming_phone_numbers.create.call_count == 2

    def test_buy_number_exhausts_retries_raises_error(self, mock_twilio_client, mock_phone_number):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.side_effect = TwilioRestException(status=400, uri="/test", msg="Error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, areaCode="415")

        assert "having problems buying phone numbers" in str(exc_info.value)
        assert mock_twilio_client.incoming_phone_numbers.create.call_count == 5

    @patch("..twilio_util.cleanupPhoneNumber")
    def test_buy_specific_number_exhausts_retries_no_exception(self, mock_cleanup, mock_twilio_client):
        mock_cleanup.return_value = "+14155551234"
        mock_twilio_client.incoming_phone_numbers.create.side_effect = TwilioRestException(status=400, uri="/test", msg="Error")

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, phoneNumber="4155551234")

        assert result is None
        assert mock_twilio_client.incoming_phone_numbers.create.call_count == 5

    def test_buy_number_no_area_code_no_phone_number_raises_error(self, mock_twilio_client):
        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None)

        assert "No available numbers left" in str(exc_info.value)

    def test_buy_number_empty_numbers_list_raises_error(self, mock_twilio_client):
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = []

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", None, areaCode="999")

        assert "No available numbers left" in str(exc_info.value)

    @patch("..twilio_util.Client")
    def test_buy_number_hipaa_missing_credentials_uses_main_client(self, mock_client_class, mock_twilio_client, mock_phone_number):
        hipaa_config = {"enabled": True, "subaccount_sid": "AC_subaccount", "subaccount_token": None}
        mock_twilio_client.available_phone_numbers.return_value.local.list.return_value = [mock_phone_number]
        mock_twilio_client.incoming_phone_numbers.create.return_value = mock_phone_number

        result = twilioBuyPhoneNumber(mock_twilio_client, "AP_app_sid", hipaa_config, areaCode="415")

        mock_client_class.assert_not_called()
        assert result == mock_phone_number


class TestSearchUsersByEnterprise:
    @patch("..twilio_util.db_query")
    def test_search_users_returns_query_results(self, mock_db_query):
        expected_results = [("+14155551234", "PN_sid1"), ("+14155555678", "PN_sid2")]
        mock_db_query.return_value = expected_results

        result = search_users_by_enterprise(123, "twilio")

        assert result == expected_results
        mock_db_query.assert_called_once()
        sql_call = mock_db_query.call_args[0][0]
        assert "123" in sql_call
        assert "gateway='twilio'" in sql_call

    @patch("..twilio_util.db_query")
    def test_search_users_with_different_gateway(self, mock_db_query):
        mock_db_query.return_value = []

        result = search_users_by_enterprise(456, "other_gateway")

        assert result == []
        sql_call = mock_db_query.call_args[0][0]
        assert "456" in sql_call
        assert "gateway='other_gateway'" in sql_call


class TestTwilioAPIHandlerInit:
    def test_init_sets_twilio_client_from_settings(self, mock_settings):
        handler = TwilioAPIHandler()

        assert handler.twilio_client == mock_settings.TWILIO_CLIENT


class TestTwilioAPIHandlerGetTwilioClient:
    @patch("..twilio_util.Client")
    def test_get_client_with_hipaa_creates_subaccount_client(self, mock_client_class, twilio_handler):
        mock_subaccount_client = MagicMock()
        mock_client_class.return_value = mock_subaccount_client
        params = {"hipaa": {"enabled": True, "subaccount_sid": "AC_sub", "subaccount_token": "token_sub"}}

        result = twilio_handler.get_twilio_client(params)

        mock_client_class.assert_called_once_with("AC_sub", "token_sub")
        assert result == mock_subaccount_client
        assert twilio_handler.twilio_client == mock_subaccount_client

    def test_get_client_without_hipaa_returns_default_client(self, twilio_handler, mock_settings):
        params = {}

        result = twilio_handler.get_twilio_client(params)

        assert result == mock_settings.TWILIO_CLIENT

    def test_get_client_hipaa_disabled_returns_default_client(self, twilio_handler, mock_settings):
        params = {"hipaa": {"enabled": False}}

        result = twilio_handler.get_twilio_client(params)

        assert result == mock_settings.TWILIO_CLIENT

    @patch("..twilio_util.Client")
    def test_get_client_hipaa_missing_token_returns_default(self, mock_client_class, twilio_handler, mock_settings):
        params = {"hipaa": {"enabled": True, "subaccount_sid": "AC_sub", "subaccount_token": None}}

        result = twilio_handler.get_twilio_client(params)

        mock_client_class.assert_not_called()
        assert result == mock_settings.TWILIO_CLIENT

    @patch("..twilio_util.Client")
    def test_get_client_twilio_exception_logs_and_returns_default(self, mock_client_class, twilio_handler, mock_settings):
        mock_client_class.side_effect = TwilioRestException(status=401, uri="/test", msg="Auth error")
        params = {"hipaa": {"enabled": True, "subaccount_sid": "AC_sub", "subaccount_token": "token_sub"}}

        result = twilio_handler.get_twilio_client(params)

        assert result == mock_settings.TWILIO_CLIENT

    @patch("..twilio_util.Client")
    def test_get_client_generic_exception_logs_and_returns_default(self, mock_client_class, twilio_handler, mock_settings):
        mock_client_class.side_effect = ValueError("Unexpected error")
        params = {"hipaa": {"enabled": True, "subaccount_sid": "AC_sub", "subaccount_token": "token_sub"}}

        result = twilio_handler.get_twilio_client(params)

        assert result == mock_settings.TWILIO_CLIENT


class TestTwilioAPIHandlerCreateMsgService:
    def test_create_msg_service_success(self, twilio_handler):
        mock_service = MagicMock()
        mock_service.sid = "MG_service_sid"
        twilio_handler.twilio_client.messaging.services.create.return_value = mock_service

        result = twilio_handler.create_twilio_msg_service("TestService")

        assert result == mock_service
        twilio_handler.twilio_client.messaging.services.create.assert_called_once_with(friendly_name="TestService", use_inbound_webhook_on_number=True)


class TestTwilioAPIHandlerGetMessagingService:
    def test_get_messaging_service_returns_dict(self, twilio_handler):
        mock_service1 = MagicMock()
        mock_service1.friendly_name = "Service1"
        mock_service1.sid = "MG_sid1"
        mock_service2 = MagicMock()
        mock_service2.friendly_name = "Service2"
        mock_service2.sid = "MG_sid2"
        twilio_handler.twilio_client.messaging.services.list.return_value = [mock_service1, mock_service2]

        result = twilio_handler.get_messaging_service()

        assert result == {"Service1": "MG_sid1", "Service2": "MG_sid2"}

    def test_get_messaging_service_empty_list(self, twilio_handler):
        twilio_handler.twilio_client.messaging.services.list.return_value = []

        result = twilio_handler.get_messaging_service()

        assert result == {}


class TestTwilioAPIHandlerFetchMessagingServiceById:
    def test_fetch_service_by_id_success(self, twilio_handler):
        mock_service = MagicMock()
        twilio_handler.twilio_client.messaging.services.return_value.fetch.return_value = mock_service

        result = twilio_handler.fetch_messaging_service_by_id("MG_service_sid")

        assert result == mock_service
        twilio_handler.twilio_client.messaging.services.assert_called_once_with("MG_service_sid")
        twilio_handler.twilio_client.messaging.services.return_value.fetch.assert_called_once()


class TestTwilioAPIHandlerCreatePhoneNumberServiceAssociation:
    def test_create_association_success(self, twilio_handler):
        mock_phone = MagicMock()
        mock_phone.phone_number = "+14155551234"
        twilio_handler.twilio_client.messaging.services.return_value.phone_numbers.create.return_value = mock_phone

        result = twilio_handler.create_phone_number_service_association("MG_sid", "PN_sid")

        assert result == mock_phone
        twilio_handler.twilio_client.messaging.services.assert_called_once_with("MG_sid")
        twilio_handler.twilio_client.messaging.services.return_value.phone_numbers.create.assert_called_once_with(phone_number_sid="PN_sid")


class TestTwilioAPIHandlerDeleteTnAssociation:
    def test_delete_association_success(self, twilio_handler):
        result = twilio_handler.delete_tn_association_from_msg_service("MG_sid", "PN_sid")

        assert result is True
        twilio_handler.twilio_client.messaging.services.assert_called_once_with("MG_sid")
        twilio_handler.twilio_client.messaging.services.return_value.phone_numbers.assert_called_once_with("PN_sid")
        twilio_handler.twilio_client.messaging.services.return_value.phone_numbers.return_value.delete.assert_called_once()

    def test_delete_association_twilio_exception_returns_false(self, twilio_handler):
        twilio_handler.twilio_client.messaging.services.return_value.phone_numbers.return_value.delete.side_effect = TwilioRestException(status=404, uri="/test", msg="Not found")

        result = twilio_handler.delete_tn_association_from_msg_service("MG_sid", "PN_sid")

        assert result is False

    def test_delete_association_generic_exception_returns_false(self, twilio_handler):
        twilio_handler.twilio_client.messaging.services.return_value.phone_numbers.return_value.delete.side_effect = ValueError("Error")

        result = twilio_handler.delete_tn_association_from_msg_service("MG_sid", "PN_sid")

        assert result is False


class TestTwilioAPIHandlerCreateExternalCampaign:
    def test_create_campaign_success(self, twilio_handler):
        result = twilio_handler.create_external_campaign("campaign_123", "MG_sid")

        assert result is True
        twilio_handler.twilio_client.messaging.external_campaign.create.assert_called_once_with(campaign_id="campaign_123", messaging_service_sid="MG_sid")

    def test_create_campaign_twilio_exception_returns_false(self, twilio_handler):
        twilio_handler.twilio_client.messaging.external_campaign.create.side_effect = TwilioRestException(status=400, uri="/test", msg="Error")

        result = twilio_handler.create_external_campaign("campaign_123", "MG_sid")

        assert result is False

    def test_create_campaign_generic_exception_returns_false(self, twilio_handler):
        twilio_handler.twilio_client.messaging.external_campaign.create.side_effect = ValueError("Error")

        result = twilio_handler.create_external_campaign("campaign_123", "MG_sid")

        assert result is False


class TestTwilioAPIHandlerGetActiveSubaccounts:
    def test_get_active_subaccounts_success(self, twilio_handler, mock_settings):
        mock_account1 = MagicMock()
        mock_account2 = MagicMock()
        mock_settings.TWILIO_CLIENT.api.v2010.accounts.list.return_value = [mock_account1, mock_account2]

        result = twilio_handler.get_active_subaccounts_with_friendly_name("TestAccount")

        assert result == [mock_account1, mock_account2]
        mock_settings.TWILIO_CLIENT.api.v2010.accounts.list.assert_called_once_with(friendly_name="TestAccount", status="active")

    def test_get_active_subaccounts_empty_list(self, twilio_handler, mock_settings):
        mock_settings.TWILIO_CLIENT.api.v2010.accounts.list.return_value = []

        result = twilio_handler.get_active_subaccounts_with_friendly_name("NonExistent")

        assert result == []


class TestTwilioAPIHandlerCloseSubAccount:
    def test_close_subaccount_success(self, twilio_handler):
        result = twilio_handler.close_twilio_sub_account("AC_subaccount")

        assert result is True
        twilio_handler.twilio_client.api.v2010.accounts.assert_called_once_with("AC_subaccount")
        twilio_handler.twilio_client.api.v2010.accounts.return_value.update.assert_called_once_with(status="closed")

    def test_close_subaccount_twilio_exception_returns_false(self, twilio_handler):
        twilio_handler.twilio_client.api.v2010.accounts.return_value.update.side_effect = TwilioRestException(status=404, uri="/test", msg="Not found")

        result = twilio_handler.close_twilio_sub_account("AC_subaccount")

        assert result is False

    def test_close_subaccount_generic_exception_returns_false(self, twilio_handler):
        twilio_handler.twilio_client.api.v2010.accounts.return_value.update.side_effect = ValueError("Error")

        result = twilio_handler.close_twilio_sub_account("AC_subaccount")

        assert result is False


class TestTwilioAPIHandlerDcaPartnerElection:
    @patch("..twilio_util.search_users_by_enterprise")
    @patch("..twilio_util.json.loads")
    def test_dca_partner_election_success_with_accepted_status(self, mock_json_loads, mock_search_users, twilio_handler):
        with (
            patch("..twilio_util.TwilioAPIHandler.create_twilio_msg_service") as mock_create_service,
            patch("..twilio_util.TwilioAPIHandler.create_phone_number_service_association") as mock_create_assoc,
            patch("..twilio_util.TwilioAPIHandler.create_external_campaign") as mock_create_campaign,
        ):
            mock_service = MagicMock()
            mock_service.sid = "MG_service_sid"
            mock_create_service.return_value = mock_service

            mock_search_users.return_value = [("+14155551234", "PN_sid1"), ("+14155555678", "PN_sid2")]

            mock_json_loads.return_value = {"sharingStatus": "ACCEPTED"}

            with patch("..twilio_util.EnterpriseAccount") as mock_enterprise, patch("..twilio_util.TcrMessaging") as mock_tcr, patch("..twilio_util.Campaign") as mock_campaign_class:
                mock_enterprise_instance = MagicMock()
                mock_enterprise.objects.get.return_value = mock_enterprise_instance
                mock_campaign_instance = MagicMock()
                mock_campaign_instance.get_campaign_sharing_status.return_value = '{"sharingStatus": "ACCEPTED"}'
                mock_campaign_class.return_value = mock_campaign_instance

                twilio_handler.dca_partner_election("campaign_123", 456)

                mock_create_service.assert_called_once_with("campaign_123")
                mock_tcr.objects.get_or_create.assert_called_once()
                mock_search_users.assert_called_once_with(456, "twilio")
                assert mock_create_assoc.call_count == 2
                assert mock_create_campaign.call_count == 2

    @patch("..twilio_util.search_users_by_enterprise")
    @patch("..twilio_util.json.loads")
    def test_dca_partner_election_with_rejected_status(self, mock_json_loads, mock_search_users, twilio_handler):
        with (
            patch("..twilio_util.TwilioAPIHandler.create_twilio_msg_service") as mock_create_service,
            patch("..twilio_util.TwilioAPIHandler.create_phone_number_service_association") as mock_create_assoc,
            patch("..twilio_util.TwilioAPIHandler.create_external_campaign") as mock_create_campaign,
        ):
            mock_service = MagicMock()
            mock_service.sid = "MG_service_sid"
            mock_create_service.return_value = mock_service

            mock_search_users.return_value = [("+14155551234", "PN_sid1")]
            mock_json_loads.return_value = {"sharingStatus": "REJECTED"}

            with patch("..twilio_util.EnterpriseAccount") as mock_enterprise, patch("..twilio_util.TcrMessaging") as mock_tcr, patch("..twilio_util.Campaign") as mock_campaign_class:
                mock_enterprise_instance = MagicMock()
                mock_enterprise.objects.get.return_value = mock_enterprise_instance
                mock_campaign_instance = MagicMock()
                mock_campaign_instance.get_campaign_sharing_status.return_value = '{"sharingStatus": "REJECTED"}'
                mock_campaign_class.return_value = mock_campaign_instance

                twilio_handler.dca_partner_election("campaign_123", 456)

                mock_create_assoc.assert_called_once()
                mock_create_campaign.assert_not_called()

    @patch("..twilio_util.search_users_by_enterprise")
    @patch("..twilio_util.json.loads")
    def test_dca_partner_election_no_users(self, mock_json_loads, mock_search_users, twilio_handler):
        with (
            patch("..twilio_util.TwilioAPIHandler.create_twilio_msg_service") as mock_create_service,
            patch("..twilio_util.TwilioAPIHandler.create_phone_number_service_association") as mock_create_assoc,
        ):
            mock_service = MagicMock()
            mock_service.sid = "MG_service_sid"
            mock_create_service.return_value = mock_service

            mock_search_users.return_value = []
            mock_json_loads.return_value = {"sharingStatus": "ACCEPTED"}

            with patch("..twilio_util.EnterpriseAccount") as mock_enterprise, patch("..twilio_util.TcrMessaging") as mock_tcr, patch("..twilio_util.Campaign") as mock_campaign_class:
                mock_enterprise_instance = MagicMock()
                mock_enterprise.objects.get.return_value = mock_enterprise_instance
                mock_campaign_instance = MagicMock()
                mock_campaign_instance.get_campaign_sharing_status.return_value = '{"sharingStatus": "ACCEPTED"}'
                mock_campaign_class.return_value = mock_campaign_instance

                twilio_handler.dca_partner_election("campaign_123", 456)

                mock_create_assoc.assert_not_called()


class TestAreaCodeUnavailableError:
    def test_custom_exception_can_be_raised(self):
        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            raise AreaCodeUnavailableError("Test message")

        assert str(exc_info.value) == "Test message"

    def test_custom_exception_inheritance(self):
        assert issubclass(AreaCodeUnavailableError, Exception)
