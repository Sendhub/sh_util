import base64
import json
import logging
import os
import re
import sys
import time
import traceback
from typing import List

import phonenumbers
import requests
import xmltodict

# Note: bandwidth import moved to function level to avoid missing package dependency
# from main.models import ShUser
from .cleanup import cleanup_phone_number

try:
    import settings
except ImportError:
    # when unit testing is invoked
    sys.path.append("/opt/sendhub/inforeach/app")
    import settings

try:
    from .cleanup import display_number, validate_phone_number
    from .twilio_util import AreaCodeUnavailableError
except ImportError:
    sys.path.append("/opt/sendhub/inforeach/app")
    from .cleanup import display_number, validate_phone_number
    from .twilio_util import AreaCodeUnavailableError


class BandwidthOrderPendingException(Exception):
    """Exception when Toll Free Number is unavailable."""


class BWNumberUnavailableError(Exception):
    """Exception when requested BW Number is unavailable."""


class BWTollFreeUnavailableError(Exception):
    """Exception when Toll Free Number is unavailable."""


class BWTagCharacterLimitExceededException(Exception):
    """Exception when the tag limit exceeds 2024 characters"""


class BWMessageCharacterLimitExceededException(Exception):
    """Exception when the message limit exceeds 2048 characters"""


class BWLengthOfMediaURLLimitExceededException(Exception):
    """Exception when the length of media URL's exceed 4096 characters"""


class BandwidthSiteMisconfiguredError(Exception):
    """Raised when BW_SITE_ID/BW_SITE_ID_AU is unset or not a valid numeric site id."""


class BandwidthSiteMismatchError(Exception):
    """Raised when a number belongs to a different Bandwidth site than expected."""


class BandwidthSiteUnverifiedError(Exception):
    """Raised when a number's owning Bandwidth site could not be determined."""


class BandwidthBulkReleaseError(Exception):
    """Raised when release_phone_number() is called with more than one number."""


class BandwidthAvailablePhoneNumber:
    """
    for bandwidth carrier, numbers returned are number (if qty = 1),
    or a list of numbers. This router also converts bandwidth list
    to list of numbers in dictionary with pretty name as key..
    similar to format provided by Twilio so that upper layers are
    at ease. Each number is in the format below:

    {"friendly_name":"(580) 271-9612", "phone_number":"+15802719612"}
    """

    def __init__(self, number):
        self.friendly_name = display_number(number)
        self.phone_number = number
        self.gateway = settings.SMS_GATEWAY_BANDWIDTH


def phonenumber_as_e164(number, country_code="US"):
    """
    This function should be called mainly with valid phone numbers.
    Exception is raised if number is invalid
    """
    if not isinstance(number, str):
        number = str(number)
    if validate_phone_number(number, False) is False:
        raise ValueError(f"Invalid phone number {number} - unable to process")
    if isinstance(number, bytes):
        number = number.decode("utf-8")
    return phonenumbers.format_number(phonenumbers.parse(number, country_code), phonenumbers.PhoneNumberFormat.E164)


class BandwidthNumberObject:
    """
    Returns an object with number and sid
       (sid is not used)
    to be compatible with twilio number object
    to minimize changes
    """

    def __init__(self, number, sid):
        self.phone_number = number
        self.sid = sid


class SHBandwidthClient:
    NUMBER_UNAVAILABLE_MSG = "We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again or choose a different area code."
    JSON_CONTENT_TYPE = "application/json"

    def __init__(self, userid=None, token=None, secret=None, username=None, password=None, debug=False, client_id=None, client_secret=None, use_oauth2=None):
        # Import moved here to avoid missing package dependency
        import bandwidth

        if not userid:
            userid = settings.BW_USER_ID
        if not token:
            token = settings.BW_API_TOKEN
        if not secret:
            secret = settings.BW_API_SECRET

        # username and password are used for account API authentication
        if not username:
            username = settings.BW_USERNAME
        if not password:
            password = settings.BW_PASSWORD

        # OAuth2 client-credentials auth - disabled by default (settings.BW_USE_OAUTH2),
        # see _get_auth_header for the switchover.
        if use_oauth2 is None:
            use_oauth2 = getattr(settings, "BW_USE_OAUTH2", False)
        if not client_id:
            client_id = getattr(settings, "BW_CLIENT_ID", None)
        if not client_secret:
            client_secret = getattr(settings, "BW_CLIENT_SECRET", None)

        self.token = token
        self.secret = secret
        self.username = username
        self.password = password
        self.use_oauth2 = use_oauth2
        self.client_id = client_id
        self.client_secret = client_secret

        self.bw_app_id = settings.BW_APP_ID

        self.user_id_na = userid
        self.user_id_au = settings.BW_USER_ID_AU

        self.bw_account_api_url_na = settings.BW_ACCOUNT_API_URL
        self.bw_account_api_url_au = settings.BW_ACCOUNT_API_URL_AU

        self.bw_site_id_na = settings.BW_SITE_ID
        self.bw_site_id_au = settings.BW_SITE_ID_AU

        if self.use_oauth2:
            self.configuration = bandwidth.Configuration(client_id=self.client_id, client_secret=self.client_secret)
            if not userid or not client_id or not client_secret:
                raise ValueError(f"Appropriate Bandwidth OAuth2 Keys are not available supplied userid: {userid}, client_id: {client_id}, client_secret: {client_secret}")
        else:
            self.configuration = bandwidth.Configuration(username=self.username, password=self.password)
            if not userid or not token or not secret or not username or not password:
                raise ValueError(f"Appropriate Bandwidth Keys are not available supplied userid: {userid}, token: {token}, secret: {secret}, username: {username}, password: {password}")

        logging.info("Inside the __init__ of SHBandwidthClient")

    def _get_encoded_credentials(self):
        credentials = self.username + ":" + self.password
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        return encoded_credentials

    def _get_common_auth_header(self):
        headers = {"Authorization": f"Basic {self._get_encoded_credentials()}"}
        return headers

    def _get_oauth_bearer_header(self):
        # Configuration.get_access_token() fetches + caches the client-credentials token
        # (see bandwidth.Configuration.get_access_token / auth_settings in the SDK).
        return {"Authorization": f"Bearer {self.configuration.get_access_token()}"}

    def _get_auth_header(self):
        """
        Authorization header for Bandwidth's Numbers/Account (dashboard) APIs, used by every
        raw ``requests`` call in this class. Switches between legacy Basic Auth and OAuth2
        client-credentials bearer tokens based on ``settings.BW_USE_OAUTH2``.
        """
        if self.use_oauth2:
            return self._get_oauth_bearer_header()
        return self._get_common_auth_header()

    @staticmethod
    def _as_e164(number, country_code="US"):
        """
        This function should be called mainly with valid phone numbers.
        Exception is raised if number is invalid.
        """
        try:
            return phonenumber_as_e164(number, country_code)
        except ValueError as err:
            logging.error("Invalid phone number %s", str(err))
            raise ValueError

    def check_if_valid_e164_format(self, number: str) -> bool:
        """
        Checks if the given phone number is in valid E.164 format.

        Valid E.164 format:
        - Must start with '+'
        - Followed by digits only
        - Must contain between 8 and 15 digits total (including country code)
        """
        if not isinstance(number, str):
            return False

        # Regex for E.164 validation
        pattern = re.compile(r"^\+[1-9]\d{7,14}$")
        return bool(pattern.match(number))

    def _cleanup_and_return_numbers(self, numbers, quantity, country_code="US"):
        """
        Helper function that takes in the numbers list returned by BW APIs, formats them and
        Returns:
        number itself if quantity is 1 else returns list of numbers.
        """
        try:
            if quantity == 1:
                return self._as_e164(numbers[0], country_code)
            elif quantity > 1:
                return [self._as_e164(number, country_code) for number in numbers]
            else:
                raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")
        except ValueError as err:
            logging.error(f"Phone number error {err}")
            raise ValueError

    def _parse_number_to_bw_format(self, number, country_code="US"):
        """Stripts the prefix '+1' from the 12 char number like '+12123456789'"""
        parsed = phonenumbers.parse(str(number), country_code)
        return str(parsed.national_number)

    def send_hello(self, from_number, to_number):
        try:
            return self.send_sms(from_number, to_number, "Hello from Sendhub through Bandwidth!")
        except ValueError as err:
            logging.error(f"Error sending message {err}")

    def check_msg_status(self, msg_id):
        return self.get_message_info(msg_id)

    def check_recipient_list_validity(self, numbers) -> List:
        """
        This method's purpose is to check if the to_numbers are in valid E.164 format.
        In case they are not then it will attempt it to convert the number to valid E.164 format.
        If the formatting to E.164 fails then at number will not make it to the final to_list.
        """
        result = []
        if isinstance(numbers, List):
            for num in numbers:
                if self.check_if_valid_e164_format(num) and validate_phone_number(num):
                    result.append(num)
                else:
                    try:
                        result.append(self._as_e164(num))
                    except ValueError:
                        print(f"Could not convert {num} to valid E.164 format")
        else:
            raise TypeError(f"Expected argument of type list, instead got of type: {numbers}")

        return result

    # Updated To Bandwidth-SDK 20.2.1
    def send_sms(self, from_number, to_number, msg, tag=None):
        """
        Sends SMS via Bandwidth-SDK 20.0.0 API call.

        Args:
            from_number:    Either an alphanumeric sender ID or the sender's Bandwidth phone number in E.164 format, which must be hosted within Bandwidth and linked to the account that is generating the message.
                            Alphanumeric Sender IDs can contain up to 11 characters, upper-case letters A-Z, lower-case letters a-z, numbers 0-9, space, hyphen -, plus +, underscore _ and ampersand &.
                            Alphanumeric Sender IDs must contain at least one letter.
            to_number:  The phone number(s) the message should be sent to in E164 format.
            msg:        The contents of the text message. Must be 2048 characters or less.
            tag (optional): A custom string that will be included in callback events of the message. Max 1024 characters.
        Returns:
            id: Upon successfully creating the message with bandwith API the id received in the response is returned

        API Documentation URL:  https://dev.bandwidth.com/apis/messaging-apis/messaging/#tag/Messages/operation/createMessage

        The api_instance.create_message call returns in below format
        <class 'bandwidth.models.message.Message'>
        Message
        (
            id='1754980470643j7jdkui3u4g2ra3q',
            owner='+18332420240',
            application_id='22cea654-e841-4bce-9120-1f56850de2c3',
            time=datetime.datetime(2025, 8, 12, 6, 34, 30, 643653, tzinfo=TzInfo(UTC)),
            segment_count=1,
            direction=<MessageDirectionEnum.OUT: 'out'>,
            to=['+14109894472'],
            var_from='+18332420240',
            text='To version 20',
            tag=None,
            priority=None,
            expiration=None,
            additional_properties={}
        )
        """
        # allow sending to a group in one call
        if not isinstance(to_number, list):
            to_number = [to_number]

        to_number = self.check_recipient_list_validity(to_number)

        if not isinstance(tag, str):
            tag = str(tag)

        if len(msg) > 2046:
            raise BWMessageCharacterLimitExceededException("Bandwidth-SDK 20.0.0 allows only 2048 characters per message")

        if tag and len(tag) > 1024:
            raise BWTagCharacterLimitExceededException("Bandwidth-SDK 20.0.0 allows only 1024 characters per tag")

        if from_number.startswith("+61"):
            account_id = self.user_id_au
        else:
            account_id = self.user_id_na

        try:
            # Import moved here to avoid missing package dependency
            import bandwidth

            with bandwidth.ApiClient(self.configuration) as api_client:
                api_instance = bandwidth.MessagesApi(api_client)
                message_request = bandwidth.MessageRequest(application_id=self.bw_app_id, to=to_number, var_from=from_number, text=msg, tag=tag)
                try:
                    logging.info("Calling the MessagesApi -> create_message for SMS")
                    api_response = api_instance.create_message(account_id, message_request)
                    logging.info(f"The response of MessagesApi -> create_message: {api_response}")
                    return api_response.id

                except Exception as e:
                    logging.error(f"Exception when calling MessagesApi -> create_message: {e}")
                    logging.error(traceback.format_exc())

        except Exception as e:
            logging.error(f"Exception occurred while sending sms via bandwidth: {e}")
            logging.error(traceback.format_exc())
            raise

    # Updated To Bandwidth-SDK 20.2.1
    def send_mms(self, from_number, to_number, msg, media, tag=None):
        """
        Sends MMS via Bandwidth-SDK 20.0.0 API call.

        Args:
            from_number:    Either an alphanumeric sender ID or the sender's Bandwidth phone number in E.164 format, which must be hosted within Bandwidth and linked to the account that is generating the message.
                            Alphanumeric Sender IDs can contain up to 11 characters, upper-case letters A-Z, lower-case letters a-z, numbers 0-9, space, hyphen -, plus +, underscore _ and ampersand &.
                            Alphanumeric Sender IDs must contain at least one letter.
            to_number:  The phone number(s) the message should be sent to in E164 format.
            msg:        The contents of the text message. Must be 2048 characters or less.
            media:      A list of URLs to include as media attachments as part of the message. Each URL can be at most 4096 characters.
            tag (optional): A custom string that will be included in callback events of the message. Max 1024 characters.

        Returns:
            id: Upon successfully creating the message with bandwith API the id received in the response is returned

        API Documentation URL:  https://dev.bandwidth.com/apis/messaging-apis/messaging/#tag/Messages/operation/createMessage

        The api_instance.create_message call returns in below format
        <class 'bandwidth.models.message.Message'>
        Message
        (
            id='1754980470643j7jdkui3u4g2ra3q',
            owner='+18332420240',
            application_id='22cea654-e841-4bce-9120-1f56850de2c3',
            time=datetime.datetime(2025, 8, 12, 6, 34, 30, 643653, tzinfo=TzInfo(UTC)),
            segment_count=1,
            direction=<MessageDirectionEnum.OUT: 'out'>,
            to=['+14109894472'],
            var_from='+18332420240',
            media=['https://picsum.photos/id/237/200/300'],
            text='To version 20',
            tag=None,
            priority=None,
            expiration=None,
            additional_properties={}
        )
        """
        # allow sending to a group in one call
        if not isinstance(to_number, list):
            to_number = [to_number]

        to_number = self.check_recipient_list_validity(to_number)

        if not isinstance(tag, str):
            tag = str(tag)

        if len(msg) > 2046:
            raise BWMessageCharacterLimitExceededException("Bandwidth-SDK 20.0.0 allows only 2048 characters per message")

        if tag and len(tag) > 1024:
            raise BWTagCharacterLimitExceededException("Bandwidth-SDK 20.0.0 allows only 1024 characters per tag")

        for item in media:
            if len(item) > 4096:
                raise BWLengthOfMediaURLLimitExceededException("Bandwidth-SDK 20.0.0 allows only 4096 characters per media URL")

        if from_number.startswith("+61"):
            account_id = self.user_id_au
        else:
            account_id = self.user_id_na

        try:
            # Import moved here to avoid missing package dependency
            import bandwidth

            with bandwidth.ApiClient(self.configuration) as api_client:
                api_instance = bandwidth.MessagesApi(api_client)
                message_request = bandwidth.MessageRequest(application_id=self.bw_app_id, to=to_number, var_from=from_number, text=msg, media=media, tag=tag)
                try:
                    logging.info("Calling the MessagesApi -> create_message for MMS")
                    api_response = api_instance.create_message(account_id, message_request)
                    logging.info(f"The response of MessagesApi -> create_message for MMS: {api_response}")
                    return api_response.id

                except Exception as e:
                    logging.error(f"Exception when calling MessagesApi -> create_message for MMS: {e}")
                    logging.error(traceback.format_exc())

        except Exception as e:
            logging.error(f"Exception occurred while sending sms via bandwidth: {e}")
            logging.error(traceback.format_exc())
            raise

    # Updated To Bandwidth-SDK 20.2.1
    def get_message_info(self, msgid=None, country_code="US"):
        """
        Get the detail of a message already sent via Bandwidth-SDK 20.0.0 API call.

        Args:
            msgid:  The ID of the message to search for. Special characters need to be encoded using URL encoding.
                    Message IDs could come in different formats, e.g., 9e0df4ca-b18d-40d7-a59f-82fcdf5ae8e6 and 1589228074636lm4k2je7j7jklbn2 are valid message ID formats.
                    Note that you must include at least one query parameter.
        Returns:
            id: Upon successfully creating the message with bandwith API the id received in the response is returned

        API Documentation URL:  https://dev.bandwidth.com/apis/messaging-apis/messaging/#tag/Messages/operation/listMessages

        Returns message info in the below format.
        <class 'bandwidth.models.messages_list.MessagesList'>
        MessagesList
        (
            total_count=1,
            page_info=PageInfo(prev_page=None, next_page=None, prev_page_token=None, next_page_token=None, additional_properties={}),
            messages=
            [
                ListMessageItem(message_id='1754885848276h2fvl5qpvsfasimh', account_id='5004525', source_tn='+18332420240', destination_tn='+14109894472',
                message_status=<MessageStatusEnum.UNDELIVERED: 'UNDELIVERED'>, message_direction=<ListMessageDirectionEnum.OUTBOUND: 'OUTBOUND'>,
                message_type=<MessageTypeEnum.SMS: 'sms'>, segment_count=1, error_code=4795, receive_time=datetime.datetime(2025, 8, 11, 4, 17, 28, 583000, tzinfo=TzInfo(UTC)),
                carrier_name='Other', message_size=None, message_length=13, attachment_count=None, recipient_count=None, campaign_class='Unregistered', campaign_id='AGGREGATOR', additional_properties={})
            ],
            additional_properties={}
        )
        """

        if country_code == "US" or country_code == "CA":
            account_id = self.user_id_na
        elif country_code == "AU":
            account_id = self.user_id_au

        # Import moved here to avoid missing package dependency
        import bandwidth

        with bandwidth.ApiClient(self.configuration) as api_client:
            api_instance = bandwidth.MessagesApi(api_client)
            try:
                logging.info("Calling the MessagesApi -> list_messages")
                api_response = api_instance.list_messages(account_id=account_id, message_id=msgid)
                logging.info(f"The response of MessagesApi -> list_messages: {api_response}")
                return api_response
            except Exception as e:
                logging.error(f"Exception when calling MessagesApi -> list_messages: {e}")
                logging.error(traceback.format_exc())
                raise

    # Updated To Bandwidth-SDK 20.2.1
    def in_service(self, number, country_code="US"):
        """
        Check if a number is in service in our account

        Args:
            number:  The phone number which we want to check is active with bandwidth
        Returns:
            True if number is in service
            False if number not in service

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/In-service-Numbers/operation/ReadInserviceTn

        API Response Format:
        <Response [200]> {'_content': b'', '_content_consumed': True, '_next': None, 'status_code': 200}
        <Response [404]> {'_content': b'', '_content_consumed': True, '_next': None, 'status_code': 404}
        """

        if not validate_phone_number(number, False):
            raise ValueError(f"Invalid phone number ({number}) passed")

        nat_number = phonenumber_as_e164(number)
        nat_number = self._parse_number_to_bw_format(str(nat_number), "US")
        retval = False

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/inserviceNumbers/{str(nat_number)}"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/inserviceNumbers/{str(nat_number)}"

        try:
            logging.info(f"Making request to Bandwidth to check if number {nat_number} is in service or not")
            response = requests.get(endpoint, headers=self._get_auth_header())
            logging.info(f"Response received from bandwidth to get InService for Phone Number {nat_number}  is {response.status_code}")

            if response.status_code == 200:
                retval = True
            elif response.status_code == 404:
                retval = False
            else:
                logging.info(f"Error response received from bandwidth to get InService for Phone Number {nat_number}  is {response.__dict__}")

        except Exception as e:
            logging.error(f"Fetchng InService for Phone Number {number} - error: {e}")
            logging.info(f"Response received from bandwidth to get InService for Phone Number {nat_number}  is {response.__dict__}")
            logging.error(traceback.format_exc())

        return retval

    # Updated To Bandwidth-SDK 20.2.1
    def find_number_in_area_code(self, area_code, quantity=1, country_code="US"):
        """
        Find a number within an area code.

        Args:
            area_code:  The phone number which we want to check is active with bandwidth
            quantity:   Has to be in range of 1 to 5000
            country_code: Can be 'US' for United States Of America or 'AU' for Australia

        Returns:
            A list of numbers

        API Documentation URL:  https://dev.bandwidth.com/docs/numbers/guides/searchingForNumbers/#tag/Service-Activation/operation/serviceActivationCheck
        """

        cleaned_numbers = []
        response = None
        response_data = None

        if quantity < 1:
            raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/accounts/{str(self.user_id_na)}/availableNumbers?areaCode={area_code}&quantity={quantity}"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/accounts/{str(self.user_id_au)}/availableNumbers?areaCode={area_code}&quantity={quantity}"

        try:
            logging.info(f"Making Request to bandwidth to get {quantity} number for Area Code {area_code}")
            response = requests.get(endpoint, headers=self._get_auth_header())
            logging.info(f"Response Status Code received from bandwidth to get {quantity} number for Area Code {area_code} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                """
                    Format of response_data
                    {'SearchResult': {'ResultCount': '1', 'TelephoneNumberList': {'TelephoneNumber': '9192052618'}}}
                    {'SearchResult': {'ResultCount': '2', 'TelephoneNumberList': {'TelephoneNumber': ['9192052618', '9192053260']}}}
                """
                numbers = response_data.get("SearchResult").get("TelephoneNumberList").get("TelephoneNumber")
                logging.info(f"Calling cleanupPhoneNumber() on the received phone numbers(s) {numbers} from bandwidth")

                if isinstance(numbers, str):
                    numbers = [numbers]
                cleaned_numbers = list(map(cleanup_phone_number, numbers))
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Failed to search for phone number(s) in given area code - error: {e}")
            logging.info(f"Response received from bandwidth to get {quantity} number(s) for Area Code {area_code} is {response.__dict__}")
            logging.error(traceback.format_exc())
            raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG) from e

        return self._cleanup_and_return_numbers(cleaned_numbers, quantity)

    # Updated To Bandwidth-SDK 20.2.1
    def search_available_toll_free_number(self, pattern=None, quantity=1, country_code="US"):
        """
        Search toll free number.
        Find a number within an area code.

        Args:
            pattern:    A 3 digit pattern between 8**, 80*, 87* (Currently 80* is having issues)
            quantity:   Has to be more than or equal to 1
        Returns:
            A list of numbers

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/v1/#tag/Available-Tns/operation/GetAvailableTns
        """
        cleaned_numbers = []
        response = None
        response_data = None

        if quantity < 1:
            raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")

        pattern = pattern if pattern in ("8**", "80*", "87*") else "8**"

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v1/accounts/{str(self.user_id_na)}/availableNumbers?tollFreeWildCardPattern={pattern}&quantity={quantity}"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v1/accounts/{str(self.user_id_au)}/availableNumbers?tollFreeWildCardPattern={pattern}&quantity={quantity}"

        try:
            logging.info(f"Making Request to bandwidth to get {quantity} Toll Free Number with pattern {pattern}")
            response = requests.get(endpoint, headers=self._get_auth_header())
            logging.info(f"Response received from bandwidth to get {quantity} Toll Free Number with pattern {pattern} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                """
                    Format of response_data
                    {'SearchResult': {'ResultCount': '1', 'TelephoneNumberList': {'TelephoneNumber': '9192052618'}}}
                    {'SearchResult': {'ResultCount': '2', 'TelephoneNumberList': {'TelephoneNumber': ['9192052618', '9192053260']}}}
                """
                if response_data.get("SearchResult"):
                    numbers = response_data.get("SearchResult").get("TelephoneNumberList").get("TelephoneNumber")
                    logging.info(f"Calling cleanupPhoneNumber() on the received toll free phone numbers(s) {numbers} from bandwidth")

                    if isinstance(numbers, str):
                        numbers = [numbers]
                        cleaned_numbers = list(map(cleanup_phone_number, numbers))
                elif response.status_code == 200 and response_data.get("SearchResult") is None:
                    logging.info(f"No toll free phonenumbers are available for pattern ({pattern})")
                    return None
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Failed to search for {quantity} toll free phone number(s) with pattern {pattern} - error: {e}")
            logging.info(f"Response received from bandwidth to get {quantity} number(s) for pattern {pattern} is {response.__dict__}")
            logging.error(traceback.format_exc())
            raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG) from e

        return self._cleanup_and_return_numbers(cleaned_numbers, quantity)

    # Updated To Bandwidth-SDK 20.2.1
    def get_number_info(self, phone_number, country_code="US"):
        """
        Search the details of a phone number.

        Args:
            phone_number:    Send a phone number in +18332420240
        Returns:
            Json of the response from bandwidth API

        API Documentation URL:  https://dev.bandwidth.com/docs/numbers/guides/manage-inventory/searchingNumbers/

        API response format has changed, new response format received in XML
        After converting it to dict
        {
            'TelephoneNumberResponse':
            {
                'TelephoneNumberDetails':
                {
                    'FullNumber': '8332420240',
                    'VendorId': '67',
                    'VendorName': 'Toll free vendor',
                    'OnNetVendor': 'false',
                    'Status': 'Inservice',
                    'AccountId': '5004525',
                    'Site':
                    {
                        'Id': '21391',
                        'Name': 'Test Environments'
                    },
                    'SipPeer':
                    {
                        'PeerId': '568351',
                        'PeerName': 'Test Dev Environment', '
                        IsDefaultPeer': 'true'
                    },
                    'ServiceTypes':
                    {
                        'ServiceType': ['Voice', 'Messaging']
                    },
                    'LastModified': '2024-02-01T22:13:16.000Z',
                    'MessagingSettings':
                    {
                        'SmsEnabled': 'true',
                        'MessageClass': 'AGGA2P',
                        'CampaignFullyProvisioned': 'false',
                        'A2pState': 'system_default',
                        'AssignedNnRoute':
                        {
                            'Nnid': '103462',
                            'Name': 'BW TF - Zipwhip - E980 (103462)'
                        }
                    }
                }
            }
        }
        """

        response = None
        response_data = None

        if not validate_phone_number(phone_number, False):
            raise ValueError(f"Invalid phone number ({phone_number}) passed")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/tnDetails"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/tnDetails"

        try:
            logging.info(f"Making Request to bandwidth to get {phone_number} detail information ")
            response = requests.get(endpoint, headers=self._get_auth_header())
            logging.info(f"Response Status Code received from bandwidth to get {phone_number} detail information is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response received from bandwidth to get {phone_number} detail information is : {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth to get {phone_number} detail information - error: {e}")
            logging.info(f"Response received from bandwidth to get {phone_number} detail information is {response.__dict__}")
            logging.error(traceback.format_exc())
            raise

        return response_data

    def _resolve_site_id(self, country_code="US", site_id=None):
        """
        Resolve and validate the Bandwidth site (sub-account) id for this environment.

        Staging and production share one Bandwidth account; the site id is the only thing
        separating their inventories. An unset value must never silently degrade into
        "whatever the account owns", so this fails loudly instead of returning None.

        Raises:
            BandwidthSiteMisconfiguredError: If the site id is unset, a placeholder, or non-numeric
        """
        if not site_id:
            site_id = self.bw_site_id_na if country_code in ("US", "CA") else self.bw_site_id_au

        site_id = str(site_id).strip() if site_id is not None else ""

        if not site_id.isdigit():
            raise BandwidthSiteMisconfiguredError(
                f"Refusing to continue: site id for country {country_code} is not a valid numeric site id (got {site_id!r}). Set BW_SITE_ID / BW_SITE_ID_AU for this environment."
            )

        return site_id

    def _assert_number_belongs_to_site(self, number, expected_site_id, country_code="US"):
        """
        Confirm a number is provisioned under expected_site_id before a destructive op.

        Fails closed: a lookup that errors, or returns no site, blocks the operation
        rather than letting it proceed on an unverified number.

        Raises:
            BandwidthSiteUnverifiedError: If the owning site could not be determined
            BandwidthSiteMismatchError: If the number belongs to a different site
        """
        try:
            site_info = self.get_siteinfo_for_number(number, country_code=country_code)
        except Exception as e:
            raise BandwidthSiteUnverifiedError(f"Refusing to act on {number}: could not determine its Bandwidth site ({e}).") from e

        actual_site_id = str((site_info or {}).get("Id", "")).strip()
        if not actual_site_id:
            raise BandwidthSiteUnverifiedError(f"Refusing to act on {number}: Bandwidth returned no site id (response: {site_info}).")

        if actual_site_id != str(expected_site_id):
            raise BandwidthSiteMismatchError(f"Refusing to act on {number}: it belongs to site {actual_site_id} ({(site_info or {}).get('Name')}), not this environment's site {expected_site_id}.")

        logging.info(f"Confirmed {number} belongs to site {actual_site_id}")
        return actual_site_id

    def _inservice_path(self, user_id, site_id=None, suffix=""):
        """
        Build an in-service inventory path, site-scoped whenever a site id is supplied.

        The site-scoped form is what keeps one environment from seeing another
        environment's numbers on a shared Bandwidth account.
        """
        if site_id:
            return f"/api/v2/accounts/{user_id}/sites/{site_id}/inserviceNumbers{suffix}"
        return f"/api/v2/accounts/{user_id}/inserviceNumbers{suffix}"

    # Updated To Bandwidth-SDK 20.2.1
    def release_phone_number(self, number, country_code="US", site_id=None):
        """
        Returns phone number 'number' back to bandwidth. By disconnecting it from our account

        Args:
            number:    Send a phone number in +19037811667
        Returns:
            Json of the response from bandwidth API

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/Disconnecting-Numbers/operation/CreateDisconnectOrder

        API response format has changed, new response format recived in XML
        After converting it to dict
        {
            'DisconnectTelephoneNumberOrderResponse':
            {
                'orderRequest':
                {
                    'OrderCreateDate': '2025-08-11T12:46:35.012Z',
                    'id': 'c8c73839-9166-4000-b6e3-7d3bf783e366',
                    'DisconnectTelephoneNumberOrderType':
                    {
                        'DisconnectMode': 'NORMAL',
                        'DisconnectReason': 'UNSPECIFIED',
                        'TelephoneNumberList':
                        {
                            'TelephoneNumber': '9037811667'
                        }
                    }
                },
                'OrderStatus': 'RECEIVED'
            }
        }
        """

        if isinstance(number, (list, tuple, set, frozenset, dict)):
            raise BandwidthBulkReleaseError(
                f"release_phone_number() takes exactly one number, got {type(number).__name__} of {len(number)}. Release numbers one at a time so each is verified against this environment's site."
            )

        response = None
        response_data = None

        number = str(number)
        if not validate_phone_number(number, False):
            raise ValueError(f"Invalid phone number ({number}) passed, unable to release")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")

        expected_site_id = self._resolve_site_id(country_code, site_id)
        self._assert_number_belongs_to_site(number, expected_site_id, country_code)

        if country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{self.user_id_na}/disconnects"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{self.user_id_au}/disconnects"

        headers = {**self._get_auth_header(), "Content-Type": self.JSON_CONTENT_TYPE}
        request_payload = {"disconnectOrderType": {"disconnectMode": "NORMAL", "phoneNumbers": [number]}}

        json_payload = json.dumps(request_payload)
        logging.info(f"To release number request payload: {json_payload}")

        try:
            logging.info(f"Making Request to bandwidth to release phone number {number}")
            response = requests.post(endpoint, headers=headers, data=json_payload)
            logging.info(f"Response Status Code received from bandwidth to release {number} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from Bandwidth to release number {number} is : {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth to release {number} detail information - error: {e}")
            logging.info(f"Response received from bandwidth to release {number} detail information is {response.__dict__}")
            logging.error(traceback.format_exc())
            raise

    # Updated To Bandwidth-SDK 20.2.1
    def get_active_number_count(self, site_id=None, country_code="US"):
        """
        Fetches the count of numbers for a given site

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/In-service-Numbers/operation/ReadInserviceTnsCount

        Args:
            site_id:  The site id of the account
        Returns:
            count of all the total numbers present
        """
        count = 0
        response = None
        response_data = None

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")

        site_id = self._resolve_site_id(country_code, site_id)
        user_id = self.user_id_na if country_code in ("US", "CA") else self.user_id_au
        if country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}{self._inservice_path(user_id, site_id, '/totals')}"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}{self._inservice_path(user_id, site_id, '/totals')}"

        headers = {**self._get_auth_header(), "Content-Type": self.JSON_CONTENT_TYPE}

        try:
            logging.info(f"Making Request to bandwidth get phone numbers configured for site_id: {site_id}")
            response = requests.get(endpoint, headers=headers)
            logging.info(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                count = response_data.get("Quantity").get("Count")
                logging.info(f"Total count of active numbers for site_id: {site_id} is: {count} numbers")
            else:
                logging.info(f"To get phone numbers configured for site_id: {site_id} Error Response from bandwidth is: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is - error: {e}")
            logging.info(f"Response received from bandwidth for site_id: {site_id} is {response.__dict__}")
            logging.error(traceback.format_exc())
            raise

        return count

    # Updated To Bandwidth-SDK 20.2.1
    def list_active_numbers(self, site_id=None, size=None, country_code="US"):
        """
        Fetches the list of all the numbers after going through pagination.
        Per response contains at most 500 is no size query params is specified

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/In-service-Numbers/operation/ReadInserviceTns

        Args:
            site_id:  The site id of the account
        Returns:
            list of all the numbers present
        """

        telephone_numbers_list = []
        total_count = 0
        remaining_count = None
        per_resp_number_count = 500
        response = None
        response_data = None
        endpoint = ""
        additional_query_params = ""
        page_count = 1
        ALL_SUCCESS_FLAG = False

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")

        site_id = self._resolve_site_id(country_code, site_id)
        user_id = self.user_id_na if country_code in ("US", "CA") else self.user_id_au
        if country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}{self._inservice_path(user_id, site_id)}"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}{self._inservice_path(user_id, site_id)}"

        headers = {**self._get_auth_header(), "Content-Type": self.JSON_CONTENT_TYPE}

        while True:
            logging.info(f"list_active_numbers pageCount: {page_count}")
            try:
                try:
                    logging.info(f"Making Request to bandwidth get phone numbers configured for site_id: {site_id}")
                    response = requests.get(endpoint + additional_query_params, headers=headers)
                except Exception as e:
                    logging.error(f"Issue occurred in additional_query_params: {additional_query_params} - error: {e}")
                    logging.info(f"Response received from bandwidth to fetch the additional_query_params: {additional_query_params} detail information is {response.__dict__}")
                    logging.error(traceback.format_exc())
                    raise

                logging.info(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is {response.status_code}")

                if response.status_code == 200:
                    response_data = xmltodict.parse(response.text)
                    links = response_data.get("TNs").get("Links")
                    logging.info(f"Links received in current requests: {links}")
                    total_count = response_data.get("TNs").get("TotalCount")
                    telephone_numbers_list += response_data.get("TNs").get("TelephoneNumbers").get("TelephoneNumber")

                    if int(total_count) > per_resp_number_count and remaining_count is None:
                        logging.info(f"Setting RemainingCount as {total_count} for first time ")
                        remaining_count = int(total_count) - per_resp_number_count
                        next_page = response_data.get("TNs").get("Links").get("next")
                        logging.info(f"Next Page is: {next_page}")

                        if isinstance(next_page, str):
                            url = re.search(r"<(.*?)>", str(next_page)).group(1)
                            additional_query_params = "?" + url.split("?")[1]
                            logging.info(f"Additional Query Parameters extracted is {additional_query_params}")
                        else:
                            logging.info("Reached at the end of all numbers")

                    elif remaining_count is not None and remaining_count > 0:
                        remaining_count = remaining_count - per_resp_number_count
                        logging.info(f"New RemainingCount ...... {remaining_count}")
                        next_page = response_data.get("TNs").get("Links").get("next")
                        logging.info(f"Next Page is: {next_page}")

                        if isinstance(next_page, str):
                            url = re.search(r"<(.*?)>", str(next_page)).group(1)
                            additional_query_params = "?" + url.split("?")[1]
                            logging.info(f"Additional Query Parameters extracted is {additional_query_params}")
                        else:
                            logging.info("Reached at the end of all numbers")
                    else:
                        remaining_count = 0
                else:
                    logging.info(f"For additional_query_params: {additional_query_params} Error Response from bandwidth: {response.__dict__}")
                    logging.info(f"Breaking our from the loop. Total {page_count} requests made")
                    break

            except Exception as e:
                logging.error(f"Issue occurred in listing all active numbers - error: {e}")
                logging.error(traceback.format_exc())
                raise

            page_count += 1
            if remaining_count <= 0:
                ALL_SUCCESS_FLAG = True
                logging.info("Exiting as nothing extra is remaining")
                break

        logging.info(f"Getting the TelephoneNumbersList Length: {len(telephone_numbers_list)}")
        logging.info(f"The total count received {total_count}")
        logging.info(f"Do The TotalCount and the length of TelephoneNumbersList match?: {'Yes' if len(telephone_numbers_list) == total_count else 'No'}")

        return telephone_numbers_list if ALL_SUCCESS_FLAG else False

    # Updated To Bandwidth-SDK 20.2.1
    def get_siteinfo_for_number(self, phone_number, country_code="US"):
        """
        Fetches the site_id and site name that is attached to the phone

        Args:
            phone_number:    Send a phone number in +12123456789

        Returns:
            Returns a dictionary: {'Id': <id>, 'Name': <name>}

        API Documentation URL:  https://dev.bandwidth.com/docs/numbers/guides/manage-inventory/searchingNumbers/

        API response format has changed, new response format received in XML
        After converting it to dict
        {'Site': {'Id': '21391', 'Name': 'Test Environments'}}
        """

        response = None
        response_data = None

        if not validate_phone_number(phone_number, False):
            raise ValueError(f"Invalid phone number ({phone_number}) passed")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/sites"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/sites"

        try:
            logging.info(f"Making Request to bandwidth to get {phone_number} detail information ")
            response = requests.get(endpoint, headers=self._get_auth_header())
            if response.status_code == 200:
                logging.info(f"Response Status Code received from bandwidth to get {phone_number} site information is {response.status_code}")
                response_data = xmltodict.parse(response.text)
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth to get {phone_number} site information - error: {e}")
            logging.info(f"Response received from bandwidth to get {phone_number} site information is {response.__dict__}")
            logging.error(traceback.format_exc())
            raise

        return response_data["Site"]

    # Updated To Bandwidth-SDK 20.2.1
    def buy_toll_free_number(self, quantity=1, pattern=None, site_id=None, user_id=None, country_code="US"):
        """
        Procures a toll free number. From Bandwidth.

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/Orders/operation/createNewPhoneNumberOrder

        Args:
            quantity:   Send a phone number in +12123456789
            pattern:    Pattern of the toll-free number
            site_id:    Bandwidth site id
            user_id:    User Id of the SH User for whoom we are purchasing the number (Currently not used to call bandwidth)

        Returns:
            Returns a dictionary: {'Id': <id>, 'Name': <name>}


        Request Payload
        {
            "customerOrderId" : user_id (Optional) Less than 40 characters,
            "orderType":
            {
                "phoneNumbers" : result,
                "type": "existingPhoneNumberOrderType",
            },
            "subAccountId": site_id,
        }

        API response format has changed, new response format received in XML
        After converting it to dict
        {
        'OrderResponse':
            {
                'Order':
                {
                    'OrderCreateDate': '2025-08-12T11:31:40.031Z',
                    'AutoActivate': 'true',
                    'BackOrderRequested': 'false',
                    'id': 'c7fbc2a7-16d7-4d3d-a842-d5402692f65d',
                        'ExistingTelephoneNumberOrderType':
                        {
                            'TelephoneNumberList':
                            {
                                'TelephoneNumber': '8336984714'
                            }
                        },
                        'PartialAllowed': 'true',
                        'SiteId': '21391'
                },
                'OrderStatus': 'RECEIVED'
            }
        }
        """

        response = None
        response_data = None

        if quantity < 1:
            raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")

        toll_free_numbers = []
        pattern = pattern if pattern in ("8**", "80*", "87*") else "8**"
        result = self.search_available_toll_free_number(pattern=pattern, quantity=1)
        logging.info(f"Result from search_available_toll_free_number() : {result}")

        if not result:
            logging.info(f"Toll free number for the particular pattern {result} is not found")
            return

        if not isinstance(result, list):
            result = [result]
            toll_free_numbers = result

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")

        site_id = self._resolve_site_id(country_code, site_id)
        if country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders"

        headers = {**self._get_auth_header(), "Content-Type": self.JSON_CONTENT_TYPE}
        request_payload = {
            "customerOrderId": user_id.id,
            "orderType": {
                "phoneNumbers": result,
                "type": "existingPhoneNumberOrderType",
            },
            "subAccountId": site_id,
        }

        json_payload = json.dumps(request_payload)
        logging.info(f"To buy new toll free number request payload: {json_payload}")

        try:
            logging.info(f"Making Request to bandwidth to purchase toll-free phone number {result}")
            response = requests.post(endpoint, headers=headers, data=json_payload)

            logging.info(f"Response Status Code received from bandwidth to purchase toll-free phone number {result} is {response.status_code}")
            if response.status_code == 201:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from bandwidth for purchasing toll-free number(s) {toll_free_numbers} : {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.info(f"Response Status Code received from bandwidth to purchase toll-free phone number {result} - error: {e}")
            logging.info(f"Response received from bandwidth to purchase phone number {result} is {response.__dict__}")
            logging.info(traceback.format_exc())

        return self._cleanup_and_return_numbers(toll_free_numbers, quantity)

    # Updated To Bandwidth-SDK 20.2.1
    def buy_phone_number(self, phone_number=None, area_code=None, user_id=None, site_id=None, country_code="US"):
        """
        We are going to buy a 'phone_number' from bandwidth

        Args:
            phone_number:   If the method is called with a specific number then we are going to attempt to get that number.
                            If purchasing that number fails we inform the user that purchase of the number failed.
            area_code:      The area code of the phone number requested
            user_id:        The SHUser ID which is passed while purchasing a number
            site_id:        The BW_SITE_ID for our account with bandwidth
            country_code:   The country code of the number we are trying to purchase
        Returns:
            Phone(s) number bought.
            None if invalid parameters or Exception if there is one.

            Note: 1 number per area code can be purchased



        If the method is called with area_code then we try to get the area code's available phone number.
        We are dropping support for choosing 10 digit number and purchasing that as new SDK no longer support that feature.

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/Orders/operation/createNewPhoneNumberOrder

        countryCodeA3:  (USA, CAN, AUS) for United States Of America, Canada, Australia

        ### Request Payload (Purchase unknown quantity of numbers)
        {
            "customerOrderId" : user_id.id  (Optional) Less than 40 characters,
            "orderType":
            {
                "areaCode": area_code,
                "countryCodeA3": countryCodeA3,
                "quantity": order_quantity,
                "type": "combinedSearchAndOrderType",
            },
            "subAccountId": site_id,
        }

        API response format has changed, new response format received in XML
        After converting it to dict
        {
            'OrderResponse':
            {
                'Order':
                {
                    'CustomerOrderId': '123',
                    'OrderCreateDate': '2025-08-13T13:49:00.137Z',
                    'AutoActivate': 'true',
                    'BackOrderRequested': 'false',
                    'id': '635492a5-4daf-42a8-a19b-d99dcb5c6c44',
                    'CombinedSearchAndOrderType':
                    {
                        'AreaCode': '929',
                        'EnableLCA': 'false',
                        'Quantity': '1',
                        'CountryCodeA3': 'USA'
                    },
                    'PartialAllowed': 'true',
                    'SiteId': '21391'
                },
                'OrderStatus': 'RECEIVED'
            }
        }

        ### Request Payload (Purchase a Particular Number)
            {
                "autoActivate": true,
                "customerOrderId" : user_id[:40]  (Optional) Less than 40 characters,
                "orderType":
                {
                    "phoneNumbers":
                    [
                        "+19292290159"
                    ],
                    "type": "existingPhoneNumberOrderType",
                },
                "subAccountId": site_id,
            }

            API response format has changed, new response format received in XML
            After converting it to dict
            {
                'OrderResponse':
                {
                    'Order':
                    {
                        'CustomerOrderId': '123',
                        'OrderCreateDate': '2025-10-15T10:34:35.269Z',
                        'AutoActivate': 'true',
                        'BackOrderRequested': 'false',
                        'id': 'a65df40f-c896-4ba2-8f1b-91b51f90f23b',
                        'ExistingTelephoneNumberOrderType':
                        {
                            'TelephoneNumberList':
                            {
                                'TelephoneNumber': '9292290159'
                            }
                        },
                        'PartialAllowed': 'true',
                        'SiteId': '21391'
                        },
                        'OrderStatus': 'RECEIVED'
                }
            }
        """

        logging.info(f"user_id: {user_id}")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")

        order_quantity = 1
        country_code_a3 = ""
        response = None
        response_data = None

        site_id = self._resolve_site_id(country_code, site_id)

        if country_code == "US":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
            country_code_a3 = "USA"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders"
            country_code_a3 = "AUS"
        elif country_code == "CA":
            country_code_a3 = "CAN"

        headers = {**self._get_auth_header(), "Content-Type": self.JSON_CONTENT_TYPE}

        if phone_number is None:
            request_payload = {
                "customerOrderId": str(user_id)[:40],
                "orderType": {
                    "areaCode": area_code,
                    "countryCodeA3": country_code_a3,
                    "quantity": order_quantity,
                    "type": "combinedSearchAndOrderType",
                },
                "subAccountId": site_id,
            }
        else:
            request_payload = {
                "autoActivate": True,
                "customerOrderId": str(user_id)[:40],
                "orderType": {
                    "phoneNumbers": [phone_number],
                    "type": "existingPhoneNumberOrderType",
                },
                "subAccountId": site_id,
            }

        json_payload = json.dumps(request_payload)
        logging.info(f"To buy new number request payload: {json_payload}")

        try:
            if phone_number is None:
                logging.info(f"Making Request to bandwidth to purchase {order_quantity} phone number(s) in country {country_code_a3} with {area_code}")
                response = requests.post(endpoint, headers=headers, data=json_payload)
                logging.info(f"Response received from bandwidth to purchase {order_quantity} phone number(s) in country {country_code_a3} with {area_code} is {response.status_code}")
            else:
                logging.info(f"Making Request to bandwidth to purchase phone number(s) {phone_number} in country {country_code_a3} with {area_code}")
                response = requests.post(endpoint, headers=headers, data=json_payload)
                logging.info(f"Response received from bandwidth to purchase phone number(s) {order_quantity} in country {country_code_a3} with {area_code} is {response.status_code}")

            if response.status_code == 201:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from bandwidth for purchasing phone numbers: {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.info(f"Response Status Code received from bandwidth to purchase {order_quantity} phone number(s) in country {country_code_a3} with {area_code} - error: {e}")
            logging.info(f"Response received from bandwidth to purchase {order_quantity} phone number(s) in country {country_code_a3} with {area_code} is {response.__dict__}")
            logging.info(traceback.format_exc())

        logging.info("Waiting for 10 seconds before fetching order details")
        time.sleep(10)  # Wait for 10 seconds

        try:
            if response_data is not None and response_data.get("OrderResponse").get("OrderStatus") == "RECEIVED":
                successful_order_id = response_data.get("OrderResponse").get("Order").get("id")
                numbers = self.fetch_placed_purchased_order_details(country_code=country_code, order_id=successful_order_id)

                if isinstance(numbers, str):
                    numbers = [numbers]
                cleaned_numbers = list(map(cleanup_phone_number, numbers))
                logging.info(f"Completed fetching order details: {cleaned_numbers}")

                if not validate_phone_number(cleaned_numbers[0]):
                    raise BWNumberUnavailableError(
                        f"Bandwidth returned an invalid/unavailable phone number {cleaned_numbers[0]!r} for order {successful_order_id}"
                    )

                return self._cleanup_and_return_numbers(cleaned_numbers, quantity=1), successful_order_id

            else:
                return False

        except Exception as e:
            logging.info(f"Issue occurred in getting the phone number from response_data. Error: {e}")
            raise

    # Updated To Bandwidth-SDK 20.2.1
    def fetch_placed_purchased_order_details(self, country_code="US", order_id=None):
        """
        Fetching the order details which has been placed with Bandwidth.

        Args:
            country_code:   The country code of the number we are trying to purchase
            orderId:        The orderId received from Bandwidth after placing an order for phone number.

        Returns:
            id: Upon successfully creating the message with bandwith API the id received in the response is returned

        Note: 1 number per area code can be purchased

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/#tag/Orders/operation/retrieveNewPhoneNumberOrder

        API response format has changed, new response format received in XML
        After converting it to dict
        #### For COMPLETE status
        {
            'OrderResponse':
            {
                'CompletedQuantity': '1',
                'CreatedByUser': 'sendhub-dev',
                'LastModifiedDate': '2025-08-13T07:36:10.266Z',
                'OrderCompleteDate': '2025-08-13T07:36:10.265Z',
                'Order':
                {
                    'CustomerOrderId': '123',
                    'OrderCreateDate': '2025-08-13T07:36:08.938Z',
                    'PeerId': '568351',
                    'AutoActivate': 'true',
                    'BackOrderRequested': 'false',
                    'CombinedSearchAndOrderType':
                    {
                        'AreaCode': '919',
                        'EnableLCA': 'false',
                        'Quantity': '1', 'CountryCodeA3': 'USA'
                    },
                    'PartialAllowed': 'true',
                    'SiteId': '21391'
                },
                'OrderStatus': 'COMPLETE',
                'CompletedNumbers':
                {
                    'TelephoneNumber':
                    {
                        'City': 'BENSON',
                        'CountryCodeA3': 'USA',
                        'LATA': '949',
                        'RateCenter': 'BENSON',
                        'State': 'NC',
                        'FullNumber': '+19197567242',
                        'Tier': '0',
                        'VendorId': '49',
                        'VendorName': 'Bandwidth CLEC'
                    }
                },
                'Summary': '1 number ordered in (919)',
                'FailedQuantity': '0'
            }
        }
        #### For RECEIVED status
        {
            'OrderResponse':
            {
                'CreatedByUser': 'sendhub-dev',
                'ErrorList':
                {
                    'Error':
                    {
                        'Code': '5019',
                        'Description': 'Order is pending. Please check the status of your order later.'
                    }
                },
                'LastModifiedDate': '2025-08-13T13:49:00.159Z',
                'Order':
                {
                    'CustomerOrderId': '123',
                    'OrderCreateDate': '2025-08-13T13:49:00.137Z',
                    'AutoActivate': 'true',
                    'BackOrderRequested': 'false',
                    'CombinedSearchAndOrderType':
                    {
                        'AreaCode': '929',
                        'EnableLCA': 'false',
                        'Quantity': '1',
                        'CountryCodeA3': 'USA'
                    },
                    'PartialAllowed': 'true',
                    'SiteId': '21391'
                },
                'OrderStatus': 'RECEIVED',
                'Summary': '1 number requested'
            }
        }
        """

        response = None
        response_data = None

        if order_id is None:
            raise ValueError("Order Id cannot be none to fetch the purchase order details")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders/{order_id}?tndetail=true"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders/{order_id}?tndetail=true"

        try:
            logging.info(f"Making Request to bandwidth to get order details for orderId: {order_id}")
            response = requests.get(endpoint, headers=self._get_auth_header())
            logging.info(f"Response from bandwidth to get order details for orderId: {order_id} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Order Id details fetched are: {response_data}")
            else:
                logging.info(f"In fetching order details {order_id} from bandwidth got Error Response : {response.__dict__}")

        except Exception as e:
            logging.info(f"In fetching order details {order_id} from bandwidth : - error: {e}")
            logging.info(f"In fetching order details {order_id} from bandwidth Response received is {response.__dict__}")
            logging.info(traceback.format_exc())
            raise

        try:
            if response_data is not None and response_data.get("OrderResponse", {}).get("OrderStatus", {}) == "COMPLETE":
                phone_number = response_data.get("OrderResponse", {}).get("CompletedNumbers", {}).get("TelephoneNumber", {}).get("FullNumber", {})
                return phone_number
            if response_data is not None and response_data.get("OrderResponse", {}).get("OrderStatus", {}) == "RECEIVED":
                message = response_data.get("OrderResponse", {}).get("ErrorList", {}).get("Error", {}).get("Description", {})
                return message
            else:
                return None

        except Exception as e:
            logging.info(f"Issue occurred in getting the phone number from response_data. Error: {e}")
            raise

    # Not calling bandwidth API directly
    # Trying to download the images received from a Bandwidth gateway
    # Using token and secret
    def get_media(self, url, out_filename=None, raw_data=False):
        """
        Fetches media file that was part of a MMS.
        Returns out filename or None if unable to

        :set raw_data to True if requires reading data in memory
        """
        if not raw_data:
            if not out_filename:
                out_filename = os.path.join(settings.BW_MMS_DIRECTORY, url.split("/")[-1])

            if not os.path.isdir(os.path.dirname(out_filename)):
                raise ValueError(f"Invalid output directory: {os.path.dirname(out_filename)} - unable to download MMS")

            if os.path.isfile(out_filename):
                logging.info(f"Filename {out_filename}, already exists - will be overwritten.....")

        try:
            if self.use_oauth2:
                resp = requests.get(url, headers=self._get_oauth_bearer_header())
            else:
                resp = requests.get(url, auth=(self.token, self.secret))
        except requests.exceptions.RequestException as e:
            logging.info(f"Error while fetching media: {e}")
            return

        if resp.status_code == requests.codes.ok:
            try:
                if raw_data:
                    return resp.content
                else:
                    with open(out_filename, "wb") as fd:
                        fd.write(resp.content)

                    return out_filename

            except Exception as e:
                logging.info(f"Error: {e} while writing file: {out_filename}")
                return

        logging.info(f"Invalid URI or an error occured, response: {resp.status_code}, response content: {resp.text}")


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
