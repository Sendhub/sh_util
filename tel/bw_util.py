import base64
import json
import logging
import os
import re
import sys
import time
from urllib.parse import urlparse

import phonenumbers
import requests
import xmltodict

# Note: bandwidth import moved to function level to avoid missing package dependency
from .cleanup import cleanup_phone_number
from .release_breaker import is_breaker_open, record_failure, record_success

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


# ISO regions for which this codebase wires Bandwidth number/inventory APIs (NA vs AU accounts).
_BW_API_INVENTORY_REGIONS = frozenset({"US", "CA", "AU"})

# Applied to every raw `requests` call in this module - none previously had a timeout,
# so a non-responding Bandwidth endpoint could hang the calling task/worker indefinitely.
BW_REQUEST_TIMEOUT = 30

# Local mirror of https://dev.bandwidth.com/docs/messaging/errors/, kept in
# tel/data/bandwidth_messaging_error_codes.json. Used to translate the numeric errorCode Bandwidth
# attaches to a failed message (see ListMessageItem.error_code) into a readable
# description for the logs.
MESSAGING_ERROR_CODES_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data",
    "bandwidth_messaging_error_codes.json",
)
_messaging_error_codes_cache = None


def _load_messaging_error_codes():
    """Load and cache the local Bandwidth messaging error code reference."""
    global _messaging_error_codes_cache
    if _messaging_error_codes_cache is None:
        try:
            with open(MESSAGING_ERROR_CODES_PATH) as f:
                _messaging_error_codes_cache = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logging.warning(f"Could not load messaging error code reference ({MESSAGING_ERROR_CODES_PATH}): {e}")
            _messaging_error_codes_cache = {}
    return _messaging_error_codes_cache


def lookup_messaging_error_code(code):
    """
    Look up a Bandwidth messaging error code against the local reference mirrored
    in tel/data/bandwidth_messaging_error_codes.json.

    Args:
        code: Numeric or string error code (e.g. 4405, '4405')

    Returns:
        Dict with 'code', 'category', 'description', 'explanation', 'billable',
        or None if the code is unset or not found in the reference.
    """
    if code is None:
        return None
    code = str(code).strip()
    if not code:
        return None

    for category, entries in _load_messaging_error_codes().items():
        if category.startswith("_") or not isinstance(entries, dict):
            continue
        if code in entries:
            return {**entries[code], "code": code, "category": category}
    return None


def log_messaging_error_code(code, context=""):
    """
    Look up a messaging error code and log the result.

    Args:
        code: Numeric or string Bandwidth messaging error code
        context: Optional prefix identifying what the error relates to (e.g. a message id)

    Returns:
        The matched reference entry, or None if the code was unset or not found.
    """
    prefix = f"{context}: " if context else ""
    info = lookup_messaging_error_code(code)
    if info:
        logging.error(f"{prefix}Bandwidth messaging error {info['code']} ({info['description']}) - {info['explanation']} [billable={info['billable']}]")
    else:
        logging.error(f"{prefix}Bandwidth messaging error {code} - not found in the local error code reference")
    return info


def _configured_supported_countries():
    supported = getattr(settings, "SUPPORTED_COUNTRIES", None) or ()
    return {str(x).strip().upper() for x in supported if x and str(x).strip()}


def _bw_inventory_markets():
    """
    Effective inventory markets: ``settings.SUPPORTED_COUNTRIES`` intersected with
    regions that have Bandwidth API wiring in this integration (``_BW_API_INVENTORY_REGIONS``).

    A service that does not configure ``SUPPORTED_COUNTRIES`` (e.g. admin, billing) is not
    restricted: every Bandwidth-wired region stays enabled, as before.
    """
    configured = _configured_supported_countries()
    if not configured:
        return _BW_API_INVENTORY_REGIONS
    return frozenset(configured & _BW_API_INVENTORY_REGIONS)


def _normalize_country_code(country_code):
    if country_code is None:
        return None
    s = str(country_code).strip().upper()
    return s or None


def resolve_bandwidth_inventory_country(country_code):
    """
    Normalize and validate ``country_code`` for Bandwidth search/buy/release/order APIs.

    Allowed inventory regions are derived as ``SUPPORTED_COUNTRIES ∩ _BW_API_INVENTORY_REGIONS``
    (configured markets limited to US/CA/AU, the only regions wired to Bandwidth here).

    SMS/MMS *recipients* are validated via ``validate_phone_number``, which honors
    ``PHONE_VALIDATION_MODE`` (universal vs supported regions).
    """
    cc = _normalize_country_code(country_code)
    if not cc:
        raise ValueError("country_code is required")
    configured = _configured_supported_countries()
    markets = _bw_inventory_markets()
    if cc not in markets:
        if configured and cc not in configured:
            raise ValueError(f"Phone inventory is not enabled for {cc}; SUPPORTED_COUNTRIES is {sorted(configured)}")
        raise ValueError(f"Phone inventory is not integrated for {cc}; Bandwidth APIs in this codebase support {', '.join(sorted(_BW_API_INVENTORY_REGIONS))} only")
    return cc


def _bw_gateway():
    """Gateway name the release circuit breaker is keyed on."""
    return getattr(settings, "SMS_GATEWAY_BANDWIDTH", "bandwidth")


def get_bandwidth_auth_header():
    """
    Authorization header (Basic or OAuth2 bearer, per ``settings.BW_USE_OAUTH2``) for callers
    that talk to Bandwidth's Account/Numbers APIs directly with ``requests`` instead of going
    through ``SHBandwidthClient`` (e.g. ``main.tendlc.bw_client.BWClient``). Mirrors
    ``SHBandwidthClient._get_auth_header``.
    """
    if getattr(settings, "BW_USE_OAUTH2", False):
        import bandwidth

        configuration = bandwidth.Configuration(client_id=settings.BW_CLIENT_ID, client_secret=settings.BW_CLIENT_SECRET)
        return {"Authorization": f"Bearer {configuration.get_access_token()}"}
    credentials = f"{settings.BW_USERNAME}:{settings.BW_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    return {"Authorization": f"Basic {encoded_credentials}"}


class BandwidthOrderPendingException(Exception):
    """Exception when Toll Free Number is unavailable."""


class BandwidthReleaseCircuitOpenError(Exception):
    """Raised when the release circuit breaker is open for Bandwidth, skipping the API call."""


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
    """
    Bandwidth Messaging + Numbers client.

    Outbound SMS/MMS recipients are validated with ``validate_phone_number`` (respects
    ``PHONE_VALIDATION_MODE`` / ``SUPPORTED_COUNTRIES`` via ``cleanup.validate_phone_number``).

    Number search, purchase, release, and in-service checks additionally require
    ``resolve_bandwidth_inventory_country`` (BW wiring ∩ ``settings.SUPPORTED_COUNTRIES``).
    """

    NUMBER_UNAVAILABLE_MSG = "We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again or choose a different area code."
    CACHE_TTL_SECONDS = 300
    MAX_SAFE_NUMBERS_IN_MEMORY = 100000

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
        # see resolve_bandwidth_inventory_country/_get_auth_header for the switchover.
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

        # phone_number:country_code -> (response_data, cached_at) for get_number_info
        self._number_info_cache = {}

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
            raise ValueError from err

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
            quantity = int(quantity)
            if quantity < 1:
                raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")
            numbers = [number for number in (numbers or []) if number]
            if not numbers:
                return None
            if quantity == 1:
                return self._as_e164(numbers[0], country_code)
            return [self._as_e164(number, country_code) for number in numbers[:quantity]]
        except ValueError as err:
            logging.error(f"Phone number error {err}")
            raise ValueError from err

    def _parse_number_to_bw_format(self, number, country_code="US"):
        """Stripts the prefix '+1' from the 12 char number like '+12123456789'"""
        parsed = phonenumbers.parse(str(number), country_code)
        return str(parsed.national_number)

    def _get_cached_number_info(self, cache_key):
        """Return cached get_number_info() data for cache_key if present and not expired."""
        cached = self._number_info_cache.get(cache_key)
        if cached is None:
            return None
        data, cached_at = cached
        if time.time() - cached_at < self.CACHE_TTL_SECONDS:
            return data
        del self._number_info_cache[cache_key]
        return None

    def _cache_number_info(self, cache_key, data):
        self._number_info_cache[cache_key] = (data, time.time())

    def clear_number_info_cache(self):
        """Clear all cached get_number_info() results. Useful for testing or forced refresh."""
        self._number_info_cache.clear()

    def _parse_link_header(self, link_header):
        """
        Extract the query-string portion (leading '?') from a Bandwidth pagination
        Link-style value, e.g. '<https://.../inserviceNumbers?page=X&size=3>'.
        Returns None if link_header isn't a non-empty string containing a URL.
        """
        if not isinstance(link_header, str) or not link_header:
            return None
        url_match = re.search(r"<([^>]+)>", link_header)
        url = url_match.group(1) if url_match else link_header
        parsed = urlparse(url)
        return f"?{parsed.query}" if parsed.query else None

    def _poll_order_status(self, order_id, country_code="US", timeout=30, poll_interval=2):
        """
        Poll fetch_placed_purchased_order_details() until the order completes, fails, or
        timeout is reached - replaces a hardcoded sleep with intelligent polling.

        Returns:
            The purchased phone number (str) if COMPLETE.
            {'code': ..., 'description': ...} if the order fails with a non-5019 error code.
            None on timeout, or if polling raises.
        """
        start_time = time.time()
        attempts = 0

        while time.time() - start_time < timeout:
            attempts += 1
            elapsed = int(time.time() - start_time)
            logging.info(f"Polling order {order_id} status (attempt {attempts}, elapsed: {elapsed}s)")

            try:
                result = self.fetch_placed_purchased_order_details(country_code=country_code, order_id=order_id)

                # A completed order returns the purchased number as a string; whether that number is
                # acceptable is the caller's call (buy_phone_number validates it), so don't keep polling.
                if isinstance(result, str) and result:
                    logging.info(f"Order {order_id} completed successfully after {elapsed}s")
                    return result

                if isinstance(result, dict):
                    error_code = result.get("code")
                    error_desc = result.get("description")

                    if error_code == "5019":
                        logging.info(f"Order {order_id} still pending (code {error_code}): {error_desc}")
                        time.sleep(poll_interval)
                        continue

                    if error_code:
                        logging.warning(f"Order {order_id} failed with code {error_code}: {error_desc}")
                        return result

                time.sleep(poll_interval)

            except Exception as e:
                logging.exception(f"Error polling order {order_id} status: {e}")
                return None

        logging.warning(f"Order {order_id} polling timeout after {timeout}s")
        return None

    def send_hello(self, from_number, to_number):
        try:
            return self.send_sms(from_number, to_number, "Hello from Sendhub through Bandwidth!")
        except ValueError as err:
            logging.error(f"Error sending message {err}")

    def check_msg_status(self, msg_id):
        return self.get_message_info(msg_id)

    def check_recipient_list_validity(self, numbers) -> list:
        """
        This method's purpose is to check if the to_numbers are in valid E.164 format.
        In case they are not then it will attempt it to convert the number to valid E.164 format.
        If the formatting to E.164 fails then at number will not make it to the final to_list.
        """
        result = []
        if isinstance(numbers, list):
            for num in numbers:
                if self.check_if_valid_e164_format(num) and validate_phone_number(num, False):
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
            from_number:    Either an alphanumeric sender ID or the sender's Bandwidth phone number in E.164 format,
                            which must be hosted within Bandwidth and linked to the account that is generating the message.
                            Alphanumeric Sender IDs can contain up to 11 characters, upper-case letters A-Z,
                            lower-case letters a-z, numbers 0-9, space, hyphen -, plus +, underscore _ and ampersand &.
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
                    logging.exception(f"Exception when calling MessagesApi -> create_message: {e}")

        except Exception as e:
            logging.exception(f"Exception occurred while sending sms via bandwidth: {e}")
            raise type(e) from e

    # Updated To Bandwidth-SDK 20.2.1
    def send_mms(self, from_number, to_number, msg, media, tag=None):
        """
        Sends MMS via Bandwidth-SDK 20.0.0 API call.

        Args:
            from_number:    Either an alphanumeric sender ID or the sender's Bandwidth phone number in E.164 format,
                            which must be hosted within Bandwidth and linked to the account that is generating the message.
                            Alphanumeric Sender IDs can contain up to 11 characters, upper-case letters A-Z,
                            lower-case letters a-z, numbers 0-9, space, hyphen -, plus +, underscore _ and ampersand &.
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
                    logging.exception(f"Exception when calling MessagesApi -> create_message for MMS: {e}")

        except Exception as e:
            logging.exception(f"Exception occurred while sending sms via bandwidth: {e}")
            raise type(e) from e

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
                ListMessageItem(message_id='1754885848276h2fvl5qpvsfasimh', account_id='5004525',
                source_tn='+18332420240', destination_tn='+14109894472',
                message_status=<MessageStatusEnum.UNDELIVERED: 'UNDELIVERED'>,
                message_direction=<ListMessageDirectionEnum.OUTBOUND: 'OUTBOUND'>,
                message_type=<MessageTypeEnum.SMS: 'sms'>, segment_count=1, error_code=4795,
                receive_time=datetime.datetime(2025, 8, 11, 4, 17, 28, 583000, tzinfo=TzInfo(UTC)),
                carrier_name='Other', message_size=None, message_length=13, attachment_count=None,
                recipient_count=None, campaign_class='Unregistered', campaign_id='AGGREGATOR', additional_properties={})
            ],
            additional_properties={}
        )
        """

        cc = resolve_bandwidth_inventory_country(country_code)
        if cc in ("US", "CA"):
            account_id = self.user_id_na
        else:
            account_id = self.user_id_au

        # Import moved here to avoid missing package dependency
        import bandwidth

        with bandwidth.ApiClient(self.configuration) as api_client:
            api_instance = bandwidth.MessagesApi(api_client)
            try:
                logging.info("Calling the MessagesApi -> list_messages")
                api_response = api_instance.list_messages(account_id=account_id, message_id=msgid)
                logging.info(f"The response of MessagesApi -> list_messages: {api_response}")

                messages = getattr(api_response, "messages", None)
                if isinstance(messages, list):
                    for message in messages:
                        error_code = getattr(message, "error_code", None)
                        if error_code is not None:
                            log_messaging_error_code(error_code, context=f"Message {getattr(message, 'message_id', msgid)}")

                return api_response
            except Exception as e:
                logging.exception(f"Exception when calling MessagesApi -> list_messages: {e}")
                raise type(e) from e

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

        cc = resolve_bandwidth_inventory_country(country_code)

        nat_number = phonenumber_as_e164(number)
        nat_number = self._parse_number_to_bw_format(str(nat_number), cc)
        retval = False

        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/inserviceNumbers/{str(nat_number)}"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/inserviceNumbers/{str(nat_number)}"

        try:
            logging.info(f"Making request to Bandwidth to check if number {nat_number} is in service or not")
            response = requests.get(endpoint, headers=self._get_auth_header(), timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response received from bandwidth to get InService for Phone Number {nat_number}  is {response.status_code}")

            if response.status_code == 200:
                retval = True
            elif response.status_code == 404:
                retval = False
            else:
                logging.info(f"Error response received from bandwidth to get InService for Phone Number {nat_number}  is {getattr(response, '__dict__', None)}")

        except Exception as e:
            logging.exception(f"Fetchng InService for Phone Number {number} - error: {e}")
            logging.info(f"Response received from bandwidth to get InService for Phone Number {nat_number}  is {getattr(response, '__dict__', None)}")

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

        try:
            quantity = int(quantity)
        except (TypeError, ValueError) as err:
            raise ValueError(f"Quantity must be an integer - passed: {quantity}") from err

        if quantity < 1:
            raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")

        cc = resolve_bandwidth_inventory_country(country_code)
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/accounts/{str(self.user_id_na)}/availableNumbers?areaCode={area_code}&quantity={quantity}"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/accounts/{str(self.user_id_au)}/availableNumbers?areaCode={area_code}&quantity={quantity}"

        try:
            logging.info(f"Making Request to bandwidth to get {quantity} number for Area Code {area_code}")
            response = requests.get(endpoint, headers=self._get_auth_header(), timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response Status Code received from bandwidth to get {quantity} number for Area Code {area_code} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text) or {}
                """
                    Format of response_data
                    {'SearchResult': {'ResultCount': '1', 'TelephoneNumberList': {'TelephoneNumber': '9192052618'}}}
                    {'SearchResult': {'ResultCount': '2', 'TelephoneNumberList': {'TelephoneNumber': ['9192052618', '9192053260']}}}
                """
                search_result = response_data.get("SearchResult") or {}
                telephone_list = search_result.get("TelephoneNumberList") or {}
                numbers = telephone_list.get("TelephoneNumber")
                logging.info(f"Calling cleanup_phone_number() on the received phone numbers(s) {numbers} from bandwidth")

                if numbers is None:
                    numbers = []
                elif isinstance(numbers, str):
                    numbers = [numbers]
                elif not isinstance(numbers, list):
                    numbers = [numbers]

                cleaned_numbers = [number for number in (cleanup_phone_number(n, cc) for n in numbers) if number]
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")
                raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG)

        except AreaCodeUnavailableError:
            raise
        except Exception as e:
            logging.exception(f"Failed to search for phone number(s) in given area code - error: {e}")
            raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG) from e

        if not cleaned_numbers:
            logging.info("No phone numbers available for area code %s", area_code)
            return None

        return self._cleanup_and_return_numbers(cleaned_numbers, quantity)

    # Updated To Bandwidth-SDK 20.2.1
    def search_available_toll_free_number(self, pattern=None, quantity=1, country_code="US"):
        """
        Search toll free number.
        Find a number within an area code.

        Args:
            pattern:    Exact 3-digit toll-free NPA (e.g. ``888``, ``833``) or a Bandwidth
                        wildcard such as ``8**``, ``80*``, ``87*``.
            quantity:   Has to be more than or equal to 1
        Returns:
            A list of numbers

        API Documentation URL:  https://dev.bandwidth.com/apis/numbers-apis/numbers/v1/#tag/Available-Tns/operation/GetAvailableTns
        """
        cleaned_numbers = []
        response = None
        response_data = None

        try:
            quantity = int(quantity)
        except (TypeError, ValueError) as err:
            raise ValueError(f"Quantity must be an integer - passed: {quantity}") from err

        if quantity < 1:
            raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")

        requested_pattern = pattern
        pattern = self._normalize_toll_free_wildcard_pattern(pattern)

        cc = resolve_bandwidth_inventory_country(country_code)
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v1/accounts/{str(self.user_id_na)}/availableNumbers?tollFreeWildCardPattern={pattern}&quantity={quantity}"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v1/accounts/{str(self.user_id_au)}/availableNumbers?tollFreeWildCardPattern={pattern}&quantity={quantity}"

        try:
            logging.info(f"Making Request to bandwidth to get {quantity} Toll Free Number with pattern {pattern}")
            response = requests.get(endpoint, headers=self._get_auth_header(), timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response received from bandwidth to get {quantity} Toll Free Number with pattern {pattern} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text) or {}
                """
                    Format of response_data
                    {'SearchResult': {'ResultCount': '1', 'TelephoneNumberList': {'TelephoneNumber': '9192052618'}}}
                    {'SearchResult': {'ResultCount': '2', 'TelephoneNumberList': {'TelephoneNumber': ['9192052618', '9192053260']}}}
                """
                if response_data.get("SearchResult"):
                    telephone_list = (response_data.get("SearchResult") or {}).get("TelephoneNumberList") or {}
                    numbers = telephone_list.get("TelephoneNumber")
                    logging.info(f"Calling cleanup_phone_number() on the received toll free phone numbers(s) {numbers} from bandwidth")

                    if numbers is None:
                        numbers = []
                    elif isinstance(numbers, str):
                        numbers = [numbers]
                    elif not isinstance(numbers, list):
                        numbers = [numbers]

                    cleaned_numbers = [number for number in (cleanup_phone_number(n, cc) for n in numbers) if number]
                    cleaned_numbers = [number for number in cleaned_numbers if self._toll_free_number_matches_pattern(number, pattern)]
                elif response.status_code == 200 and response_data.get("SearchResult") is None:
                    logging.info(f"No toll free phonenumbers are available for pattern ({pattern})")
                    return None
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")

        except Exception as e:
            logging.exception(f"Failed to search for {quantity} toll free phone number(s) with pattern {pattern} - error: {e}")
            raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG) from e

        if not cleaned_numbers:
            logging.info(
                "No toll free numbers matched requested pattern %r (normalized=%r)",
                requested_pattern,
                pattern,
            )
            return None

        return self._cleanup_and_return_numbers(cleaned_numbers, quantity)

    @staticmethod
    def _normalize_toll_free_wildcard_pattern(pattern):
        """Map an admin NPA / wildcard to Bandwidth ``tollFreeWildCardPattern``.

        Exact NPAs like ``888`` must be passed through. The previous allow-list only
        accepted ``8**`` / ``80*`` / ``87*``, so ``888`` silently became ``8**`` and
        returned unrelated NPAs such as ``833``.
        """
        if pattern is None or str(pattern).strip() == "":
            return "8**"

        normalized = str(pattern).strip()
        wildcard_patterns = ("8**", "80*", "83*", "84*", "85*", "86*", "87*", "88*")
        if normalized in wildcard_patterns:
            return normalized

        toll_free_npas = list(getattr(settings, "TOLL_FREE_AREA_CODES", []) or [])
        # 800 is a valid US toll-free NPA even when absent from app settings.
        if "800" not in toll_free_npas:
            toll_free_npas.append("800")

        if normalized in toll_free_npas or re.fullmatch(r"8\d{2}", normalized):
            return normalized

        logging.warning(
            "Unsupported toll-free search pattern %r; falling back to wildcard 8**",
            normalized,
        )
        return "8**"

    @staticmethod
    def _toll_free_number_matches_pattern(number, pattern):
        """Return True when ``number``'s NPA matches an exact or wildcard TF pattern."""
        if not number or not pattern:
            return False

        digits = re.sub(r"\D", "", str(number))
        if len(digits) >= 11 and digits.startswith("1"):
            npa = digits[1:4]
        elif len(digits) >= 10:
            npa = digits[0:3]
        else:
            return False

        if "*" not in str(pattern):
            return npa == str(pattern)

        # Convert Bandwidth-style wildcards (8**, 88*) into a regex anchored on the NPA.
        regex = "^" + re.escape(str(pattern)).replace(r"\*", r"\d") + "$"
        return re.match(regex, npa) is not None

    # Updated To Bandwidth-SDK 20.2.1
    def get_number_info(self, phone_number, country_code="US", use_cache=True):
        """
        Search the details of a phone number.

        Args:
            phone_number:    Send a phone number in +18332420240
            use_cache:       Reuse a result cached for CACHE_TTL_SECONDS (5 min). Default True.
        Returns:
            Json of the response from bandwidth API

        Raises:
            RuntimeError: Bandwidth returned a non-200 response

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

        cache_key = f"{phone_number}:{country_code}"
        if use_cache:
            cached_data = self._get_cached_number_info(cache_key)
            if cached_data is not None:
                logging.info(f"Cache hit for get_number_info({phone_number})")
                return cached_data

        cc = resolve_bandwidth_inventory_country(country_code)
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/tnDetails"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/tnDetails"

        try:
            logging.info(f"Making Request to bandwidth to get {phone_number} detail information ")
            response = requests.get(endpoint, headers=self._get_auth_header(), timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response Status Code received from bandwidth to get {phone_number} detail information is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response received from bandwidth to get {phone_number} detail information is : {response_data}")
                if use_cache:
                    self._cache_number_info(cache_key, response_data)
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")
                raise RuntimeError(f"Failed to get number info for {phone_number}: HTTP {response.status_code}")

        except Exception as e:
            logging.exception(f"Response Status Code received from bandwidth to get {phone_number} detail information - error: {e}")
            raise type(e) from e

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

        if isinstance(number, list | tuple | set | frozenset | dict):
            raise BandwidthBulkReleaseError(
                f"release_phone_number() takes exactly one number, got {type(number).__name__} of {len(number)}. Release numbers one at a time so each is verified against this environment's site."
            )

        if is_breaker_open(_bw_gateway()):
            msg = f"Skipping release of {number} on Bandwidth: circuit breaker open"
            logging.info(msg)
            raise BandwidthReleaseCircuitOpenError(msg)

        response = None
        response_data = None

        number = str(number)
        if not validate_phone_number(number, False):
            raise ValueError(f"Invalid phone number ({number}) passed, unable to release")

        cc = resolve_bandwidth_inventory_country(country_code)

        expected_site_id = self._resolve_site_id(cc, site_id)
        self._assert_number_belongs_to_site(number, expected_site_id, cc)

        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{self.user_id_na}/disconnects"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{self.user_id_au}/disconnects"

        request_payload = {"disconnectOrderType": {"disconnectMode": "NORMAL", "phoneNumbers": [number]}}
        json_payload = json.dumps(request_payload)
        logging.info(f"To release number request payload: {json_payload}")

        try:
            headers = {**self._get_auth_header(), "Content-Type": "application/json"}
            logging.info(f"Making Request to bandwidth to release phone number {number}")
            response = requests.post(endpoint, headers=headers, data=json_payload, timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response Status Code received from bandwidth to release {number} is {response.status_code}")

            if response.status_code in (200, 201):
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from Bandwidth to release number {number} is : {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")
                raise RuntimeError(f"Bandwidth release failed for {number}: HTTP {response.status_code} - {response.text}")

        except Exception as e:
            record_failure(_bw_gateway())
            logging.exception(f"Response Status Code received from bandwidth to release {number} detail information - error: {e}")
            raise

        record_success(_bw_gateway())
        return response_data

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

        cc = resolve_bandwidth_inventory_country(country_code)
        site_id = self._resolve_site_id(cc, site_id)

        user_id = self.user_id_na if cc in ("US", "CA") else self.user_id_au
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}{self._inservice_path(user_id, site_id, '/totals')}"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}{self._inservice_path(user_id, site_id, '/totals')}"

        headers = {**self._get_auth_header(), "Content-Type": "application/json"}

        try:
            logging.info(f"Making Request to bandwidth get phone numbers configured for site_id: {site_id}")
            response = requests.get(endpoint, headers=headers, timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                count = response_data.get("Quantity").get("Count")
                logging.info(f"Total count of active numbers for site_id: {site_id} is: {count} numbers")
            else:
                logging.info(f"To get phone numbers configured for site_id: {site_id} Error Response from bandwidth is: {getattr(response, '__dict__', None)}")
                raise RuntimeError(f"Failed to get active number count for site_id {site_id}: HTTP {response.status_code}")

        except Exception as e:
            logging.exception(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is - error: {e}")
            raise type(e) from e

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

        Raises:
            RuntimeError: Bandwidth returned a non-200 response mid-pagination
        """

        TelephoneNumbersList = []
        TotalCount = 0
        RemainingCount = None
        per_resp_number_count = 500
        response = None
        response_data = None
        endpoint = ""
        additional_query_params = ""
        pageCount = 1

        cc = resolve_bandwidth_inventory_country(country_code)
        site_id = self._resolve_site_id(cc, site_id)

        user_id = self.user_id_na if cc in ("US", "CA") else self.user_id_au
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}{self._inservice_path(user_id, site_id)}"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}{self._inservice_path(user_id, site_id)}"

        while True:
            logging.info(f"list_active_numbers pageCount: {pageCount}")
            try:
                try:
                    headers = {**self._get_auth_header(), "Content-Type": "application/json"}
                    logging.info(f"Making Request to bandwidth get phone numbers configured for site_id: {site_id}")
                    response = requests.get(endpoint + additional_query_params, headers=headers, timeout=BW_REQUEST_TIMEOUT)
                except Exception as e:
                    logging.exception(f"Issue occurred in additional_query_params: {additional_query_params} - error: {e}")
                    logging.info(f"Response received from bandwidth to fetch the additional_query_params: {additional_query_params} detail information is {getattr(response, '__dict__', None)}")
                    raise type(e) from e

                logging.info(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is {response.status_code}")

                if response.status_code == 200:
                    response_data = xmltodict.parse(response.text)
                    links = response_data.get("TNs").get("Links")
                    logging.info(f"Links received in current requests: {links}")
                    TotalCount = response_data.get("TNs").get("TotalCount")
                    page_numbers = response_data.get("TNs").get("TelephoneNumbers").get("TelephoneNumber")
                    if isinstance(page_numbers, str):
                        page_numbers = [page_numbers]
                    TelephoneNumbersList += page_numbers or []

                    if int(TotalCount) > per_resp_number_count and RemainingCount is None:
                        if int(TotalCount) > self.MAX_SAFE_NUMBERS_IN_MEMORY:
                            logging.warning(f"Loading {TotalCount} numbers into memory exceeds safety threshold ({self.MAX_SAFE_NUMBERS_IN_MEMORY}). Consider pagination or streaming.")
                        logging.info(f"Setting RemainingCount as {TotalCount} for first time ")
                        RemainingCount = int(TotalCount) - per_resp_number_count
                        nextPage = (response_data.get("TNs").get("Links") or {}).get("next")
                        logging.info(f"Next Page is: {nextPage}")

                        additional_query_params = self._parse_link_header(nextPage)
                        if additional_query_params:
                            logging.info(f"Additional Query Parameters extracted is {additional_query_params}")
                        else:
                            # No usable next link: there is nothing more to fetch, so stop paging
                            # rather than re-requesting the same page (or concatenating None).
                            additional_query_params = ""
                            RemainingCount = 0
                            logging.info("Reached at the end of all numbers")

                    elif RemainingCount is not None and RemainingCount > 0:
                        RemainingCount = RemainingCount - per_resp_number_count
                        logging.info(f"New RemainingCount ...... {RemainingCount}")
                        nextPage = (response_data.get("TNs").get("Links") or {}).get("next")
                        logging.info(f"Next Page is: {nextPage}")

                        additional_query_params = self._parse_link_header(nextPage)
                        if additional_query_params:
                            logging.info(f"Additional Query Parameters extracted is {additional_query_params}")
                        else:
                            # No usable next link: there is nothing more to fetch, so stop paging
                            # rather than re-requesting the same page (or concatenating None).
                            additional_query_params = ""
                            RemainingCount = 0
                            logging.info("Reached at the end of all numbers")
                    else:
                        RemainingCount = 0
                else:
                    logging.info(f"For additional_query_params: {additional_query_params} Error Response from bandwidth: {getattr(response, '__dict__', None)}")
                    raise RuntimeError(f"Bandwidth API error while listing active numbers: HTTP {response.status_code}")

            except Exception as e:
                logging.exception(f"Issue occurred in listing all active numbers - error: {e}")
                raise type(e) from e

            pageCount += 1
            if RemainingCount is None or RemainingCount <= 0:
                logging.info("Exiting as nothing extra is remaining")
                break

        logging.info(f"Getting the TelephoneNumbersList Length: {len(TelephoneNumbersList)}")
        logging.info(f"The total count received {TotalCount}")
        logging.info(f"Do The TotalCount and the length of TelephoneNumbersList match?: {'Yes' if len(TelephoneNumbersList) == TotalCount else 'No'}")

        return TelephoneNumbersList

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

        cc = resolve_bandwidth_inventory_country(country_code)
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/sites"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/sites"

        try:
            logging.info(f"Making Request to bandwidth to get {phone_number} detail information ")
            response = requests.get(endpoint, headers=self._get_auth_header(), timeout=BW_REQUEST_TIMEOUT)
            if response.status_code == 200:
                logging.info(f"Response Status Code received from bandwidth to get {phone_number} site information is {response.status_code}")
                response_data = xmltodict.parse(response.text)
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")

        except Exception as e:
            logging.exception(f"Response Status Code received from bandwidth to get {phone_number} site information - error: {e}")
            logging.info(f"Response received from bandwidth to get {phone_number} site information is {getattr(response, '__dict__', None)}")
            raise type(e) from e

        if response_data is None:
            raise RuntimeError(f"Failed to get site information for {phone_number}: HTTP {response.status_code if response is not None else 'no response'}")

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

        logging.info(f"In buy_toll_free_number() with user_id received is {getattr(user_id, 'id', user_id)}")

        customer_order_id = None
        if user_id is not None:
            customer_order_id = str(getattr(user_id, "id", user_id))[:40]

        if quantity < 1:
            raise ValueError(f"Quantity can not be < 1 - passed: {quantity}")

        cc = resolve_bandwidth_inventory_country(country_code)

        toll_free_numbers = []
        pattern = pattern if pattern in ("8**", "80*", "87*") else "8**"
        result = self.search_available_toll_free_number(pattern=pattern, quantity=1, country_code=cc)
        logging.info(f"Result from search_available_toll_free_number() : {result}")

        if not result:
            logging.info(f"Toll free number for the particular pattern {result} is not found")
            return

        if not isinstance(result, list):
            result = [result]
        toll_free_numbers = result

        site_id = self._resolve_site_id(cc, site_id)

        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders"

        headers = {**self._get_auth_header(), "Content-Type": "application/json"}
        request_payload = {
            "orderType": {
                "phoneNumbers": result,
                "type": "existingPhoneNumberOrderType",
            },
            "subAccountId": site_id,
        }

        if customer_order_id is not None:
            request_payload["customerOrderId"] = customer_order_id

        json_payload = json.dumps(request_payload)
        logging.info(f"To buy new toll free number request payload: {json_payload}")

        try:
            logging.info(f"Making Request to bandwidth to purchase toll-free phone number {result}")
            response = requests.post(endpoint, headers=headers, data=json_payload, timeout=BW_REQUEST_TIMEOUT)

            logging.info(f"Response Status Code received from bandwidth to purchase toll-free phone number {result} is {response.status_code}")
            if response.status_code == 201:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from bandwidth for purchasing toll-free number(s) {toll_free_numbers} : {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")
                raise BWTollFreeUnavailableError(f"Bandwidth toll-free purchase failed with status {response.status_code} for {toll_free_numbers}")

        except BWTollFreeUnavailableError:
            raise
        except Exception as e:
            logging.info(f"Response Status Code received from bandwidth to purchase toll-free phone number {result} - error: {e}")
            logging.info(logging.exception("Unhandled exception"))
            raise BWTollFreeUnavailableError(f"Bandwidth toll-free purchase failed for {toll_free_numbers}: {e}") from e

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
            country_code:   ISO market for the purchase (must lie in ``SUPPORTED_COUNTRIES`` and in
                            Bandwidth-wired regions US/CA/AU — see ``resolve_bandwidth_inventory_country``).
        Returns:
            ``(phone_number, order_id)`` for the number bought, or False if the order did not
            complete. Exception if there is one.

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

        logging.info(f"In buy_phone_number() with user_id received is {getattr(user_id, 'id', user_id)}")

        customer_order_id = None
        if user_id is not None:
            customer_order_id = str(getattr(user_id, "id", user_id))[:40]

        cc = resolve_bandwidth_inventory_country(country_code)

        order_quantity = 1
        countryCodeA3 = ""
        response = None
        response_data = None

        site_id = self._resolve_site_id(cc, site_id)

        if cc == "US":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
            countryCodeA3 = "USA"
        elif cc == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders"
            countryCodeA3 = "AUS"
        else:
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
            countryCodeA3 = "CAN"

        headers = {**self._get_auth_header(), "Content-Type": "application/json"}

        if phone_number is None:
            request_payload = {
                "orderType": {
                    "areaCode": area_code,
                    "countryCodeA3": countryCodeA3,
                    "quantity": order_quantity,
                    "type": "combinedSearchAndOrderType",
                },
                "subAccountId": site_id,
            }
        else:
            request_payload = {
                "autoActivate": True,
                "orderType": {
                    "phoneNumbers": [phone_number],
                    "type": "existingPhoneNumberOrderType",
                },
                "subAccountId": site_id,
            }

        if customer_order_id is not None:
            request_payload["customerOrderId"] = customer_order_id

        json_payload = json.dumps(request_payload)
        logging.info(f"To buy new number request payload: {json_payload}")

        try:
            if phone_number is None:
                logging.info(f"Making Request to bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code}")
                response = requests.post(endpoint, headers=headers, data=json_payload, timeout=BW_REQUEST_TIMEOUT)
                logging.info(f"Response received from bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code} is {response.status_code}")
            else:
                logging.info(f"Making Request to bandwidth to purchase phone number(s) {phone_number} in country {countryCodeA3} with {area_code}")
                response = requests.post(endpoint, headers=headers, data=json_payload, timeout=BW_REQUEST_TIMEOUT)
                logging.info(f"Response received from bandwidth to purchase phone number(s) {order_quantity} in country {countryCodeA3} with {area_code} is {response.status_code}")

            if response.status_code == 201:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from bandwidth for purchasing phone numbers: {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {getattr(response, '__dict__', None)}")

        except Exception as e:
            logging.info(f"Response Status Code received from bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code} - error: {e}")
            logging.info(f"Response received from bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code} is {getattr(response, '__dict__', None)}")
            logging.info(logging.exception("Unhandled exception"))

        try:
            if response_data is not None and response_data.get("OrderResponse").get("OrderStatus") == "RECEIVED":
                successful_order_id = response_data.get("OrderResponse").get("Order").get("id")
                logging.info(f"Order {successful_order_id} received, polling for completion (max 30s)")
                result = self._poll_order_status(order_id=successful_order_id, country_code=cc, timeout=30, poll_interval=2)

                if result is None or isinstance(result, dict):
                    logging.warning(f"Order {successful_order_id} did not complete: {result}")
                    return False

                numbers = [result]
                cleaned_numbers = list(map(cleanup_phone_number, numbers))
                logging.info(f"Completed fetching order details: {cleaned_numbers}")

                if not cleaned_numbers:
                    return False
                if not validate_phone_number(cleaned_numbers[0], False):
                    raise BWNumberUnavailableError(f"Bandwidth returned an invalid/unavailable phone number {cleaned_numbers[0]!r} for order {successful_order_id}")
                return self._cleanup_and_return_numbers(cleaned_numbers, quantity=1), successful_order_id

            else:
                return False

        except Exception as e:
            logging.info(f"Issue occurred in getting the phone number from response_data. Error: {e}")
            raise type(e) from e

    # Updated To Bandwidth-SDK 20.2.1
    def fetch_placed_purchased_order_details(self, country_code="US", order_id=None):
        """
        Fetching the order details which has been placed with Bandwidth.

        Args:
            country_code:   The country code of the number we are trying to purchase
            order_id:       The order id received from Bandwidth after placing an order for phone number.

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

        cc = resolve_bandwidth_inventory_country(country_code)
        if cc in ("US", "CA"):
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders/{order_id}?tndetail=true"
        else:
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders/{order_id}?tndetail=true"

        try:
            logging.info(f"Making Request to bandwidth to get order details for order_id: {order_id}")
            response = requests.get(endpoint, headers=self._get_auth_header(), timeout=BW_REQUEST_TIMEOUT)
            logging.info(f"Response from bandwidth to get order details for order_id: {order_id} is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Order Id details fetched are: {response_data}")
            else:
                logging.info(f"In fetching order details {order_id} from bandwidth got Error Response : {getattr(response, '__dict__', None)}")

        except Exception as e:
            logging.info(f"In fetching order details {order_id} from bandwidth : - error: {e}")
            logging.info(f"In fetching order details {order_id} from bandwidth Response received is {getattr(response, '__dict__', None)}")
            logging.info(logging.exception("Unhandled exception"))
            raise type(e) from e

        try:
            if response_data is not None and response_data.get("OrderResponse", {}).get("OrderStatus", {}) == "COMPLETE":
                phone_number = response_data.get("OrderResponse", {}).get("CompletedNumbers", {}).get("TelephoneNumber", {}).get("FullNumber", {})
                return phone_number
            if response_data is not None and response_data.get("OrderResponse", {}).get("OrderStatus", {}) == "RECEIVED":
                error = response_data.get("OrderResponse", {}).get("ErrorList", {}).get("Error", {})
                # Surfacing the code (not just the description) lets _poll_order_status tell
                # "still pending" (5019) apart from a real failure.
                return {"code": error.get("Code"), "description": error.get("Description")}
            else:
                return None

        except Exception as e:
            logging.info(f"Issue occurred in getting the phone number from response_data. Error: {e}")
            raise type(e) from e

    # Not calling bandwidth API directly
    # Trying to download the images received from a Bandwidth gateway
    # Uses token/secret Basic auth, or an OAuth2 bearer token when settings.BW_USE_OAUTH2 is set
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
                resp = requests.get(url, headers=self._get_oauth_bearer_header(), timeout=BW_REQUEST_TIMEOUT)
            else:
                resp = requests.get(url, auth=(self.token, self.secret), timeout=BW_REQUEST_TIMEOUT)
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
