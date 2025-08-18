import base64
import json
import logging
import os
import re
import sys
import time
import traceback

import phonenumbers
import requests
import xmltodict
from main.models import ShUser
from sh_util.tel import cleanupPhoneNumber

try:
    import settings
except ImportError:
    # when unit testing is invoked
    sys.path.append("/opt/sendhub/inforeach/app")
    import settings

try:
    from sh_util.tel import AreaCodeUnavailableError, displayNumber, validatePhoneNumber
except ImportError:
    sys.path.append("/opt/sendhub/inforeach/app")
    from sh_util.tel import validatePhoneNumber, AreaCodeUnavailableError
    from sh_util.tel import displayNumber

import bandwidth


class BandwidthOrderPendingException(Exception):
        """Exception when Toll Free Number is unavailable."""


class BWNumberUnavailableError(Exception):
    """Exception when requested BW Number is unavailable."""


class BWTollFreeUnavailableError(Exception):
    """Exception when Toll Free Number is unavailable."""


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
        self.friendly_name = displayNumber(number)
        self.phone_number = number
        self.gateway = settings.SMS_GATEWAY_BANDWIDTH


def phonenumber_as_e164(number, country_code='US'):
    '''
      this function should be called mainly with valid
      phone numbers.
      Exception is raised if number is invalid
    '''
    if not isinstance(number, str):
        number = str(number)
    if validatePhoneNumber(number, False) is False:
        raise ValueError("Invalid phone number {} - unable to process".
                         format(number))
    return phonenumbers.format_number(
        phonenumbers.parse(number, country_code),
        phonenumbers.PhoneNumberFormat.E164
    )


class BandwidthNumberObject:
    """
       returns an object with number and sid
          (sid is not used)
       to be compatible with twilio number object
       to minimize changes
    """
    def __init__(self, number, sid):
        self.phone_number = number
        self.sid = sid


class SHBandwidthClient(object):
    NUMBER_UNAVAILABLE_MSG = \
        'We are currently having problems buying phone numbers from  ' \
        'our carrier. Please wait a moment and try again or choose a ' \
        'different area code.'

    def __init__(self, userid=None, token=None,
                 secret=None, username=None, password=None,
                 debug=False):
        if not userid:
            userid = settings.BW_USER_ID
        if not token:
            token = settings.BW_API_TOKEN
        if not secret:
            secret = settings.BW_API_SECRET
        # username and password are used for account
        # API authentication
        if not username:
            username = settings.BW_USERNAME
        if not password:
            password = settings.BW_PASSWORD

        # saving these as part of object as media get
        # operations are done here.... BW SDK when
        # released, may not support it.
        self.userid = userid
        self.token = token
        self.secret = secret
        self.username = username
        self.password = password

        if not userid or not token or not secret \
           or not username or not password:
            raise ValueError('Appropriate Bandwidth Keys are not available '
                             'supplied userid: {}, token: {}, '
                             'secret: {}, username: {}, password: {}'.
                             format(userid, token, secret,
                                    username, password))

        self.voice_client = bandwidth.client('voice',
                                             userid,
                                             token,
                                             secret,
                                             api_version='v2',
                                             DEBUG=debug)
        self.sms_client = bandwidth.client(
            'messaging',
            userid,
            token,
            secret,
            api_version='v2',
            api_endpoint=settings.BW_MESSAGING_API_URL,
            DEBUG=debug
        )
        self.account_client = bandwidth.client(
            'account',
            userid,
            username,
            password,
            api_version='v2',
            api_endpoint=settings.BW_ACCOUNT_API_URL,
            account_id=settings.BW_ACCOUNT_ID,
            DEBUG=debug
        )

    def _as_e164(self, number, country_code='US'):
        return phonenumber_as_e164(number, country_code)

    def send_sms(self, from_number, to_number, msg, tag=None):
        """
           sends SMS via Bandwidth API call.
           returns message_id
        """
        # allow sending to a group in one call
        if not isinstance(to_number, list):
            to_number = [to_number]

        return self.sms_client.send_message(
            from_=self._as_e164(from_number),
            to=[self._as_e164(number) for number in to_number],
            text=msg,
            tag=tag,
            applicationId=settings.BW_APP_ID
        )

    def send_mms(self, from_number, to_number, msg, media, tag=None):
        """
           sends MMS via Bandwidth API call.
           returns message_id
        """
        # allow sending to a group in one call
        if not isinstance(to_number, list):
            to_number = [to_number]

        return self.sms_client.send_message(
            from_=self._as_e164(from_number),
            to=[self._as_e164(number) for number in to_number],
            text=msg,
            tag=tag,
            media=media,
            applicationId=settings.BW_APP_ID
        )

    def get_message_info(self, msgid=None):
        """
           returns message info.

           with v2 messaging, this method does not work.
        """
        raise NotImplementedError('This method is not supported '
                                  'with v2 messaging')
        if msgid:
            return self.sms_client.get_message(msgid)

    def send_hello(self, from_number, to_number):
        return self.send_sms(from_number, to_number,
                             'Hello from Sendhub through Bandwidth!')

    def check_msg_status(self, msg_id):
        return self.get_message(msg_id)

    def _cleanup_and_return_numbers(self,
                                    numbers,
                                    quantity,
                                    country_code='US'):
        """
          helper function that takes in the numbers list
          returned by BW APIs, formats them and returns:
          number itself if quantity is 1 else
          returns list of numbers.
        """
        if quantity == 1:
            return self._as_e164(numbers[0], country_code)
        elif quantity > 1:
            return [self._as_e164(number, country_code) for number in numbers]
        else:
            raise ValueError('Quantity can not be < 1 - passed: {}'.
                             format(quantity))

    def _parse_number_to_bw_format(self, number, country_code='US'):
        # cleanup the number - remove country code as bandwidth
        # does not except country code?
        # TODO: must be an API from phonenumbers library that allows
        # parsing national number
        return phonenumbers.format_number(
            phonenumbers.parse(str(number), 'US'),
            phonenumbers.PhoneNumberFormat.E164
        )[2:]

    # Updated to bandwidth-sdk 20.0.0
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

        Request Payload
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

        """

        if isinstance(user_id, ShUser):
            logging.info(f"In buy_phone_number() with user_id.id received is {user_id.id}")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")

        result = []
        order_quantity = 1
        countryCodeA3 = ""
        response = None
        response_data = None

        if not site_id:
            if country_code == "US" or country_code == "CA":
                site_id = self.bw_site_id_na
            elif country_code == "AU":
                site_id = self.bw_site_id_au

        if country_code == "US":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
            countryCodeA3 = "USA"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders"
            countryCodeA3 = "AUS"
        elif country_code == "CA":
            countryCodeA3 = "CAN"

        headers = {"Authorization": f"Basic {self._get_encoded_credentials()}", "Content-Type": "application/json"}
        request_payload = {
            "customerOrderId": user_id.id,
            "orderType": {
                "areaCode": area_code,
                "countryCodeA3": countryCodeA3,
                "quantity": order_quantity,
                "type": "combinedSearchAndOrderType",
            },
            "subAccountId": site_id,
        }

        json_payload = json.dumps(request_payload)
        logging.info(f"To buy new number request payload: {json_payload}")

        try:

            logging.info(f"Making Request to bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code}")
            response = requests.post(endpoint, headers=headers, data=json_payload)
            logging.info(
                f"Response received from bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code} is {response.status_code}"
            )

            if response.status_code == 201:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response from bandwidth for purchasing phone numbers: {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.info(
                f"Response Status Code received from bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code} - error: {e}"
            )
            logging.info(
                f"Response received from bandwidth to purchase {order_quantity} phone number(s) in country {countryCodeA3} with {area_code} is {response.__dict__}"
            )
            logging.info(traceback.print_exc())

        logging.info(f"Waiting for 10 seconds before fetching order details")
        time.sleep(10)  # Wait for 10 seconds

        try:
            if response_data is not None and response_data.get("OrderResponse").get("OrderStatus") == "RECEIVED":

                successful_order_id = response_data.get("OrderResponse").get("Order").get("id")
                numbers = self.fetch_placed_purchased_order_details(country_code=country_code, orderId=successful_order_id)

                if isinstance(numbers, str):
                    numbers = [numbers]
                cleaned_numbers = list(map(cleanupPhoneNumber, numbers))
                logging.info(f"Completed fetching order details: {cleaned_numbers}")

                if result is not None:
                    if not validatePhoneNumber(cleaned_numbers[0]):
                        return result
                    else:
                        return self._cleanup_and_return_numbers(cleaned_numbers, quantity=1)
                else:
                    return False

            else:
                return False

        except Exception as e:
            logging.info(f"Issue occurred in getting the phone number from response_data. Error: {e}")
            raise type(e)

    # Updated to bandwidth-sdk 20.0.0
    def release_phone_number(self, number, country_code="US"):
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

        response = None
        response_data = None

        number = str(number)
        if not validatePhoneNumber(number, False):
            raise ValueError(f"Invalid phone number ({number}) passed, unable to release")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{self.user_id_na}/disconnects"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{self.user_id_au}/disconnects"

        headers = {"Authorization": f"Basic {self._get_encoded_credentials()}", "Content-Type": "application/json"}
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
            logging.error(traceback.print_exc())
            raise type(e)

    # Updated to bandwidth-sdk 20.0.0
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
            endpoint = (
                f"{str(self.bw_account_api_url_na)}/api/accounts/{str(self.user_id_na)}/availableNumbers?areaCode={area_code}&quantity={quantity}"
            )
        elif country_code == "AU":
            endpoint = (
                f"{str(self.bw_account_api_url_au)}/api/accounts/{str(self.user_id_au)}/availableNumbers?areaCode={area_code}&quantity={quantity}"
            )

        try:
            logging.info(f"Making Request to bandwidth to get {quantity} number for Area Code {area_code}")
            response = requests.get(endpoint, headers=self._get_common_auth_header())
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
                cleaned_numbers = list(map(cleanupPhoneNumber, numbers))
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Failed to search for phone number(s) in given area code - error: {e}")
            logging.info(f"Response received from bandwidth to get {quantity} number(s) for Area Code {area_code} is {response.__dict__}")
            logging.error(traceback.print_exc())
            raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG)

        return self._cleanup_and_return_numbers(cleaned_numbers, quantity)

    # Updated to bandwidth-sdk 20.0.0
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
            response = requests.get(endpoint, headers=self._get_common_auth_header())
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
                        cleaned_numbers = list(map(cleanupPhoneNumber, numbers))
                elif response.status_code == 200 and response_data.get("SearchResult") is None:
                    logging.info(f"No toll free phonenumbers are available for pattern ({pattern})")
                    return None
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Failed to search for {quantity} toll free phone number(s) with pattern {pattern} - error: {e}")
            logging.info(f"Response received from bandwidth to get {quantity} number(s) for pattern {pattern} is {response.__dict__}")
            logging.error(traceback.print_exc())
            raise AreaCodeUnavailableError(SHBandwidthClient.NUMBER_UNAVAILABLE_MSG)

        return self._cleanup_and_return_numbers(cleaned_numbers, quantity)

    # Updated to bandwidth-sdk 20.0.0
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

        if isinstance(user_id, ShUser):
            logging.info(f"In buy_phone_number() with user_id.id received is {user_id.id}")

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

        if not site_id:
            if country_code == "US" or country_code == "CA":
                site_id = self.bw_site_id_na
            elif country_code == "AU":
                site_id = self.bw_site_id_au

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{str(self.user_id_na)}/orders"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{str(self.user_id_au)}/orders"

        headers = {"Authorization": f"Basic {self._get_encoded_credentials()}", "Content-Type": "application/json"}
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
            logging.info(traceback.print_exc())

        return self._cleanup_and_return_numbers(toll_free_numbers, quantity)

    # Updated to bandwidth-sdk 20.0.0
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

        if not validatePhoneNumber(number, False):
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
            response = requests.get(endpoint, headers=self._get_common_auth_header())
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
            logging.error(traceback.print_exc())

        return retval

    # Updated to bandwidth-sdk 20.0.0
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

        TelephoneNumbersList = []
        TotalCount = 0
        RemainingCount = None
        per_resp_number_count = 500
        response = None
        response_data = None
        endpoint = ""
        additional_query_params = ""
        pageCount = 1
        ALL_SUCCESS_FLAG = False

        if not site_id:
            if country_code == "US" or country_code == "CA":
                site_id = self.bw_site_id_na
            elif country_code == "AU":
                site_id = self.bw_site_id_au

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{self.user_id_na}/inserviceNumbers"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{self.user_id_au}/inserviceNumbers"

        headers = {"Authorization": f"Basic {self._get_encoded_credentials()}", "Content-Type": "application/json"}

        while True:
            logging.info(f"list_active_numbers pageCount: {pageCount}")
            try:
                try:
                    logging.info(f"Making Request to bandwidth get phone numbers configured for site_id: {site_id}")
                    response = requests.get(endpoint + additional_query_params, headers=headers)
                except Exception as e:
                    logging.error(f"Issue occurred in additional_query_params: {additional_query_params} - error: {e}")
                    logging.info(
                        f"Response received from bandwidth to fetch the additional_query_params: {additional_query_params} detail information is {response.__dict__}"
                    )
                    logging.error(traceback.print_exc())
                    raise type(e)

                logging.info(
                    f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is {response.status_code}"
                )

                if response.status_code == 200:

                    response_data = xmltodict.parse(response.text)
                    links = response_data.get("TNs").get("Links")
                    logging.info(f"Links received in current requests: {links}")
                    TotalCount = response_data.get("TNs").get("TotalCount")
                    TelephoneNumbersList += response_data.get("TNs").get("TelephoneNumbers").get("TelephoneNumber")

                    if int(TotalCount) > per_resp_number_count and RemainingCount is None:

                        logging.info(f"Setting RemainingCount as {TotalCount} for first time ")
                        RemainingCount = int(TotalCount) - per_resp_number_count
                        nextPage = response_data.get("TNs").get("Links").get("next")
                        logging.info(f"Next Page is: {nextPage}")

                        if type(nextPage) == str:
                            url = re.search(r"<(.*?)>", str(nextPage)).group(1)
                            additional_query_params = "?" + url.split("?")[1]
                            logging.info(f"Additional Query Parameters extracted is {additional_query_params}")
                        else:
                            logging.info(f"Reached at the end of all numbers")

                    elif RemainingCount > 0:

                        RemainingCount = RemainingCount - per_resp_number_count
                        logging.info(f"New RemainingCount ...... {RemainingCount}")
                        nextPage = response_data.get("TNs").get("Links").get("next")
                        logging.info(f"Next Page is: {nextPage}")

                        if type(nextPage) == str:
                            url = re.search(r"<(.*?)>", str(nextPage)).group(1)
                            additional_query_params = "?" + url.split("?")[1]
                            logging.info(f"Additional Query Parameters extracted is {additional_query_params}")
                        else:
                            logging.info(f"Reached at the end of all numbers")
                    else:
                        RemainingCount = 0
                else:
                    logging.info(f"For additional_query_params: {additional_query_params} Error Response from bandwidth: {response.__dict__}")
                    logging.info(f"Breaking our from the loop. Total {pageCount} requests made")
                    break

            except Exception as e:
                logging.error(f"Issue occurred in listing all active numbers - error: {e}")
                logging.error(traceback.print_exc())
                raise type(e)

            pageCount += 1
            if RemainingCount <= 0:
                ALL_SUCCESS_FLAG = True
                logging.info("Exiting as nothing extra is remaining")
                break

        logging.info(f"Getting the TelephoneNumbersList Length: {len(TelephoneNumbersList)}")
        logging.info(f"The total count received {TotalCount}")
        logging.info(f"Do The TotalCount and the length of TelephoneNumbersList match?: {'Yes' if len(TelephoneNumbersList) == TotalCount else 'No'}")

        return TelephoneNumbersList if ALL_SUCCESS_FLAG else False

    # Updated to bandwidth-sdk 20.0.0
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

        if not site_id:
            if country_code == "US" or country_code == "CA":
                site_id = self.bw_site_id_na
            elif country_code == "AU":
                site_id = self.bw_site_id_au

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/v2/accounts/{self.user_id_na}/inserviceNumbers/totals"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/v2/accounts/{self.user_id_au}/inserviceNumbers/totals"

        headers = {"Authorization": f"Basic {self._get_encoded_credentials()}", "Content-Type": "application/json"}

        try:
            logging.info(f"Making Request to bandwidth get phone numbers configured for site_id: {site_id}")
            response = requests.get(endpoint, headers=headers)
            logging.info(
                f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is {response.status_code}"
            )

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                count = response_data.get("Quantity").get("Count")
                logging.info(f"Total count of active numbers for site_id: {site_id} is: {count} numbers")
            else:
                logging.info(f"To get phone numbers configured for site_id: {site_id} Error Response from bandwidth is: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth for phone numbers configured for site_id: {site_id} is - error: {e}")
            logging.info(f"Response received from bandwidth for site_id: {site_id} is {response.__dict__}")
            logging.error(traceback.print_exc())
            raise type(e)

        return count

    # Updated to bandwidth-sdk 20.0.0
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

        if not validatePhoneNumber(phone_number, False):
            raise ValueError(f"Invalid phone number ({phone_number}) passed")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/sites"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/sites"

        try:

            logging.info(f"Making Request to bandwidth to get {phone_number} detail information ")
            response = requests.get(endpoint, headers=self._get_common_auth_header())
            if response.status_code == 200:
                logging.info(f"Response Status Code received from bandwidth to get {phone_number} site information is {response.status_code}")
                response_data = xmltodict.parse(response.text)
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth to get {phone_number} site information - error: {e}")
            logging.info(f"Response received from bandwidth to get {phone_number} site information is {response.__dict__}")
            logging.error(traceback.print_exc())
            raise type(e)

        return response_data["Site"]

    # Updated to bandwidth-sdk 20.0.0
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

        if not validatePhoneNumber(phone_number, False):
            raise ValueError(f"Invalid phone number ({phone_number}) passed")

        if country_code not in ("US", "CA", "AU"):
            raise ValueError(f"Only numbers in US/CA/AU are supported, requested country: {country_code}")
        elif country_code == "US" or country_code == "CA":
            endpoint = f"{str(self.bw_account_api_url_na)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/tnDetails"
        elif country_code == "AU":
            endpoint = f"{str(self.bw_account_api_url_au)}/api/tns/{self._parse_number_to_bw_format(phone_number)}/tnDetails"

        try:
            logging.info(f"Making Request to bandwidth to get {phone_number} detail information ")
            response = requests.get(endpoint, headers=self._get_common_auth_header())
            logging.info(f"Response Status Code received from bandwidth to get {phone_number} detail information is {response.status_code}")

            if response.status_code == 200:
                response_data = xmltodict.parse(response.text)
                logging.info(f"Response received from bandwidth to get {phone_number} detail information is : {response_data}")
            else:
                logging.info(f"Error Response from bandwidth: {response.__dict__}")

        except Exception as e:
            logging.error(f"Response Status Code received from bandwidth to get {phone_number} detail information - error: {e}")
            logging.info(f"Response received from bandwidth to get {phone_number} detail information is {response.__dict__}")
            logging.error(traceback.print_exc())
            raise type(e)

        return response_data

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



if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
