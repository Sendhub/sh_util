"""
    Twilio Utilities
"""

import logging
import re
import traceback
from decimal import Decimal


def twilio_phone_number_properties(avail):
    """
    Return a dict shaped like the old _properties for AvailablePhoneNumber instances.
    Expect avail to be an AvailablePhoneNumberInstance from client.api.v2010...
    """
    return {
        "friendly_name": getattr(avail, "friendly_name", None),
        "phone_number": getattr(avail, "phone_number", None),
        "lata": getattr(avail, "lata", None),
        "locality": getattr(avail, "locality", None),
        "rate_center": getattr(avail, "rate_center", None),
        "latitude": getattr(avail, "latitude", None),
        "longitude": getattr(avail, "longitude", None),
        "region": getattr(avail, "region", None),
        "postal_code": getattr(avail, "postal_code", None) or getattr(avail, "postal_code", None),
        "iso_country": getattr(avail, "iso_country", None) or getattr(avail, "country", None),
        "address_requirements": getattr(avail, "address_requirements", None),
        "beta": getattr(avail, "beta", False),
        "capabilities": getattr(avail, "capabilities", None),
        "_solution": {
            "account_sid": getattr(avail, "account_sid", None) or getattr(avail, "_solution", {}).get("account_sid") if hasattr(avail, "_solution") else None,
            "country_code": getattr(avail, "country_code", None) or getattr(avail, "_solution", {}).get("country_code") if hasattr(avail, "_solution") else None,
        }
    }


def sanitize(value):
    if isinstance(value, Decimal):
        return round(float(value), 6)
    if isinstance(value, dict):
        return {k: sanitize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize(v) for v in value]
    return value

class AreaCodeUnavailableError(Exception):
    """Exception when requested area code is unavailable."""


def twilioFindNumberInAreaCode(twilioClient, areaCode, countryCode='US', max_limit=6, only_list=False):
    """
        Find a number within an area code.

        Args:
            twilioClient        : The twilio Client
            areaCode (int)      : A 3 digit area code of USA, Canada
            countryCode (str)   : A string of 2 digit country code
        Returns:
            An list(str) of the phonenumbers
    """
    try:
        logging.info(f"Before searching for area-code: {areaCode}")
        result = twilioClient.api.v2010.accounts(twilioClient.username).available_phone_numbers(countryCode).local.list(area_code=areaCode, limit=max_limit)
        logging.info(f"After searching for area-code: {areaCode}")
        logging.info(f"Result: {result}")
        return [inst.phone_number for inst in result] if only_list else result

    except Exception as e:
        logging.error(f"Exception occurred while trying to list number for area-code: {areaCode} Error was: {e}")
        logging.error(traceback.print_exc())
        raise AreaCodeUnavailableError('We are currently having problems finding phone numbers from our carrier. Please wait a moment and try again.') from e


def twilioFindTollFreeNumberInAreaCode(twilioClient, pattern, countryCode='US', max_limit=6):
    """
        Find a toll-free number.

        Args:
            twilioClient        : The twilio Client
            areaCode (int)      : A 3 digit area code of USA, Canada
            countryCode (str)   : A string of 2 digit country code
        Returns:
            An list(str) of the phonenumbers
    """

    pattern = pattern if re.match(r"^8[0,3-7]", pattern) else None

    try:
        logging.info(f"Before searching for toll_free")
        result = twilioClient.api.v2010.accounts(twilioClient.username).available_phone_numbers(countryCode).toll_free.list(contains=pattern, limit=max_limit)
        logging.info(f"After searching for toll_free")
        logging.info(f" Result: {result}")
        # return [inst.phone_number for inst in result]
        return result
    except Exception as e:
        logging.error(f"Exception occurred while trying to list number for toll-free number Error was: {e}")
        logging.error(traceback.print_exc())
        raise AreaCodeUnavailableError('We are currently having problems finding phone numbers from our carrier. Please wait a moment and try again.') from e


def twilioBuyPhoneNumber(twilioClient, appSid, areaCode=None, countryCode='US', phoneNumber=None):
    """
        Buy a phone number from twilio.

        Args:
            twilioClient        : The twilio Client
            appSid (sid)        : The sid of the application in (staging, production)
            areaCode (int)      : A 3 digit area code of USA, Canada
            countryCode (str)   : A string of 2 digit country code
            phoneNumber (str)   : A phonenumber if any available

        Returns:
            phonenumber (str)   : The purchased phone number
    """

    if areaCode:
        number = twilioFindNumberInAreaCode(twilioClient, areaCode, countryCode=countryCode, max_limit=1)
    else:
        number = None

    if number:
        try:
            logging.info(f"Trying to purchase Twilio number for area-code: {areaCode} number: {number}")
            newNumber = twilioClient.incoming_phone_numbers.create(phone_number=number, sms_application_sid=appSid, voice_application_sid=appSid)
            logging.info(f"SID of purchase: {newNumber.sid} Number: {newNumber.phone_number}")
            return newNumber
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase number for area-code: {areaCode} Error was: {e}")
            logging.error(traceback.print_exc())
            raise AreaCodeUnavailableError('We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again.') from e

    elif phoneNumber is not None:
        try:
            logging.info(f"Trying to purchase Twilio number: {phoneNumber}")
            newNumber = twilioClient.incoming_phone_numbers.create(phone_number=phoneNumber, sms_application_sid=appSid, voice_application_sid=appSid)
            logging.info(f"SID of purchase: {newNumber.sid} Number: {newNumber.phone_number}")
            return newNumber
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase number: {phoneNumber} Error was: {e}")
            logging.error(traceback.print_exc())
            raise AreaCodeUnavailableError('We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again.') from e

    else:
        raise AreaCodeUnavailableError('No available numbers left in that area code')


def twilioBuyTollFreePhoneNumber(twilioClient, appSid, pattern=None, countryCode='US', phoneNumber=None):
    """
        Buy a toll free phone number from twilio.

        Args:
            twilioClient        : The twilio Client
            appSid (sid)        : The sid of the application in (staging, production)
            areaCode (int)      : A 3 digit area code of USA, Canada
            countryCode (str)   : A string of 2 digit country code
            phoneNumber (str)   : A phonenumber if any available

        Returns:
            phonenumber (str)   : The purchased phone number
    """

    if pattern:
        number = twilioFindTollFreeNumberInAreaCode(twilioClient, pattern, countryCode=countryCode, max_limit=1)
    else:
        number = None

    if number:
        try:
            logging.info(f"Trying to purchase Toll-Free Twilio number for pattern: {pattern} number: {number}")
            newNumber = twilioClient.incoming_phone_numbers.create(phone_number=number, sms_application_sid=appSid, voice_application_sid=appSid)
            logging.info(f"SID of purchase: {newNumber.sid} Toll-Free Number: {newNumber.phone_number}")
            return newNumber.phone_number
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase Toll-Free number for pattern: {pattern} Error was: {e}")
            logging.error(traceback.print_exc())
            raise AreaCodeUnavailableError('We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again.') from e

    elif phoneNumber is not None:
        try:
            logging.info(f"Trying to purchase Toll-Free Twilio number: {phoneNumber}")
            newNumber = twilioClient.incoming_phone_numbers.create(phone_number=phoneNumber, sms_application_sid=appSid, voice_application_sid=appSid)
            logging.info(f"SID of purchase: {newNumber.sid} Toll-Free Number: {newNumber.phone_number}")
            return newNumber.phone_number
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase Toll-Free number: {phoneNumber} Error was: {e}")
            logging.error(traceback.print_exc())
            raise AreaCodeUnavailableError('We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again.') from e

    else:
        raise AreaCodeUnavailableError('No available numbers left in that area code')
