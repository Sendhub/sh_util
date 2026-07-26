"""
Twilio Utilities
"""

import logging
import re
import traceback
from decimal import Decimal

_AREA_CODE_UNAVAILABLE_MSG = "We are currently having problems buying phone numbers from our carrier. Please wait a moment and try again."


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
        },
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


def twilio_find_number_in_area_code(twilio_client, area_code, country_code="US", max_limit=6, only_list=False):
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
        logging.info(f"Before searching for area-code: {area_code}")
        result = twilio_client.api.v2010.accounts(twilio_client.username).available_phone_numbers(country_code).local.list(area_code=area_code, limit=max_limit)
        logging.info(f"After searching for area-code: {area_code}")
        logging.info(f"Result: {result}")
        return [inst.phone_number for inst in result] if only_list else result

    except Exception as e:
        logging.error(f"Exception occurred while trying to list number for area-code: {area_code} Error was: {e}")
        logging.error(traceback.format_exc())
        raise AreaCodeUnavailableError("We are currently having problems finding phone numbers from our carrier. Please wait a moment and try again.") from e


def twilio_find_toll_free_number_in_area_code(twilio_client, pattern, country_code="US", max_limit=6):
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
        logging.info("Before searching for toll_free")
        result = twilio_client.api.v2010.accounts(twilio_client.username).available_phone_numbers(country_code).toll_free.list(contains=pattern, limit=max_limit)
        logging.info("After searching for toll_free")
        logging.info(f" Result: {result}")
        return result
    except Exception as e:
        logging.error(f"Exception occurred while trying to list number for toll-free number Error was: {e}")
        logging.error(traceback.format_exc())
        raise AreaCodeUnavailableError("We are currently having problems finding phone numbers from our carrier. Please wait a moment and try again.") from e


def twilio_buy_phone_number(twilio_client, app_sid, area_code=None, country_code="US", phone_number=None):
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

    if area_code:
        number = twilio_find_number_in_area_code(twilio_client, area_code, country_code=country_code, max_limit=1)
    else:
        number = None

    if number:
        try:
            logging.info(f"Trying to purchase Twilio number for area-code: {area_code} number: {number}")
            new_number = twilio_client.incoming_phone_numbers.create(phone_number=number, sms_application_sid=app_sid, voice_application_sid=app_sid)
            logging.info(f"SID of purchase: {new_number.sid} Number: {new_number.phone_number}")
            return new_number
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase number for area-code: {area_code} Error was: {e}")
            logging.error(traceback.format_exc())
            raise AreaCodeUnavailableError(_AREA_CODE_UNAVAILABLE_MSG) from e

    elif phone_number is not None:
        try:
            logging.info(f"Trying to purchase Twilio number: {phone_number}")
            new_number = twilio_client.incoming_phone_numbers.create(phone_number=phone_number, sms_application_sid=app_sid, voice_application_sid=app_sid)
            logging.info(f"SID of purchase: {new_number.sid} Number: {new_number.phone_number}")
            return new_number
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase number: {phone_number} Error was: {e}")
            logging.error(traceback.format_exc())
            raise AreaCodeUnavailableError(_AREA_CODE_UNAVAILABLE_MSG) from e

    else:
        raise AreaCodeUnavailableError("No available numbers left in that area code")


def twilio_buy_toll_free_phone_number(twilio_client, app_sid, pattern=None, country_code="US", phone_number=None):
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
        number = twilio_find_toll_free_number_in_area_code(twilio_client, pattern, country_code=country_code, max_limit=1)
    else:
        number = None

    if number:
        try:
            logging.info(f"Trying to purchase Toll-Free Twilio number for pattern: {pattern} number: {number}")
            new_number = twilio_client.incoming_phone_numbers.create(phone_number=number, sms_application_sid=app_sid, voice_application_sid=app_sid)
            logging.info(f"SID of purchase: {new_number.sid} Toll-Free Number: {new_number.phone_number}")
            return new_number.phone_number
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase Toll-Free number for pattern: {pattern} Error was: {e}")
            logging.error(traceback.format_exc())
            raise AreaCodeUnavailableError(_AREA_CODE_UNAVAILABLE_MSG) from e

    elif phone_number is not None:
        try:
            logging.info(f"Trying to purchase Toll-Free Twilio number: {phone_number}")
            new_number = twilio_client.incoming_phone_numbers.create(phone_number=phone_number, sms_application_sid=app_sid, voice_application_sid=app_sid)
            logging.info(f"SID of purchase: {new_number.sid} Toll-Free Number: {new_number.phone_number}")
            return new_number.phone_number
        except Exception as e:
            logging.error(f"Exception occurred while trying to purchase Toll-Free number: {phone_number} Error was: {e}")
            logging.error(traceback.format_exc())
            raise AreaCodeUnavailableError(_AREA_CODE_UNAVAILABLE_MSG) from e

    else:
        raise AreaCodeUnavailableError("No available numbers left in that area code")
