"""
Telephone utilities.

This module is providing helper functions and re-exports for
telephone-related utilities used across the codebase.
"""

import logging
import re

from ..db import db_query as _db_query
from .bw_util import BWNumberUnavailableError, BWTollFreeUnavailableError, SHBandwidthClient, phonenumber_as_e164
from .cleanup import cleanup_phone_number, display_number, is_special_twilio_number, is_toll_free_number, validate_phone_number, validate_phone_number_by_country
from .tel_util import BuyPhoneNumberFromCarrier, FindPhoneNumberInAreaCode, ReleaseNumberSafely, SHBoughtNumberObject
from .twilio_util import AreaCodeUnavailableError

_contactNumberCleaner = re.compile(r"^[+0-9]*$")

# Temporarily disabling memoization.
# @_distMemoizeWithExpiry(180)


def is_send_hub_number(number):
    """
    Return whether a number is a SendHub number.

    This is validating the input and querying the database for a
    matching SendHub phone number record. It is returning False for
    invalid input.
    """

    if _contactNumberCleaner.match(number) is None:
        logging.warning("Refusing to run query with invalid input")
        return False

    res = _db_query(
        """
            SELECT "pn"."number" "number" FROM "main_phonenumber" "pn"
            JOIN "main_extendeduser" "eu"
                ON "pn"."id" = "eu"."twilio_phone_number_id"
            WHERE "pn"."number" = %s
        """,
        (number,),
        as_dict=True,
    )
    return len(res) > 0 and len(res[0].get("number", "")) > 0


__all__ = [
    "cleanup_phone_number",
    "AreaCodeUnavailableError",
    "is_send_hub_number",
    "validate_phone_number",
    "validate_phone_number_by_country",
    "display_number",
    "is_send_hub_number",
    "is_special_twilio_number",
    "is_toll_free_number",
    "SHBandwidthClient",
    "BWNumberUnavailableError",
    "BWTollFreeUnavailableError",
    "phonenumber_as_e164",
    "BuyPhoneNumberFromCarrier",
    "SHBoughtNumberObject",
    "ReleaseNumberSafely",
    "FindPhoneNumberInAreaCode",
]
