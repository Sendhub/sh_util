# -*- coding: utf-8 -*-

"""Telephone utilities."""

import re
import logging

from .cleanup import cleanupPhoneNumber, validatePhoneNumber, \
    displayNumber, isSpecialTwilioNumber, isTollFreeNumber
from .twilio_util import AreaCodeUnavailableError
from ..db import db_query as _db_query
from .bw_util import SHBandwidthClient, \
    BWNumberUnavailableError, BWTollFreeUnavailableError, \
    phonenumber_as_e164
from .tel_util import BuyPhoneNumberFromCarrier, \
    ReleaseNumberSafely, FindPhoneNumberInAreaCode, \
    SHBoughtNumberObject

_contactNumberCleaner = re.compile(r'^[+0-9]*$')

# @TODO MEMOIZATION TEMPORARILY DISABLED
# @_distMemoizeWithExpiry(180)


def isSendHubNumber(number):
    """@return True is the number is a sendhub number, False otherwise."""
    if _contactNumberCleaner.match(number) is None:
        logging.warn(u'Refusing to run query with invalid input')
        return False

    res = _db_query(
        '''
            SELECT "pn"."number" "number" FROM "main_phonenumber" "pn"
            JOIN "main_extendeduser" "eu"
                ON "pn"."id" = "eu"."twilio_phone_number_id"
            WHERE "pn"."number" = %s
        ''',
        (number,),
        as_dict=True
    )
    return len(res) > 0 and len(res[0].get('number', '')) > 0


__all__ = [
    'cleanupPhoneNumber',
    'AreaCodeUnavailableError',
    'isSendHubNumber',
    'validatePhoneNumber',
    'displayNumber',
    'isSendHubNumber',
    'isSpecialTwilioNumber',
    'isTollFreeNumber',
    'SHBandwidthClient',
    'BWNumberUnavailableError',
    'BWTollFreeUnavailableError',
    'phonenumber_as_e164',
    'BuyPhoneNumberFromCarrier',
    'SHBoughtNumberObject',
    'ReleaseNumberSafely',
    'FindPhoneNumberInAreaCode'
]
