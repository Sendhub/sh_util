# -*- coding: utf-8 -*-

"""Telephone utilities."""

import re
import logging

from .cleanup import cleanupPhoneNumber, validatePhoneNumber, displayNumber
from .twilio_util import findNumberInAreaCode
from .twilio_util import buyPhoneNumber
from .twilio_util import AreaCodeUnavailableError
from ..db.distributed import \
    evaluatedDistributedSelect as _evaluatedDistributedSelect
from ..functional import distMemoizeWithExpiry as _distMemoizeWithExpiry


_contactNumberCleaner = re.compile(r'^[+0-9]*$')

# @TODO MEMOIZATION TEMPORARILY DISABLED
#@_distMemoizeWithExpiry(180)
def isSendHubNumber(number):
    """@return True is the number is a sendhub number, False otherwise."""
    if _contactNumberCleaner.match(number) is None:
        logging.warn(u'Refusing to run query with invalid input')
        return False

    res = _evaluatedDistributedSelect(
        '''
            SELECT "pn"."number" "number" FROM "main_phonenumber" "pn"
            JOIN "main_extendeduser" "eu"
                ON "pn"."id" = "eu"."twilio_phone_number_id"
            WHERE "pn"."number" = %s
        ''',
        (number,),
        asDict=True
    )
    return len(res) > 0 and len(res[0].get('number', '')) > 0


__all__ = [
    'cleanupPhoneNumber',
    'findNumberInAreaCode',
    'buyPhoneNumber',
    'AreaCodeUnavailableError',
    'isSendHubNumber',
    'validatePhoneNumber',
    'displayNumber',
    'isSendHubNumber',
]

