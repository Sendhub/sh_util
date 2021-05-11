# -*- coding: utf-8 -*-

import unicodedata
import re
from .format_string_to_fit_in_n_chars import squeeze_sms_message
from .format_string_to_fit_in_n_chars import format_string_to_fit_in_n_chars
from .ec2HostnameToIp import ec2HostnameToIp
from .split import splitString
from . import case


def ensureAscii(text):
    if type(text) == str:
        encodedText = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')  # noqa
    else:
        encodedText = text

    return encodedText


# Used to cleanup SQL queries sometimes (not always guaranteed to be safe
# WRT messing up your SQL query, discretion required).
_spacesRe = re.compile(r'\s+', re.M)
toSingleLine = lambda s: _spacesRe.sub(' ', s).strip()  # noqa


def stringify(obj):
    """Convert any numeric elements to strings."""
    if type(obj) is dict:
        for k, v in list(obj.items()):
            obj[stringify(k)] = stringify(v)
    elif type(obj) is list:
        return list(map(stringify, obj))
    elif type(obj) is int:
        return str(obj)
    return obj


__all__ = [
    'ec2HostnameToIp',
    'squeeze_sms_message',
    'format_string_to_fit_in_n_chars',
    'ensureAscii',
    'splitString',
    'case',
    'stringify',
]
