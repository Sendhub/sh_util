import re
import unicodedata

from . import case
from .ec2HostnameToIp import ec2HostnameToIp
from .format_string_to_fit_in_n_chars import format_string_to_fit_in_n_chars, squeeze_sms_message
from .split import splitString


def ensureAscii(text):
    if isinstance(text, str):
        encodedText = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')
    else:
        encodedText = text

    return encodedText


# Used to cleanup SQL queries sometimes (not always guaranteed to be safe
# WRT messing up your SQL query, discretion required).
_spacesRe = re.compile(r'\s+', re.M)
toSingleLine = lambda s: _spacesRe.sub(' ', s).strip()


# def stringify(obj):
#     """Convert any numeric elements to strings."""
#     if type(obj) is dict:
#         for k, v in list(obj.items()):
#             obj[stringify(k)] = stringify(v)
#     elif type(obj) is list:
#         return map(stringify, obj)
#     elif isinstance(obj, int):
#         return str(obj)
#     return obj

def stringify(obj):
    """
    Recursively convert numeric elements to strings.

    - Converts int and float values to their string representation (but NOT bool).
    - Converts numeric dict keys to strings (leaves string keys alone).
    - Recurses into lists and dicts and returns new objects (doesn't mutate input).
    """
    # Handle dicts: produce a new dict so we don't mutate while iterating
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            # convert numeric keys to strings, keep string keys as-is
            new_key = stringify(k) if not isinstance(k, str) else k
            new[new_key] = stringify(v)
        return new

    # Handle lists: return a real list (not a map object)
    if isinstance(obj, list):
        return [stringify(item) for item in obj]

    # Numbers: convert ints/floats to strings but avoid converting bool
    if isinstance(obj, (int, float)) and not isinstance(obj, bool):
        return str(obj)

    # Everything else: return as-is
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

