import re
import unicodedata

from . import case
from .ec2HostnameToIp import ec2_hostname_to_ip
from .format_string_to_fit_in_n_chars import (
    format_string_to_fit_in_n_chars,
    squeeze_sms_message,
)
from .split import split_string


def ensure_ascii(text):
    if isinstance(text, str):
        encoded_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    else:
        encoded_text = text

    return encoded_text


# Used to cleanup SQL queries sometimes (not always guaranteed to be safe
# WRT messing up your SQL query, discretion required).
_spacesRe = re.compile(r"\s+", re.M)
toSingleLine = lambda s: _spacesRe.sub(" ", s).strip()  # noqa


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
    "ec2_hostname_to_ip",
    "squeeze_sms_message",
    "format_string_to_fit_in_n_chars",
    "ensure_ascii",
    "split_string",
    "case",
    "stringify",
]
