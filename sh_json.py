"""
This module provides utilities for JSON encoding with support for datetime objects
and cycle-safe structures.

Functions:
- default_encoder: Encodes datetime objects into JSON-compatible formats.
- encode: Encodes Python objects into JSON strings.
- _normalize: Normalizes Python objects to remove cycles and apply custom encoders.
"""

import calendar
import datetime
from collections.abc import Mapping, Sequence, Set

import simplejson


def default_encoder(obj):
    """
    Encodes datetime objects into JSON-compatible formats.

    If the object has a `timetuple` attribute, it calculates the milliseconds
    since the epoch. Otherwise, it converts the object to a list.
    """

    if hasattr(obj, "timetuple"):
        if isinstance(obj, datetime.datetime):
            if obj.utcoffset() is not None:
                obj = obj - obj.utcoffset()

        millis = int(calendar.timegm(obj.timetuple()) * 1000 + (obj.microsecond if hasattr(obj, "microsecond") else 0) / 1000)
        return str(millis)
    return list(obj)


def encode(o):
    """
    Encodes Python objects into JSON strings.

    This function ensures cycle-safe encoding by normalizing the input object
    and applying the `default_encoder` where necessary.
    """

    clean = _normalize(o, default_encoder=default_encoder)
    return simplejson.dumps(clean)


def _normalize(obj, default_encoder=None, _seen=None):
    """
    Normalizes Python objects to remove cycles and apply custom encoders.

    This function traverses nested structures, replacing cyclic references
    with a placeholder and applying the `default_encoder` to non-basic objects.
    """

    if _seen is None:
        _seen = set()

    oid = id(obj)
    if oid in _seen:
        return "<CIRCULAR>"

    # Handling scalar values
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    _seen.add(oid)
    try:
        # Handling dictionaries
        if isinstance(obj, Mapping):
            return {str(k): _normalize(v, default_encoder, _seen) for k, v in obj.items()}

        # Handling lists and tuples
        if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
            return [_normalize(v, default_encoder, _seen) for v in obj]

        # Handling sets
        if isinstance(obj, Set):
            return sorted(_normalize(v, default_encoder, _seen) for v in obj)

        # Handling non-basic objects using the default encoder
        if default_encoder is not None:
            try:
                encoded = default_encoder(obj)
                return _normalize(encoded, default_encoder, _seen)
            except Exception:
                pass

        # Fallback to string representation
        return repr(obj)

    finally:
        _seen.remove(oid)
