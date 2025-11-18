# -*- coding: utf-8 -*-
from collections.abc import Mapping, Sequence, Set


def defaultEncoder(obj):
    """Default JSON serializer with datetime support."""
    import calendar, datetime

    if hasattr(obj, 'timetuple'):
        if isinstance(obj, datetime.datetime):
            if obj.utcoffset() is not None:
                obj = obj - obj.utcoffset()

        millis = int(calendar.timegm(obj.timetuple()) * 1000 + (obj.microsecond if hasattr(obj, 'microsecond') else 0) / 1000)
        return str(millis)

    return obj

# def encode(o):
# 	"""JSON encoder with datetime support"""
# 	import simplejson
# 	return simplejson.dumps(o, default=defaultEncoder)



def _normalize(obj, default_encoder=None, _seen=None):
    """
    Remove cycles from nested structures and apply default_encoder where needed.
    Does not change structure other than replacing cyclic references.
    """
    if _seen is None:
        _seen = set()

    oid = id(obj)
    if oid in _seen:
        return "<CIRCULAR>"

    # scalars
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    _seen.add(oid)
    try:
        # dicts
        if isinstance(obj, Mapping):
            return {str(k): _normalize(v, default_encoder, _seen) for k, v in obj.items()}

        # lists / tuples
        if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
            return [_normalize(v, default_encoder, _seen) for v in obj]

        # sets
        if isinstance(obj, Set):
            return sorted(_normalize(v, default_encoder, _seen) for v in obj)

        # non-basic object → use your defaultEncoder
        if default_encoder is not None:
            try:
                encoded = default_encoder(obj)
                return _normalize(encoded, default_encoder, _seen)
            except Exception:
                pass

        # final fallback – string repr
        return repr(obj)

    finally:
        _seen.remove(oid)


def encode(o):
    """JSON encoder with datetime support, but cycle-safe."""
    from .json import defaultEncoder
    import simplejson
    clean = _normalize(o, default_encoder=defaultEncoder)
    return simplejson.dumps(clean)
