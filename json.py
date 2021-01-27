# -*- coding: utf-8 -*-
"""
json
"""
import calendar
import datetime
import simplejson

def default_encoder(obj):
    """Default JSON serializer with datetime support."""
    if hasattr(obj, 'timetuple'):
        if isinstance(obj, datetime.datetime):
            if obj.utcoffset() is not None:
                obj = obj - obj.utcoffset()

        millis = int(calendar.timegm(obj.timetuple()) * 1000 + (
            obj.microsecond if hasattr(obj, 'microsecond') else 0) / 1000)
        return str(millis)

    return obj


def encode(obj):
    """JSON encoder with datetime support"""
    return simplejson.dumps(obj, default=default_encoder)
