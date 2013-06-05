# -*- coding: utf-8 -*-

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

def encode(o):
	"""JSON encoder with datetime support"""
	import simplejson
	return simplejson.dumps(o, default=defaultEncoder)

