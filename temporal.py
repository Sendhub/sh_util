# encoding: utf-8

"""
Date and time helper facilities.
"""

__author__ = 'Jay Taylor [@jtaylor]'


import time
from datetime import datetime, timedelta


def epoch():
    """Return the number of seconds since 1970."""
    return int(time.time())


def parse_ISO8601_UTC_datestring(date_string):
    #check input schedule time is the correct length: 24
    # format = 2010-04-13T15:29:40+0000
    if len(date_string) != 24:
        raise Exception(
            'Timestamps must be 24 characters long, e.g.:'
            '2010-04-13T15:29:40+0000'
        )

    # collect timezone info and remove it from stamp, due to python bug
    date_string, tz_info = date_string[:-8], date_string[-5:]

    neg, hours, minutes = tz_info[0], int(tz_info[1:3]), int(tz_info[3:])

    if neg == '+':
        hours, minutes = hours * -1, minutes * -1

    #convert str to timestamp in the form of: 2010-04-13T15:29:40+0000
    date_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M')

    #add in timezone info
    date_obj += timedelta(hours=hours, minutes=minutes)
    return date_obj


def pretty_utc_timestamp(ts=None):
    """Nicely formatted UTC timestamp."""
    return (ts if ts is not None else datetime.utcnow()) \
        .strftime('%Y-%m-%d %H:%M:%S UTC')


def weekStartDateString(date=None):
    """
    Get the date for the start of a week of a given date.
    Example
    :param date: a date object
    :return:
    """
    if date is None:
        date = datetime.utcnow()

    # Subtract the days since monday
    date = date - timedelta(date.weekday())
    return date.strftime("%Y-%m-%d")