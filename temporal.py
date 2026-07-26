"""
Date and time helper facilities.

This module provides utility functions for working with dates and times, including
functions for epoch time, parsing ISO8601 UTC date strings, and formatting timestamps.
"""

__author__ = "Jay Taylor [@jtaylor]"


import time
from datetime import datetime, timedelta, timezone


def epoch():
    """
    Returning the number of seconds since 1970.
    """

    return int(time.time())


def parse_iso8601_utc_datestring(date_string):
    """
    Parsing an ISO8601 UTC date string and returning a datetime object.

    The input date string must be 24 characters long and in the format:
    2010-04-13T15:29:40+0000.

    Args:
        date_string (str): The ISO8601 UTC date string to parse.

    Returns:
        datetime: The parsed datetime object.

    Raises:
        Exception: If the date string is not 24 characters long.
    """

    if len(date_string) != 24:
        raise ValueError("Timestamps must be 24 characters long, e.g.: 2010-04-13T15:29:40+0000")

    # Collecting timezone info and removing it from the timestamp due to a Python bug
    date_string, tz_info = date_string[:-8], date_string[-5:]

    neg, hours, minutes = tz_info[0], int(tz_info[1:3]), int(tz_info[3:])

    if neg == "+":
        hours, minutes = hours * -1, minutes * -1

    # Converting string to timestamp in the form of: 2010-04-13T15:29:40+0000
    date_obj = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    # Adding timezone info
    date_obj += timedelta(hours=hours, minutes=minutes)
    return date_obj


def pretty_utc_timestamp(_ts=None):
    """
    Generating a nicely formatted UTC timestamp.

    Args:
        _ts (datetime, optional): The datetime object to format. Defaults to None.

    Returns:
        str: The formatted UTC timestamp as a string.
    """

    return (_ts if _ts is not None else datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S UTC")


def week_start_date_string(date=None):
    """
    Calculating the start date of the week for a given date.

    If no date is provided, the current UTC date is used.

    Args:
        date (datetime, optional): The date object to calculate the week start for. Defaults to None.

    Returns:
        str: The start date of the week as a string in the format YYYY-MM-DD.
    """

    if date is None:
        date = datetime.now(timezone.utc)

    # Subtracting the days since Monday
    date = date - timedelta(date.weekday())
    return date.strftime("%Y-%m-%d")
