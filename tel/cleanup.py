"""
Phone number manipulation tools.

This module is providing utilities for cleaning, validating, formatting,
and inspecting phone numbers using the ``phonenumbers`` library.

"""

__author__ = 'Jay Taylor [@jtaylor]'


import logging

import phonenumbers


def cleanupPhoneNumber(number, region='US'):
    """
    Clean up a phone number and return it in E.164 format.

    This function is leaving shortcodes (3-6 digits) unchanged. If a
    number is starting with the Australian international prefix ``+61``,
    the region is switching to ``AU`` before parsing. Parsing and
    formatting are using the ``phonenumbers`` library.

    Args:
        number (str): The phone number to clean.
        region (str): The default region to use for parsing (default 'US').

    Returns:
        str: The cleaned phone number in E.164 format or the original shortcode.
    """

    # Leaving shortcodes alone
    if len(number) in (3, 4, 5, 6) and number.isdigit():
        return number

    # Switching to AU region for numbers starting with +61
    if str(number).startswith('+61'):
        region = 'AU'

    # Using the given region to parse and then formatting to E.164
    p = phonenumbers.parse(number, region)
    return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164)


def isSpecialTwilioNumber(number):
    """
    Return True if the provided number is a known special Twilio number.

    Args:
        number (str): The phone number to check.

    Returns:
        bool: True if the number is in the special-numbers list.
    """

    specialNumbers = ['+7378742833', '+2562533', '+8656696', '+266696687', '']
    return number in specialNumbers



def validatePhoneNumber(number, allowShortcode=True, country_code='US'):
    """
    Validate a phone number for the given country/region.

    This function is returning True for allowed shortcodes (when
    ``allowShortcode`` is True) and for numbers that are valid for the
    specified ``country_code`` according to the ``phonenumbers`` library.

    Args:
        number (str): The phone number to validate.
        allowShortcode (bool): Whether to treat short numeric codes (3-6
            digits) as valid (default True).
        country_code (str): The country/region code to validate against
            (default 'US').

    Returns:
        bool: True if the number is considered valid, False otherwise.
    """

    valid = False

    try:
        # Leaving shortcodes alone
        if number is not None:
            if allowShortcode and len(number) in (3, 4, 5, 6) and number.isdigit():  # noqa
                return True

            # Using the given country_code to parse the number
            p = phonenumbers.parse(number, country_code)

            # Checking that the parsed number is valid for the region
            if phonenumbers.is_valid_number_for_region(p, country_code):
                phonenumbers.format_number(p,  phonenumbers.PhoneNumberFormat.E164)
                valid = True
    except phonenumbers.NumberParseException as e:
        logging.warning(f'Detected invalid phone number: {number} - {e}')

    return valid


def displayNumber(number, region='US'):
    """
    Return a human-friendly representation of the phone number.

    This function is parsing the number using the provided ``region`` and
    is formatting it using the national format for most regions and the
    international format for Australia (``AU``). If parsing fails, the
    function is falling back to a simple hyphenated grouping.

    Args:
        number (str): The phone number to format.
        region (str): The default region to use for parsing (default 'US').

    Returns:
        str: The formatted phone number for display.
    """

    try:
        # Using the given region to parse the number
        p = phonenumbers.parse(number, region)
        if region == 'AU':
            formattedNumber = phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        else:
            formattedNumber = phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.NATIONAL)
    except phonenumbers.NumberParseException:
        try:
            formattedNumber = '-'.join([number[:3], number[3:6], number[6:]])
        except IndexError:
            formattedNumber = number

    return formattedNumber


def isTollFreeNumber(number, region='US'):
    """
    Return True if the number is a toll-free number.

    Args:
        number (str): The phone number to check.
        region (str): The region to use for parsing (default 'US').

    Returns:
        bool: True if the number is toll-free, False otherwise.
    """
    try:
        p = phonenumbers.parse(number, 'US')
    except phonenumbers.NumberParseException:
        return False

    if phonenumbers.number_type(p) == phonenumbers.PhoneNumberType.TOLL_FREE:
        return True

    return False
