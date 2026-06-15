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
    Validate a phone number globally — no country context required.

    For numbers in E.164 format (starting with ``+``), the ITU calling code
    embedded in the number is used directly and ``country_code`` is ignored.
    For bare national numbers without a ``+`` prefix, ``country_code`` is
    used only as a last-resort parsing hint.

    Args:
        number (str|bytes): The phone number to validate.
        allowShortcode (bool): Whether to treat short numeric codes (3-6
            digits) as valid (default True).
        country_code (str): Fallback region hint for bare national numbers
            that lack a ``+`` prefix (default 'US').

    Returns:
        bool: True if the number is considered valid, False otherwise.
    """

    try:
        if number is None:
            return False

        if isinstance(number, bytes):
            number = number.decode('utf-8')

        if allowShortcode and len(number) in (3, 4, 5, 6) and number.isdigit():
            return True

        # Parse region-free first: works for any E.164 number (+XXXXXXXXX).
        # The phonenumbers library reads the ITU calling code from the number
        # itself, so no country context is needed at all.
        try:
            p = phonenumbers.parse(number, None)
        except phonenumbers.NumberParseException:
            # Bare national number with no '+' — use the hint as a last resort.
            p = phonenumbers.parse(number, country_code)

        return phonenumbers.is_valid_number(p)

    except (phonenumbers.NumberParseException, UnicodeDecodeError) as e:
        logging.warning(f'Detected invalid phone number: {number} - {e}')
        return False


def validatePhoneNumberByCountry(number, country_code):
    """
    Validate a phone number against a specific country's calling-code group.

    Numbers sharing the same ITU country calling code are accepted together.
    For example, US and Canada both use ``+1``, so a Canadian number passes
    validation for ``country_code='US'`` and vice versa.

    Args:
        number (str|bytes): The phone number to validate (E.164 preferred).
        country_code (str): The ISO 3166-1 alpha-2 region code to validate
            against (e.g. ``'US'``, ``'CA'``, ``'AU'``).

    Returns:
        bool: True if the number is valid and its calling code matches the
        calling code of the given ``country_code``, False otherwise.
    """

    if number is None or country_code is None:
        return False

    if isinstance(number, bytes):
        try:
            number = number.decode('utf-8')
        except UnicodeDecodeError:
            return False

    if not number:
        return False

    try:
        calling_code = phonenumbers.country_code_for_region(country_code)
        if calling_code == 0:
            return False

        p = phonenumbers.parse(number, country_code)
        if not phonenumbers.is_valid_number(p):
            return False

        return p.country_code == calling_code
    except phonenumbers.NumberParseException:
        return False


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
