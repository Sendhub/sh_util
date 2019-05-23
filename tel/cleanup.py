# encoding: utf-8

"""
Phone number manipulation tools.
"""

__author__ = 'Jay Taylor [@jtaylor]'


import phonenumbers
import logging


def cleanupPhoneNumber(number, region='US'):
    # Leave shortcodes alone.
    if len(number) in (3, 4, 5, 6) and number.isdigit():
        return number

    # it's okay to search for the region US for all US/Can b/c they share
    # the same parsing/formatting rules
    p = phonenumbers.parse(number, region)
    return phonenumbers.format_number(
        p,
        phonenumbers.PhoneNumberFormat.E164
    )


def isSpecialTwilioNumber(number):
    specialNumbers = ['+7378742833', '+2562533', '+8656696', '+266696687', '']
    return number in specialNumbers


def validatePhoneNumber(number, allowShortcode=True):
    valid = False

    try:
        # Leave shortcodes alone.

        if number is not None:
            if allowShortcode and len(number) in (3, 4, 5, 6) and number.isdigit():  # noqa
                return True

            # it's okay to search for the region US for all US/Can b/c they
            # share the same parsing/formatting rules
            p = phonenumbers.parse(number, 'US')

            # but we need to check the number is valid in either country
            if phonenumbers.is_valid_number_for_region(p, 'US') or \
                    phonenumbers.is_valid_number_for_region(p, 'CA'):
                phonenumbers.format_number(
                    p,
                    phonenumbers.PhoneNumberFormat.E164
                )
                valid = True
    except phonenumbers.NumberParseException as e:
        logging.warning('Detected invalid phone number: {0} - {1}'.
                        format(number, e))

    return valid


def displayNumber(number, region='US'):
    """Prettier phone number for display purposes."""
    try:
        # it's okay to search for the region US for all US/Can b/c they
        # share the same parsing/formatting rules
        p = phonenumbers.parse(number, region)
        formattedNumber = phonenumbers.format_number(
            p,
            phonenumbers.PhoneNumberFormat.NATIONAL
        )
    except phonenumbers.NumberParseException:
        try:
            formattedNumber = '-'.join([number[:3], number[3:6], number[6:]])
        except IndexError:
            formattedNumber = number

    return formattedNumber


def isTollFreeNumber(number, region='US'):
    """ returns True if number is a TollFree Number """
    try:
        p = phonenumbers.parse(number, 'US')
    except phonenumbers.NumberParseException:
        return False

    if phonenumbers.number_type(p) == \
            phonenumbers.PhoneNumberType.TOLL_FREE:
        return True

    return False
