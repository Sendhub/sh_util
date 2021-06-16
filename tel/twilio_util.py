# -*- coding: utf-8 -*-

"""Twilio utilities."""

import logging
from . import cleanupPhoneNumber


class AreaCodeUnavailableError(Exception):
    """Exception when requested area code is unavailable."""


def twilioFindNumberInAreaCode(twilioClient, areaCode, countryCode='US'):
    """Find a number within an area code."""
    result = twilioClient.api.available_phone_numbers(countryCode).local.list(area_code=areaCode)
    return result

def twilioBuyPhoneNumber(twilioClient, appSid, areaCode=None, countryCode='US',
                         phoneNumber=None):
    """Buy a phone number from twilio."""
    # NB: This could probably actually just use the
    # twilioClient.phone_numbers.purchase(area_code=xxx) method, and it'd be
    # faster.
    numbers = twilioClient.phone_numbers.search(area_code=areaCode,
                                                country=countryCode) \
        if areaCode is not None else False

    if numbers:
        # Attempt to buy twillio number up to 5 times before giving up and
        # showing an error. Sometimes twilio will advertise numbers that
        # cannot be bought.
        for index in range(0, 5):
            try:
                logging.info('buy_phone_number(): buying new number.')
                newNumber = numbers[0].purchase(sms_application_sid=appSid,
                                                voice_application_sid=appSid)
                return newNumber

            except Exception as e:
                logging.error('buy_phone_number(): Failed Buying number. '
                              'Attempt count is: {0}'.format(index))
                logging.error('buy_phone_number() error was: {0}'.format(e))
        else:
            # If we've exhaused our iteration, and we did not break, this else
            # block will run. For more info on for...else see:
            # http://docs.python.org/tutorial/controlflow.html#break-and-
            #   continue-statements-and-else-clauses-on-loops
            #
            # If we didn't get the number, throw an error
            logging.error('buy_phone_number(): Exhausted MAX tries. '
                          'Throwing an error.')
            raise AreaCodeUnavailableError(
                'We are currently having problems buying phone numbers from  '
                'our carrier. Please wait a moment and try again.'
            )

    elif phoneNumber is not None:
        for index in range(0, 5):
            try:
                newNumber = twilioClient.phone_numbers.purchase(
                    sms_application_sid=appSid,
                    voice_application_sid=appSid,
                    phone_number=cleanupPhoneNumber(phoneNumber)
                )

                return newNumber
                # Purchase requested number
            except Exception as e:  # noqa
                import traceback
                traceback.print_exc()

    else:
        raise AreaCodeUnavailableError('No available numbers left in that '
                                       'area code')
