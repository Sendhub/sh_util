# encoding: utf-8

"""
Phone number manipulation tools.
"""

import settings
import logging
from .bw_util import SHBandwidthClient, BandwidthNumberObject
from .bw_util import BandwidthAvailablePhoneNumber
from .twilio_util import twilioBuyPhoneNumber, twilioFindNumberInAreaCode


class ReleaseNumberSafely:
    """
        wrapper that releases numbers back to the carrier.
    """
    def __init__(self, number, gateway, sid):
        self.number = number
        self.gateway = gateway
        self.sid = sid

    def __call__(self):
        '''
        releases the number back to carrier
        :return:
            True if successfully released
            False otherwise
        '''
        if self.gateway == settings.SMS_GATEWAY_TWILIO:
            return self._twilio_safe_number_release()
        elif self.gateway == settings.SMS_GATEWAY_BANDWIDTH:
            return self._bandwidth_safe_number_release()
        else:
            logging.info('Invalid Carrier {} for number release'.
                         format(self))
        return False

    def _twilio_safe_number_release(self):
        '''
        Looks up this number on twilio and releases if the app sid matches
        the app sid configured for this environment.
        :return:
        '''
        deleted = False
        try:
            nbr_object = settings.TWILIO_CLIENT.phone_numbers.get(self.sid)

            if nbr_object.voice_application_sid == settings.TWILIO_APP_SID:
                logging.info('Releasing number: '
                             '{}'.format(nbr_object.phone_number))
                nbr_object.delete()
                deleted = True
        except Exception as e:
            logging.warning(
                'Unable to delete number {} on twilio: '
                '{}'.format(self.number, e))

        return deleted

    def _bandwidth_safe_number_release(self):
        '''
        Looks up this number on Bandwidth and releases the number
        the app sid configured for this environment. APP ID will be
        added by BW client based on configuration so staging wont
        remove prod and vice-versa.
        :return:
        '''
        deleted = False
        try:
            SHBandwidthClient().release_phone_number(self.number)
            deleted = True
        except Exception as e:
            logging.warning(
                'Unable to delete number {} on Bandwidth: '
                '{}'.format(self.number, e))

        return deleted


class BuyPhoneNumberFromCarrier:
    """
        wrapper that buys phone numbers from the carrier.
    """
    def __call__(self, gateway, sid, area_code=None,
                 country_code='US', phone_number=None,
                 toll_free=False, user=None):
        """
           router that routes calls to appropriate carrier
           specific driver.
        """
        if gateway == settings.SMS_GATEWAY_TWILIO:
            return twilioBuyPhoneNumber(
                twilioClient=settings.TWILIO_CLIENT,
                appSid=sid,
                areaCode=area_code,
                countryCode=country_code,
                phoneNumber=phone_number
            )
        elif gateway == settings.SMS_GATEWAY_BANDWIDTH:
            return self._bandwidth_buy_number(area_code, country_code,
                                              phone_number, toll_free,
                                              user)
        else:
            logging.info('Invalid gateway {} to buy a number'.
                         format(gateway))

    def _bandwidth_buy_number(self, area_code, country_code='US',
                              phone_number=None, toll_free=False,
                              user=None):
        """
            makes a call to appropriate function to buy
            a regular or toll free phone number
        """
        bw_client = SHBandwidthClient()
        if toll_free:
            number = bw_client.buy_toll_free_number(
                user_id=user
            )
        else:
            number = bw_client.buy_phone_number(
                phone_number=phone_number,
                area_code=area_code,
                user_id=user,
                country_code=country_code
            )

        return BandwidthNumberObject(number, None)


class FindPhoneNumberInAreaCode:
    """
        wrapper that finds phone numbers from the carrier
        in a given area code.
    """

    def __call__(self, gateway, area_code=None,
                 country_code='US', quantity=4,
                 toll_free=False):
        """
           router that routes calls to appropriate carrier
           specific driver.

        """
        if gateway == settings.SMS_GATEWAY_TWILIO:
            return twilioFindNumberInAreaCode(
                twilioClient=settings.TWILIO_CLIENT,
                areaCode=area_code,
                countryCode=country_code
            )
        elif gateway == settings.SMS_GATEWAY_BANDWIDTH:
            if toll_free:
                # TODO: add support for pattern via portal
                avail_numbers = SHBandwidthClient().search_available_toll_free_number(  # noqa
                    quantity=quantity
                )
            else:
                avail_numbers = SHBandwidthClient().find_number_in_area_code(
                    area_code=area_code,
                    country_code=country_code,
                    quantity=quantity
                )

            if quantity == 1:
                avail_numbers = [avail_numbers]  # convert to list

            return [BandwidthAvailablePhoneNumber(number) for number in avail_numbers]  # noqa
        else:
            logging.info('Invalid Carrier {} to search a number'.
                         format(gateway))
