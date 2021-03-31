# encoding: utf-8

"""
Phone number management abstraction layer
"""

import settings
import logging
from .bw_util import SHBandwidthClient, BandwidthNumberObject
from .bw_util import BandwidthAvailablePhoneNumber
from .bw_util import BWTollFreeUnavailableError
from .twilio_util import twilioBuyPhoneNumber, twilioFindNumberInAreaCode
from .twilio_util import AreaCodeUnavailableError


class SHBoughtNumberObject:
    """
       returns an object with number and sid
          (sid is not used)
       to be compatible with twilio number object
       to minimize changes
    """
    def __init__(self, number, sid, gateway):
        self.phone_number = number
        self.sid = sid
        self.gateway = gateway


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
                         format(self.gateway))
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
    def _sendhub_buy_number(self, gateway, sid, area_code,
                            country_code, phone_number,
                            toll_free, user):
        """
           router that routes calls to appropriate carrier
           specific driver - internal only.
        """
        nbr_obj = None
        if gateway == settings.SMS_GATEWAY_TWILIO:
            nbr_obj = twilioBuyPhoneNumber(
                twilioClient=settings.TWILIO_CLIENT,
                appSid=sid,
                areaCode=area_code,
                countryCode=country_code,
                phoneNumber=phone_number
            )
        elif gateway == settings.SMS_GATEWAY_BANDWIDTH:
            nbr_obj = self._bandwidth_buy_number(area_code, country_code,
                                                 phone_number, toll_free,
                                                 user)
        else:
            logging.info('Invalid gateway {} to buy a number'.
                         format(gateway))

        if nbr_obj:
            return SHBoughtNumberObject(
                nbr_obj.phone_number,
                nbr_obj.sid,
                gateway
            )

    def __call__(self, gateway, sid, area_code=None,
                 country_code='US', phone_number=None,
                 toll_free=False, user=None, alt_gateway=False):
        '''
            cycles through supported gateways.. tries preferred gateway
            first and then tries alternate gateway
        '''
        if gateway not in settings.SUPPORTED_GATEWAYS:
            raise AreaCodeUnavailableError(
                'Invalid gateway: {}'.format(gateway)
            )

        alternate_gateways = [gw for gw in settings.SUPPORTED_GATEWAYS
                              if gw != gateway]

        nbr_obj = None
        exception_msg = None
        try:
            nbr_obj = self._sendhub_buy_number(
                gateway, sid, area_code, country_code,
                phone_number, toll_free, user
            )
        # only for area codes specifics... not for toll-free or
        # complete number.
        # Bandwidth driver raises different exception for different
        # error cases and area code only should be caught
        # Twilio driver does not raise exception for failed full
        # number bought case rather returns None
        except AreaCodeUnavailableError as e:
            exception_msg = e
            logging.info('Unable to buy a number, exception: {}, '
                         'gateway: {}'.format(exception_msg, gateway))

            # if backup GW should be tried
            if not alt_gateway:
                raise

            for a_gateway in alternate_gateways:
                logging.info('trying alternate gateway: {}'.format(a_gateway))
                try:
                    nbr_obj = self._sendhub_buy_number(
                        a_gateway, sid, area_code, country_code,
                        phone_number, toll_free, user
                    )
                except AreaCodeUnavailableError as e:
                    logging.info('Unable to buy number alternate gateway, '
                                 'exception: {}, gateway: {}'.
                                 format(e, a_gateway))
                    pass
                else:
                    return nbr_obj
        else:
            # in cases no valid number is returned and no
            # exception occured, let it fall through and
            # raise another exception
            # this closes gaps with Twilio driver - dont want
            # to change the driver
            if isinstance(nbr_obj, SHBoughtNumberObject):
                return nbr_obj

        # number isnt available, raise an exception for upper layers
        # that are dependent on this exception
        raise AreaCodeUnavailableError('{}'.format(exception_msg))

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
                 toll_free=False, toll_free_area_code='8**'):
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
                try:
                    avail_numbers = SHBandwidthClient().search_available_toll_free_number(  # noqa
                        quantity=quantity,
                        pattern=str(toll_free_area_code[:-1])+'*'
                    )
                except BWTollFreeUnavailableError as e:
                    logging.info('exception {} while searching for toll-free '
                                 'numbers'.format(e))
                    avail_numbers = []
            else:
                try:
                    avail_numbers = SHBandwidthClient().find_number_in_area_code(  # noqa
                        area_code=area_code,
                        country_code=country_code,
                        quantity=quantity
                    )
                except AreaCodeUnavailableError as e:
                    logging.info('exception {} while searching for numbers '
                                 'in area code: {}'.format(e, area_code))
                    avail_numbers = []

            if not isinstance(avail_numbers, list):
                avail_numbers = [avail_numbers]  # convert to list

            return [BandwidthAvailablePhoneNumber(number) for number in avail_numbers]  # noqa
        else:
            logging.info('Invalid Carrier {} to search a number'.
                         format(gateway))
            return []
