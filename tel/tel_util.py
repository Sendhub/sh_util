"""
    Phone number management abstraction layer
"""

import logging

import src.config as settings

from .bw_util import BandwidthAvailablePhoneNumber, BandwidthNumberObject, BWTollFreeUnavailableError, SHBandwidthClient
from .twilio_util import AreaCodeUnavailableError, twilioBuyPhoneNumber, twilioBuyTollFreePhoneNumber, twilioFindNumberInAreaCode, twilioFindTollFreeNumberInAreaCode


class SHBoughtNumberObject:
    """
       Returns an object with number and sid (sid is not used)
       - Compatible with twilio number object
       - Minimizing changes
    """
    def __init__(self, number, sid, gateway):
        self.phone_number = number
        self.sid = sid
        self.gateway = gateway


class ReleaseNumberSafely:
    """
        Wrapper that releases numbers back to the carrier.
    """
    def __init__(self, number, gateway, sid):
        self.number = number
        self.gateway = gateway
        self.sid = sid

    def __call__(self):
        '''
        Releasing the number back to carrier

        Returns:
            deleted bool: If the number was successfully released then True or else False.
        '''
        if self.gateway == settings.SMS_GATEWAY_TWILIO:
            return self._twilio_safe_number_release()
        elif self.gateway == settings.SMS_GATEWAY_BANDWIDTH:
            return self._bandwidth_safe_number_release()
        else:
            logging.info(f"Invalid Carrier {self.gateway} for number release")

        return False

    def _twilio_safe_number_release(self):
        """
        Looks up this number on twilio and releases if the app sid matches the app sid configured for this environment.

        Returns:
            deleted bool: If the number was successfully released then True or else False.
        """

        deleted = False
        try:
            nbr_object = settings.TWILIO_CLIENT.phone_numbers.get(self.sid)

            if nbr_object.voice_application_sid == settings.TWILIO_APP_SID:
                logging.info(f"Releasing number: {nbr_object.phone_number}")
                nbr_object.delete()
                deleted = True
        except Exception as e:
            logging.warning(f"Unable to delete number {self.number} on twilio: {e}")

        return deleted

    def _bandwidth_safe_number_release(self):
        """
        Looks up this number on Bandwidth and releases the number the app sid configured for this environment.
        APP ID will be added by BW client based on configuration so staging wont remove prod and vice-versa.

        Returns:
            deleted bool: If the number was successfully released then True or else False.
        """

        deleted = False
        try:
            SHBandwidthClient().release_phone_number(self.number)
            deleted = True
        except Exception as e:
            logging.warning(f"Unable to delete number {self.number} on Bandwidth: {e}")

        return deleted


class BuyPhoneNumberFromCarrier:
    """
        wrapper that buys phone numbers from the carrier.
    """
    def _sendhub_buy_number(self, gateway, sid, area_code, country_code, phone_number, toll_free, user):
        """
           Router that routes calls to appropriate carrier specific driver - internal only.
        """

        nbr_obj = None

        if gateway == settings.SMS_GATEWAY_TWILIO:
            nbr_obj = self._twilio_buy_number(sid, area_code, country_code='US', phone_number=phone_number, toll_free=False)
        elif gateway == settings.SMS_GATEWAY_BANDWIDTH:
            nbr_obj = self._bandwidth_buy_number(area_code, country_code, phone_number, toll_free, user)
        else:
            logging.info(f'Invalid gateway {gateway} to buy a number')

        logging.info(f"Purchased Number Object: {nbr_obj} and Gateway: {gateway}")

        if nbr_obj:
            return nbr_obj


    def __call__(self, gateway, sid, area_code=None, country_code='US', phone_number=None, toll_free=False, user=None, alt_gateway=False):
        """
            Cycles through supported gateways..
            Tries preferred gateway first and then tries alternate gateway
        """
        if gateway not in settings.SUPPORTED_GATEWAYS:
            raise AreaCodeUnavailableError(f'Invalid gateway: {gateway}')

        alternate_gateways = [gw for gw in settings.SUPPORTED_GATEWAYS if gw != gateway]

        nbr_obj = None
        exception_msg = None
        try:
            nbr_obj = self._sendhub_buy_number(gateway, sid, area_code, country_code, phone_number, toll_free, user)
            return nbr_obj
        # only for area codes specifics... not for toll-free or
        # complete number.
        # Bandwidth driver raises different exception for different
        # error cases and area code only should be caught
        # Twilio driver does not raise exception for failed full
        # number bought case rather returns None
        except AreaCodeUnavailableError as e:
            exception_msg = e
            logging.info(f'Unable to buy a number, exception: {exception_msg}, gateway: {gateway}')

            # if backup GW should be tried
            if not alt_gateway:
                raise

            for a_gateway in alternate_gateways:
                logging.info(f'Trying alternate gateway: {a_gateway}')
                try:
                    nbr_obj = self._sendhub_buy_number(a_gateway, sid, area_code, country_code, phone_number, toll_free, user)
                except AreaCodeUnavailableError as e:
                    logging.info(f'Unable to buy number alternate gateway, exception: {e}, gateway: {a_gateway}')
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
        raise AreaCodeUnavailableError(f'{exception_msg}')

    def _bandwidth_buy_number(self, area_code, country_code='US', phone_number=None, toll_free=False, user=None):
        """
            Makes a call to appropriate function to buy a regular or toll free phone number
        """
        bw_client = SHBandwidthClient()
        if toll_free:
            number = bw_client.buy_toll_free_number(quantity=1, pattern=area_code, site_id=settings.BW_SITE_ID, user_id=user)
        else:
            number, sid = bw_client.buy_phone_number(phone_number=phone_number, area_code=area_code, user_id=user, country_code=country_code)

        return BandwidthNumberObject(number, sid)

    def _twilio_buy_number(self, sid, area_code, country_code='US', phone_number=None, toll_free=False):
        """
            Making a call to appropriate function to buy a regular or toll free phone number.
        """
        if toll_free:
            number = twilioBuyTollFreePhoneNumber(twilioClient=settings.TWILIO_CLIENT, appSid=settings.TWILIO_APP_SID_STAGING, pattern=area_code, countryCode='US', phoneNumber=phone_number)
        else:
            number = twilioBuyPhoneNumber(twilioClient=settings.TWILIO_CLIENT, appSid=sid, areaCode=area_code, countryCode=country_code, phoneNumber=phone_number)

        return number


class FindPhoneNumberInAreaCode:
    """
        Wrapper that finds phone numbers from the carrier in a given area code.
    """

    def __call__(self, gateway, area_code=None, country_code='US', quantity=4, toll_free=False, toll_free_area_code=None):
        """
           Router that routes calls to appropriate carrier specific driver.
        """

        if gateway == settings.SMS_GATEWAY_TWILIO:
            if toll_free:
                try:
                    avail_numbers = twilioFindTollFreeNumberInAreaCode(settings.TWILIO_CLIENT, pattern=toll_free_area_code, countryCode='US', max_limit=quantity)
                except AreaCodeUnavailableError as e:
                    logging.info(f"Exception {e} while searching for toll-free numbers which contain: {toll_free_area_code}")
                    avail_numbers = []
            else:
                try:
                    avail_numbers = twilioFindNumberInAreaCode(twilioClient=settings.TWILIO_CLIENT, areaCode=area_code, countryCode=country_code, max_limit=quantity, only_list=False)
                except AreaCodeUnavailableError as e:
                    logging.info(f"Exception {e} while searching for numbers in area code: {area_code}")
                    avail_numbers = []

            # BandwidthAvailablePhoneNumber similar implementation is not required for Twilio as the objects contain the necessary information

            return avail_numbers

        elif gateway == settings.SMS_GATEWAY_BANDWIDTH:
            if toll_free:
                try:
                    avail_numbers = SHBandwidthClient().search_available_toll_free_number(pattern=toll_free_area_code, quantity=quantity)
                except BWTollFreeUnavailableError as e:
                    logging.info(f"Exception {e} while searching for toll-free numbers with pattern: {toll_free_area_code}")
                    avail_numbers = []
            else:
                try:
                    avail_numbers = SHBandwidthClient().find_number_in_area_code(area_code=area_code, country_code=country_code, quantity=quantity)
                except AreaCodeUnavailableError as e:
                    logging.info(f"Exception {e} while searching for numbers in area code: {area_code}")
                    avail_numbers = []

            if not isinstance(avail_numbers, list):
                avail_numbers = [avail_numbers]

            return [BandwidthAvailablePhoneNumber(number) for number in avail_numbers]
        else:
            logging.info(f"Invalid Carrier {gateway} to search a number")
            return []
