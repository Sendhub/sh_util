import os
import sys
import logging
import phonenumbers
import json
import requests

try:
    import settings
except ImportError:
    # when unit testing is invoked
    sys.path.append('/opt/sendhub/inforeach/app')
    import settings

try:
    from sh_util.tel import validatePhoneNumber, AreaCodeUnavailableError
    from sh_util.tel import displayNumber
except ImportError:
    sys.path.append('/opt/sendhub/inforeach/app')
    from sh_util.tel import validatePhoneNumber, AreaCodeUnavailableError
    from sh_util.tel import displayNumber

import bandwidth
from bandwidth.account import BandwidthAccountAPIException
try:
    from bandwidth.account import BandwidthOrderPendingException
except ImportError:
    # future proof - this exception is SendHub defined in BW code
    # with eventual release of BW's SDK, exception may no longer
    # relevant
    class BandwidthOrderPendingException(Exception):
        """Exception when Toll Free Number is unavailable."""


class BWNumberUnavailableError(Exception):
    """Exception when requested BW Number is unavailable."""


class BWTollFreeUnavailableError(Exception):
    """Exception when Toll Free Number is unavailable."""


class BandwidthAvailablePhoneNumber:
    """
       for bandwidth carrier, numbers returned are number (if qty = 1),
       or a list of numbers. This router also converts bandwidth list
       to list of numbers in dictionary with pretty name as key..
       similar to format provided by Twilio so that upper layers are
       at ease. Each number is in the format below:

       {"friendly_name":"(580) 271-9612", "phone_number":"+15802719612"}
    """
    def __init__(self, number):
        self.friendly_name = displayNumber(number)
        self.phone_number = number
        self.gateway = settings.SMS_GATEWAY_BANDWIDTH


def phonenumber_as_e164(number, country_code='US'):
    """
      this function should be called mainly with valid
      phone numbers.
      Exception is raised if number is invalid
    """
    if not isinstance(number, str):
        number = str(number)
    if validatePhoneNumber(number, False) is False:
        raise ValueError("Invalid phone number %i - unable to process",
                         number)
    return phonenumbers.format_number(
        phonenumbers.parse(number, country_code),
        phonenumbers.PhoneNumberFormat.E164
    )


class BandwidthNumberObject:
    """
       returns an object with number and sid
          (sid is not used)
       to be compatible with twilio number object
       to minimize changes
    """
    def __init__(self, number, sid):
        self.phone_number = number
        self.sid = sid


class SHBandwidthClient(object):
    NUMBER_UNAVAILABLE_MSG = \
        'We are currently having problems buying phone numbers from  ' \
        'our carrier. Please wait a moment and try again or choose a ' \
        'different area code.'

    def __init__(self, userid=None, token=None,
                 secret=None, username=None, password=None,
                 debug=False):
        if not userid:
            userid = settings.BW_USER_ID
        if not token:
            token = settings.BW_API_TOKEN
        if not secret:
            secret = settings.BW_API_SECRET
        # username and password are used for account
        # API authentication
        if not username:
            username = settings.BW_USERNAME
        if not password:
            password = settings.BW_PASSWORD

        # saving these as part of object as media get
        # operations are done here.... BW SDK when
        # released, may not support it.
        self.userid = userid
        self.token = token
        self.secret = secret
        self.username = username
        self.password = password

        if not userid or not token or not secret \
           or not username or not password:
            raise ValueError('Appropriate Bandwidth Keys are not available '
                             'supplied userid: %i, token: %r, '
                             'secret: %r, username: %r, password: %r',
                             userid, token, secret, username, password)

        self.voice_client = bandwidth.client('voice',
                                             userid,
                                             token,
                                             secret,
                                             api_version='v2',
                                             DEBUG=debug)
        self.sms_client = bandwidth.client(
            'messaging',
            userid,
            token,
            secret,
            api_version='v2',
            api_endpoint=settings.BW_MESSAGING_API_URL,
            DEBUG=debug
        )
        self.account_client = bandwidth.client(
            'account',
            userid,
            username,
            password,
            api_version='v2',
            api_endpoint=settings.BW_ACCOUNT_API_URL,
            account_id=settings.BW_ACCOUNT_ID,
            DEBUG=debug
        )

    @staticmethod
    def _as_e164(number, country_code='US'):
        """
        d
        """
        return phonenumber_as_e164(number, country_code)

    def send_sms(self, from_number, to_number, msg, tag=None):
        """
           sends SMS via Bandwidth API call.
           returns message_id
        """
        # allow sending to a group in one call
        if not isinstance(to_number, list):
            to_number = [to_number]

        return self.sms_client.send_message(
            from_=self._as_e164(from_number),
            to=[self._as_e164(number) for number in to_number],
            text=msg,
            tag=tag,
            applicationId=settings.BW_APP_ID
        )

    def send_mms(self, from_number, to_number, msg, media, tag=None):
        """
           sends MMS via Bandwidth API call.
           returns message_id
        """
        # allow sending to a group in one call
        if not isinstance(to_number, list):
            to_number = [to_number]

        return self.sms_client.send_message(
            from_=self._as_e164(from_number),
            to=[self._as_e164(number) for number in to_number],
            text=msg,
            tag=tag,
            media=media,
            applicationId=settings.BW_APP_ID
        )

    def get_message_info(self, msgid=None):
        """
           returns message info.
           with v2 messaging, this method does not work.
        """
        if msgid:
            return self.sms_client.get_message(msgid)

        raise NotImplementedError('This method is not supported '
                                  'with v2 messaging')


    def send_hello(self, from_number, to_number):
        return self.send_sms(from_number, to_number,
                             'Hello from Sendhub through Bandwidth!')

    def check_msg_status(self, msg_id):
        return self.sms_client.get_message(msg_id)

    def _cleanup_and_return_numbers(self,
                                    numbers,
                                    quantity,
                                    country_code='US'):
        """
          helper function that takes in the numbers list
          returned by BW APIs, formats them and returns:
          number itself if quantity is 1 else
          returns list of numbers.
        """
        if quantity == 1:
            return self._as_e164(numbers[0], country_code)
        elif quantity > 1:
            return [self._as_e164(number, country_code) for number in numbers]
        else:
            raise ValueError('Quantity can not be < 1 - passed: %i',
                             quantity)

    def _parse_number_to_bw_format(self, number, country_code='US'):
        """
        cleanup the number - remove country code as bandwidth
        does not except country code?
        TODO: must be an API from phonenumbers library that allows
        parsing national number
        """
        return phonenumbers.format_number(
            phonenumbers.parse(str(number), 'US'),
            phonenumbers.PhoneNumberFormat.E164
        )[2:]

    def buy_phone_number(self, phone_number=None,
                         area_code=None, user_id=None,
                         site_id=None,
                         country_code='US'):
        """
          buy a phone number 'phone_number' from bandwidth
          :
          :param: phone_number - if specified, number to be bought
          :param: area_code - if specific phone number is not the ask
          :param: country_code = 'US' only supported
          :param: user_id this phone number if allocated to - for BW dashboard

          : returns: phone number bought, None if invalid parameters
          :          or Exception if there is one.
        """
        if country_code not in ('US', 'CA'):
            logging.info('Only numbers in US or CA are supported, requested '
                         'country: %i', country_code)

        site_id = site_id if site_id else settings.BW_SITE_ID

        if phone_number:
            if validatePhoneNumber(phone_number, False) is False:
                raise ValueError("Invalid phone number passed- unable to buy")

            # a specific number ought to be ordered
            logging.info('buy_phone_number(): buying requested number: {}.'.
                         format(phone_number, site_id))
            try:
                newNumber = self.account_client.order_phone_number(
                    number=self._parse_number_to_bw_format(phone_number),
                    name='SendHub Customer: {}'.format(user_id),
                    quantity=1,
                    siteid=site_id
                )
            except BandwidthOrderPendingException as order_id:
                logging.warning('Order %i is pending for phone number: %i, '
                                'user: %s, looks like bandwidth service is '
                                'slow. Error out for now and nightly cleanup '
                                'task will release the number.',
                                order_id, phone_number, user_id)
                raise BWNumberUnavailableError(
                    'Pending Number Order: ' +
                    SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
                )
            except BandwidthAccountAPIException as err:
                # If we didn't get the number, throw an error
                err_resp = 'We could not get number %i from our carrier. ' \
                           'Carrier Message: %r.', phone_number, str(err)
                logging.error(err_resp)
                raise BWNumberUnavailableError(err_resp)

            # we bought the number successfully
            return self._cleanup_and_return_numbers(newNumber, quantity=1)
        else:
            if area_code is None:
                return False

            try:
                ordered_number = self.account_client.search_and_order_local_numbers(  # noqa
                              area_code=area_code,
                              quantity=1,
                              name='SendHub Customer: {}'.format(user_id),
                              siteid=site_id
                )

            except BandwidthOrderPendingException as order_id:
                logging.warning('Order %i is pending for a number in '
                                'area code: %i, user_id: %i, qty: 1, '
                                'looks like bandwidth service is slow. '
                                'Error out for now and nightly cleanup task '
                                'will release the number.',
                                order_id, area_code, user_id)
                raise AreaCodeUnavailableError(
                    'Pending Area Code Order: ' +
                    SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
                )
            except BandwidthAccountAPIException as err:
                # If we didn't get the number, throw an error
                logging.error('buy_phone_number(): could not get number. '
                              'Throwing an error - %r.', str(err))
                raise AreaCodeUnavailableError(
                    SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
                )

            return self._cleanup_and_return_numbers(ordered_number, quantity=1)

    def release_phone_number(self, number):
        """
          returns phone number 'number' back to bandwidth
          :
          :param: Number - number to be returned
          : returns True or Exception if there is one.
        """
        number = str(number)
        if validatePhoneNumber(number, False) is False:
            raise ValueError("Invalid phone number %i - unable to release",
                             number)

        nat_number = self._parse_number_to_bw_format(str(number), 'US')
        try:
            self.account_client.delete_phone_number(nat_number)
        except BandwidthAccountAPIException as err:
            logging.info("Error Deleting phone# %i, Exception: %r",
                         number, str(err))
            raise

    def find_number_in_area_code(self,
                                 area_code,
                                 quantity=1,
                                 country_code='US'):
        """Find a number within an area code."""
        if country_code not in ('US', 'CA'):
            logging.info('Only numbers in US/CA are supported, requested '
                         'country: %i', country_code)

        if quantity < 1:
            raise ValueError('Quantity can not be < 1 - passed: %i',
                             quantity)

        try:
            numbers = self.account_client.search_available_local_numbers(
                area_code=area_code,
                quantity=quantity
            )
        except BandwidthAccountAPIException as err:
            logging.info('Failed to search for phone number in given area '
                         'code - error: %r', str(err))
            raise AreaCodeUnavailableError(
                SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
            )

        else:
            if not numbers:
                raise AreaCodeUnavailableError(
                    SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
                )
            return self._cleanup_and_return_numbers(numbers, quantity)

    def search_available_toll_free_number(self, pattern=None, quantity=1):
        """searche toll free number."""
        if quantity < 1:
            raise ValueError('Quantity can not be < 1 - passed: %i',
                             quantity)

        try:
            pattern = pattern if pattern else '8**'
            toll_free_numbers = self.account_client.search_available_toll_free_numbers(  # noqa
                                       quantity=quantity,
                                       pattern=pattern
            )

        except BandwidthAccountAPIException as err:
            # If we didn't get the number, throw an error
            logging.error('search_tollfree(): could not get toll '
                          'free number. '
                          'Throwing an error - %r.', str(err))
            raise BWTollFreeUnavailableError(
                SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
            )

        else:
            if not toll_free_numbers:
                raise BWTollFreeUnavailableError(
                    SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
                )
            return self._cleanup_and_return_numbers(toll_free_numbers,
                                                    quantity)

    def buy_toll_free_number(self,
                             quantity=1,
                             pattern=None,
                             site_id=None,
                             user_id=None):
        """procures a toll free number."""
        if quantity < 1:
            raise ValueError('Quantity can not be < 1 - passed: %r',
                             quantity)

        site_id = site_id if site_id else settings.BW_SITE_ID
        try:
            toll_free_numbers = self.account_client.search_and_order_toll_free_numbers(  # noqa
                                 quantity=quantity,
                                 pattern=pattern,
                                 siteid=site_id,
                                 name='SendHub Customer: {}'.format(user_id),
            )
        except BandwidthOrderPendingException as order_id:
            logging.warning('Order %i is pending for a toll-free number for '
                            'user: %i. Looks like bandwidth service is slow. '
                            'Error out for now and nightly cleanup task '
                            'will release the number.', order_id, user_id)
            raise BWTollFreeUnavailableError(
                'Toll Free Number Order Pending: ' +
                SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
            )

        except BandwidthAccountAPIException as err:
            # If we didn't get the number, throw an error
            logging.error('buy_tollfree_phone_number(): could not get '
                          'toll free number. '
                          'Throwing an error - %r.', str(err))
            raise BWTollFreeUnavailableError(
                SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
            )

        else:
            if not toll_free_numbers:
                raise BWTollFreeUnavailableError(
                    SHBandwidthClient.NUMBER_UNAVAILABLE_MSG
                )
            return self._cleanup_and_return_numbers(toll_free_numbers,
                                                    quantity)

    def in_service(self, number):
        """
            verifies if number if in service

            : returns True if number is in service
            : returns False if is not.
        """
        nat_number = phonenumber_as_e164(number)
        nat_number = self._parse_number_to_bw_format(str(nat_number), 'US')
        retval = False
        try:
            self.account_client.get_phone_number(nat_number)
            retval = True
        except BandwidthAccountAPIException as err:
            logging.info("Phone number query: %i, caused error: %r",
                         number, str(err))
            pass

        return retval

    def list_active_numbers(self, site_id=None, size=None):
        """
            Fetches the list of all in service numbers
            : returns lazy enumerator to the numbers - handles
                      pagination
        """
        site_id = site_id if site_id else settings.BW_SITE_ID
        try:
            numbers = self.account_client.list_phone_numbers(
                site_id=site_id,
                size=size
            )
        except BandwidthAccountAPIException as err:
            logging.info("List Phone number query: caused error: {}",
                         str(err))
            raise

        return numbers

    def get_active_number_count(self, site_id=None):
        """
            Fetches the count of numbers for a given site
        """
        site_id = site_id if site_id else settings.BW_SITE_ID
        try:
            count = self.account_client.get_phone_number_count(
                site_id=site_id
            )
        except BandwidthAccountAPIException as e:
            logging.info("Active Phone number query, caused error: {}".
                         format(e))
            raise

        return count

    def get_siteinfo_for_number(self, phone_number):
        """
            Fetches the site_id and site name that is attached to the phone
            number and returns a dictionary: {'Id': <id>, 'Name': <name>}
        """
        if validatePhoneNumber(phone_number, False) is False:
            raise ValueError("Invalid phone number ({}) passed".
                             format(phone_number))

        try:
            site_info = self.account_client.get_siteinfo_for_number(
                phone_number
            )
        except BandwidthAccountAPIException as e:
            logging.info("Site info for Phone number {}, caused error: {}".
                         format(phone_number, e))
            raise

        return json.loads(json.dumps(site_info))

    def get_number_info(self, phone_number):
        """
            Fetches the site_id and site name that is attached to the phone
            number. This method returns an object with information as follows:

            {u'Status': u'Inservice',
             u'VendorId': u'67',
             u'LastModified': u'2019-03-28T17:13:32.000Z',
             u'FullNumber': u'8334095439',
             u'Site': {u'Id': u'21391', u'Name': u'Test Environments'},
             u'MessagingSettings': {u'SmsEnabled': u'true', u'A2pState': u'system_default'},  # noqa
             u'SipPeer': {u'PeerId': u'568351', u'IsDefaultPeer': u'false', u'PeerName': u'Test Dev Environment'},  # noqa
             u'VendorName': u'Toll free vendor',
             u'AccountId': u'5004525'}
        """
        if validatePhoneNumber(phone_number, False) is False:
            raise ValueError("Invalid phone number ({}) passed".
                             format(phone_number))

        try:
            number_info = self.account_client.get_phone_number(
                phone_number
            )
        except BandwidthAccountAPIException as e:
            logging.info("Number info for Phone number {}, caused error: {}".
                         format(phone_number, e))
            raise

        return json.loads(json.dumps(number_info))

    def get_media(self, url, out_filename=None, raw_data=False):
        """
            fetches media file that was part of a MMS.
            returns out filename or None if unable to

            :set raw_data to True if requires reading data in memory
        """
        if not raw_data:
            if not out_filename:
                out_filename = os.path.join(settings.BW_MMS_DIRECTORY,
                                            url.split('/')[-1])

            if not os.path.isdir(os.path.dirname(out_filename)):
                raise ValueError('Invalid output directory: {} - '
                                 'unable to download MMS'.
                                 format(os.path.dirname(out_filename)))

            if os.path.isfile(out_filename):
                logging.info('filename {}, already exists - will be '
                             'overwritten.....'.format(out_filename))

        try:
            resp = requests.get(url, auth=(self.token, self.secret))
        except requests.exceptions.RequestException as e:
            logging.info('Error while fetching media: {}'.format(e))
            return

        if resp.status_code == requests.codes.ok:
            try:
                if raw_data:
                    return resp.content
                else:
                    with open(out_filename, 'wb') as fd:
                        fd.write(resp.content)

                    return out_filename
            except Exception as e:
                logging.info('Error: {} while writing file: {}'.
                             format(e, out_filename))
                return

        logging.info('Invalid URI or an error occured, response: {}, '
                     'response content: {}'.format(resp.status_code,
                                                   resp.text))


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
