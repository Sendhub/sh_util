import unittest
from mock import patch
import settings
from sh_util.tel import BuyPhoneNumberFromCarrier, \
    ReleaseNumberSafely, FindPhoneNumberInAreaCode, \
    SHBandwidthClient


class TelUtilTestCases(unittest.TestCase):
    """
       unit tests all the functions defined in the file
       tel_utils.py
    """
    @patch('sh_util.tel.tel_util.twilioBuyPhoneNumber')
    def test_buy_number_areacode_twilio(self, mock_twilio_buy):
        """
           tests that when trying to buy a phone number from
           twilio provider, appropriate function is called with
           appropriate parameters.

           based on area code
        """
        dummy_sid = 'dummysid'
        area_code = '919'
        BuyPhoneNumberFromCarrier()(settings.SMS_GATEWAY_TWILIO,
                                    dummy_sid, area_code=area_code)
        mock_twilio_buy.assert_called_with(
            twilioClient=settings.TWILIO_CLIENT,
            appSid=dummy_sid,
            areaCode=area_code,
            countryCode='US',
            phoneNumber=None
        )

    @patch('sh_util.tel.tel_util.twilioBuyPhoneNumber')
    def test_buy_number_twilio(self, mock_twilio_buy):
        """
           tests that when trying to buy a phone number from
           twilio provider, appropriate function is called with
           appropriate parameters.

           buy actual phone number
        """
        dummy_sid = 'dummysid'
        area_code = '919'
        phone_number = '+19193456789'
        BuyPhoneNumberFromCarrier()(settings.SMS_GATEWAY_TWILIO,
                                    dummy_sid, area_code=area_code,
                                    phone_number=phone_number)
        mock_twilio_buy.assert_called_with(
            twilioClient=settings.TWILIO_CLIENT,
            appSid=dummy_sid,
            areaCode=area_code,
            countryCode='US',
            phoneNumber=phone_number
        )

    @patch.object(SHBandwidthClient, 'buy_phone_number')
    @patch.object(SHBandwidthClient, 'buy_toll_free_number')
    def test_buy_number_areacode_bw(self, mock_buy_tf_number, mock_buy_number):
        """
           tests that when trying to buy a phone number from
           bandwidth provider, appropriate function is called with
           appropriate parameters.

           based on area code
        """
        dummy_sid = 'dummysid'
        area_code = '919'
        phone_number = '+19193456789'
        dummy_user = 'dummy_user'
        mock_buy_tf_number.return_value = '+18331234567'
        mock_buy_number.return_value = phone_number

        BuyPhoneNumberFromCarrier()(settings.SMS_GATEWAY_BANDWIDTH,
                                    dummy_sid, area_code=area_code,
                                    phone_number=phone_number,
                                    toll_free=False, user=dummy_user)

        mock_buy_number.assert_called_with(
            phone_number=phone_number,
            area_code=area_code,
            user_id=dummy_user,
            country_code='US'
        )

    @patch.object(SHBandwidthClient, 'buy_phone_number')
    @patch.object(SHBandwidthClient, 'buy_toll_free_number')
    def test_buy_number_bw(self, mock_buy_tf_number, mock_buy_number):
        """
           tests that when trying to buy a phone number from
           bandwidth provider, appropriate function is called with
           appropriate parameters.

           based on area code
        """
        dummy_sid = 'dummysid'
        area_code = '919'
        phone_number = '+19193456789'
        dummy_user = 'dummy_user'
        mock_buy_tf_number.return_value = '+18331234567'
        mock_buy_number.return_value = phone_number

        BuyPhoneNumberFromCarrier()(settings.SMS_GATEWAY_BANDWIDTH,
                                    dummy_sid, area_code=area_code,
                                    phone_number=phone_number,
                                    toll_free=False, user=dummy_user)

        mock_buy_number.assert_called_with(
            phone_number=phone_number,
            area_code=area_code,
            user_id=dummy_user,
            country_code='US'
        )

    @patch.object(SHBandwidthClient, 'buy_phone_number')
    @patch.object(SHBandwidthClient, 'buy_toll_free_number')
    def test_buy_tf_number_bw(self, mock_buy_tf_number, mock_buy_number):
        """
           tests that when trying to buy a phone number from
           bandwidth provider, appropriate function is called with
           appropriate parameters.

           based on area code
        """
        dummy_sid = 'dummysid'
        area_code = '919'
        phone_number = '+19193456789'
        dummy_user = 'dummy_user'
        mock_buy_tf_number.return_value = '+18331234567'
        mock_buy_number.return_value = phone_number

        BuyPhoneNumberFromCarrier()(settings.SMS_GATEWAY_BANDWIDTH,
                                    dummy_sid, area_code=area_code,
                                    phone_number=phone_number,
                                    toll_free=True, user=dummy_user)

        mock_buy_tf_number.assert_called_with(
            user_id=dummy_user
        )

    @patch.object(SHBandwidthClient, 'buy_phone_number')
    @patch.object(SHBandwidthClient, 'buy_toll_free_number')
    @patch('sh_util.tel.tel_util.twilioBuyPhoneNumber')
    def test_buy_number_invalid(self, mock_twilio_buy,
                                mock_bw_tf_buy, mock_bw_buy):
        """
           tests invalid gateway when buying a number
           should return an error.
        """
        dummy_sid = 'dummysid'
        area_code = '919'
        phone_number = '+19193456789'
        dummy_user = 'dummy_user'
        mock_bw_tf_buy.return_value = '+18331234567'
        mock_bw_buy.return_value = phone_number
        mock_twilio_buy.return_value = phone_number

        self.assertIsNone(BuyPhoneNumberFromCarrier()(
            'invalid-gateway',
            dummy_sid,
            area_code=area_code,
            phone_number=phone_number,
            toll_free=True,
            user=dummy_user)
        )

        mock_bw_tf_buy.assert_not_called()
        mock_twilio_buy.assert_not_called()
        mock_bw_buy.assert_not_called()

    @patch.object(ReleaseNumberSafely, '_twilio_safe_number_release')
    def test_release_number_twilio(self, mock_twilio_release):
        """
           tests that when trying to release a phone number from
           twilio provider, appropriate function is called with
           appropriate parameters.
        """
        mock_twilio_release.return_value = True

        dummy_sid = 'dummysid'
        phone_number = '+19193456789'
        self.assertTrue(ReleaseNumberSafely(phone_number,
                                            settings.SMS_GATEWAY_TWILIO,
                                            dummy_sid)())

        mock_twilio_release.assert_called_once()

    @patch.object(SHBandwidthClient, 'release_phone_number')
    def test_release_number_bw(self, mock_bw_release):
        """
           tests that when trying to release a phone number from
           bandwidth provider, appropriate function is called with
           appropriate parameters.
        """
        mock_bw_release.return_value = True
        dummy_sid = 'dummysid'
        phone_number = '+19193456789'
        self.assertTrue(ReleaseNumberSafely(phone_number,
                                            settings.SMS_GATEWAY_BANDWIDTH,
                                            dummy_sid)())

        mock_bw_release.assert_called_once()

    @patch.object(SHBandwidthClient, 'release_phone_number')
    @patch.object(ReleaseNumberSafely, '_twilio_safe_number_release')
    def test_release_number_invalid(self, mock_twilio, mock_bw):
        """
           tests that when trying to release a phone number from
           invalid provider
        """
        mock_twilio.return_value = True
        mock_bw.return_value = True
        dummy_sid = 'dummysid'
        phone_number = '+19193456789'
        self.assertFalse(ReleaseNumberSafely(phone_number,
                                             'invalid-gateway',
                                             dummy_sid)())
        mock_twilio.assert_not_called()
        mock_bw.assert_not_called()

    @patch('sh_util.tel.tel_util.twilioFindNumberInAreaCode')
    def test_avail_number_twilio(self, mock_twilio):
        """
           tests that when trying to lookup for available phone numbers
           from twilio provider, appropriate function is called with
           appropriate parameters.

           based on area code
        """
        area_code = '919'
        return_value = ['+19191002000', '+19192003000']
        mock_twilio.return_value = return_value
        self.assertEqual(FindPhoneNumberInAreaCode()(
            settings.SMS_GATEWAY_TWILIO,
            area_code=area_code), return_value)
        mock_twilio.assert_called_with(
            twilioClient=settings.TWILIO_CLIENT,
            areaCode=area_code,
            countryCode='US'
        )

    @patch.object(SHBandwidthClient, 'find_number_in_area_code')
    def test_avail_number_bw(self, mock_bw):
        """
           tests that when trying to lookup for available phone numbers
           from bandwidth provider, appropriate function is called with
           appropriate parameters.

           based on area code
        """
        import random
        area_code = '919'
        self.maxDiff = None
        for quantity in range(10):
            if quantity == 1:
                return_value = '+1919{0:07d}'.format(
                    random.randint(0, 9999999)
                )
                exp_return_value = [return_value]
            else:
                return_value = []
                for i in range(quantity):
                    return_value.append(
                        '+1919{0:07d}'.format(random.randint(0, 9999999))
                    )
                exp_return_value = return_value

            mock_bw.return_value = return_value

            avail_numbers = FindPhoneNumberInAreaCode()(
                settings.SMS_GATEWAY_BANDWIDTH,
                area_code=area_code,
                quantity=quantity
            )

            mock_bw.assert_called_with(
                area_code=area_code,
                country_code='US',
                quantity=quantity
            )
            self.assertIsNotNone(avail_numbers)
            self.assertIsInstance(avail_numbers, list)
            self.assertEqual(quantity, len(avail_numbers))

            for i in range(quantity):
                self.assertEqual(exp_return_value[i],
                                 avail_numbers[i].phone_number)

    @patch.object(SHBandwidthClient, 'search_available_toll_free_number')
    def test_avail_tf_number_bw(self, mock_bw):
        """
           tests that when trying to lookup for available phone numbers
           from bandwidth provider, appropriate function is called with
           appropriate parameters.

           Toll Free
        """
        import random
        area_code = '919'
        self.maxDiff = None
        for quantity in range(10):
            if quantity == 1:
                return_value = '+1833{0:07d}'.format(
                    random.randint(0, 9999999)
                )
                exp_return_value = [return_value]
            else:
                return_value = []
                for i in range(quantity):
                    return_value.append(
                        '+1833{0:07d}'.format(random.randint(0, 9999999))
                    )
                exp_return_value = return_value

            mock_bw.return_value = return_value

            avail_numbers = FindPhoneNumberInAreaCode()(
                settings.SMS_GATEWAY_BANDWIDTH,
                area_code=area_code,
                quantity=quantity,
                toll_free=True
            )

            mock_bw.assert_called_with(
                quantity=quantity
            )
            self.assertIsNotNone(avail_numbers)
            self.assertIsInstance(avail_numbers, list)
            self.assertEqual(quantity, len(avail_numbers))

            for i in range(quantity):
                self.assertEqual(exp_return_value[i],
                                 avail_numbers[i].phone_number)

    @patch.object(SHBandwidthClient, 'search_available_toll_free_number')
    @patch.object(SHBandwidthClient, 'find_number_in_area_code')
    @patch('sh_util.tel.tel_util.twilioFindNumberInAreaCode')
    def test_avail_number_invalid(self, mock_twilio, mock_bw, mock_bw_tf):
        """
           tests that when trying to query available phone numbers from
           invalid provider
        """
        area_code = '919'
        return_value = ['+19191002000', '+19192003000']
        mock_twilio.return_value = return_value
        self.assertIsNone(FindPhoneNumberInAreaCode()(
            'invalid-gateway',
            area_code=area_code))

        mock_twilio.assert_not_called()
        mock_bw.assert_not_called()
        mock_bw_tf.assert_not_called()
