import sys
import os
import logging
import unittest
import inspect
import datetime
from bandwidth.account import BandwidthAccountAPIException

try:
    from sh_util.tel import AreaCodeUnavailableError
except:
    sys.path.append('/opt/sendhub/inforeach/app')
    from sh_util.tel import AreaCodeUnavailableError

from bw_util import SHBandwidthClient, BWTollFreeUnavailableError
from bw_util import phonenumber_as_e164

SUCCESS_AREA_CODE = '919'
FAILURE_AREA_CODE = '212'

ENABLE_BW_DEBUGGING = os.getenv('ENABLE_BW_DEBUGGING', False) == 'True'


def is_local_devenv():
    return os.getenv('PERSONAL_DEV_ENV', False) == 'True'


def verify_orderinfo(bw_client, order_id):
    return bw_client.get_phoneorder_info(order_id)


class BandwidthAccountHelpers:
    def __init__(self):
        self.bw_client = SHBandwidthClient(debug=ENABLE_BW_DEBUGGING)

    def _buy_phonenumber_with_areacode(self, area_code, country_code='US'):
        search_number = None
        try:
            search_number = self.bw_client.buy_phone_number(
                area_code=area_code,
                country_code=country_code
            )
            logging.info("available number: {}".format(search_number))
        except AreaCodeUnavailableError as e:
            logging.info("Exception as {}".format(e))

        return search_number

    def _buy_phonenumber_with_phonenum(self, phone_num):
        bought_number = None
        try:
            bought_number = self.bw_client.buy_phone_number(
                phone_number=phone_num
            )
            logging.info("available number: {}".format(bought_number))
        except AreaCodeUnavailableError as e:
            logging.info("Exception as {}".format(e))

        return bought_number

    def _search_phonenumber_with_areacode(self, area_code, quantity=1):
        """ searches for one phone number within given area code """
        search_number = None
        try:
            search_number = self.bw_client.find_number_in_area_code(
                area_code=area_code,
                quantity=quantity
            )
        except AreaCodeUnavailableError as e:
            logging.info("Exception as {}".format(e))

        return search_number

    def _buy_tollfree_phonenumber(self, quantity=1):
        search_number = None
        try:
            search_number = self.bw_client.buy_toll_free_number(
                quantity=quantity
            )
            logging.info("available number: {}".format(search_number))
        except BWTollFreeUnavailableError as e:
            logging.info("Exception as {}".format(e))

        return search_number

    def _search_tollfree_phonenumber(self, quantity=1):
        search_number = None
        try:
            search_number = self.bw_client.search_available_toll_free_number(
                pattern='8**',
                quantity=quantity
            )
            logging.info("available numbers: {}".format(search_number))
        except BWTollFreeUnavailableError as e:
            logging.info("Exception as {}".format(e))

        return search_number

    def _delete_phonenumber(self, phone_no, re_raise=True):
        logging.info("Deleting phonenumber: {}".format(phone_no))
        try:
            self.bw_client.release_phone_number(phone_no)
        except ValueError as e:
            logging.info("Received ValueError exception: {}:{}".
                         format(e, phone_no))
            if re_raise:
                raise
            else:
                pass
        except BandwidthAccountAPIException as e:
            logging.info("Received BandwidthAccountAPIException "
                         "exception: {}:{}".format(e, phone_no))
            if re_raise:
                raise
            else:
                pass

    def _delete_phonenum_list(self, cleanup_list, re_raise=True):
        for phonenum in cleanup_list:
            self._delete_phonenumber(phonenum, re_raise=re_raise)


class BandwidthAccountTestCases(unittest.TestCase):
    INVALID_NUMBER = ['', 0, 1234, 34567, '89101112ab']
    NOT_BW_NUMBER = ['+19254797926', '+14087036579', '(408)703-6579', 4087036579]  # noqa`
    FOREIGN_NUMBERS = ['+44 1509 813888', '+91 9999999999', 9199999999999]

    def setUp(self):
        self.helper = BandwidthAccountHelpers()

    def tearDown(self):
        pass

    def test_search_invalid_qty_phone_number(self):
        self.assertRaises(ValueError,
                          self.helper._search_phonenumber_with_areacode,
                          area_code=SUCCESS_AREA_CODE,
                          quantity=-1)

    def test_search_one_phone_number(self):
        number = self.helper._search_phonenumber_with_areacode(
            area_code=SUCCESS_AREA_CODE, quantity=1
        )
        logging.info("Numbers: {}".format(number))
        self.assertIsNotNone(number)

    def test_search_multiple_phone_numbers(self):
        quantity = 10
        number = self.helper._search_phonenumber_with_areacode(
            area_code=SUCCESS_AREA_CODE,
            quantity=quantity
        )
        logging.info("Numbers: {}".format(number))
        self.assertIsNotNone(number)
        self.assertIsInstance(number, list)
        self.assertEqual(len(number), quantity)

    def test_search_invalid_qty_tollfree_number(self):
        self.assertRaises(ValueError,
                          self.helper._search_tollfree_phonenumber,
                          quantity=-1)

    def test_search_one_tollfree_number(self):
        number = self.helper._search_tollfree_phonenumber(
            quantity=1
        )
        self.assertIsNotNone(number)

    def test_search_multiple_tollfree_numbers(self):
        quantity = 4
        number = self.helper._search_tollfree_phonenumber(
            quantity=quantity
        )
        self.assertIsNotNone(number)
        self.assertIsInstance(number, list)
        self.assertEqual(len(number), quantity)

    def test_search_and_buy_phonenumber(self):
        search_number = self.helper._search_phonenumber_with_areacode(
            area_code=SUCCESS_AREA_CODE
        )
        self.assertIsNotNone(search_number)
        logging.info("searched phone number: {}, going to buy".
                     format(search_number))
        bought_number = self.helper._buy_phonenumber_with_phonenum(
            search_number
        )
        self.assertIsNotNone(bought_number)
        logging.info("Bought number: {}".format(bought_number))

        if bought_number:
            self.assertIsNone(self.helper._delete_phonenumber(bought_number))

    def test_buying_deleting_phonenumbers(self):
        # test buying phone number with success
        phonenum = self.helper._buy_phonenumber_with_areacode(
            area_code=SUCCESS_AREA_CODE
        )
        self.assertIsNotNone(phonenum)
        if phonenum:
            self.assertIsNone(self.helper._delete_phonenumber(phonenum))
        # test buying phone number with failure
        phonenum = self.helper._buy_phonenumber_with_areacode(
            area_code=FAILURE_AREA_CODE
        )
        self.assertFalse(phonenum)
        if phonenum:
            self.assertIsNone(self.helper._delete_phonenumber(phonenum))

    def test_buy_delete_one_tollfree_number(self):
        toll_free = self.helper._buy_tollfree_phonenumber(quantity=1)
        self.assertIsNotNone(toll_free)
        if toll_free:
            self.assertIsNone(self.helper._delete_phonenumber(toll_free))

    def test_buy_delete_multiple_tollfree_number(self):
        quantity = 2
        toll_free_nums = self.helper._buy_tollfree_phonenumber(
            quantity=quantity
        )
        self.assertIsNotNone(toll_free_nums)
        self.assertIsInstance(toll_free_nums, list)
        self.assertEqual(len(toll_free_nums), quantity)

        if toll_free_nums:
            for num in toll_free_nums:
                self.assertIsNone(self.helper._delete_phonenumber(num))

    def test_delete_invalid_number(self):
        for number in self.INVALID_NUMBER:
            self.assertRaises(ValueError,
                              self.helper._delete_phonenumber,
                              number,
                              re_raise=True)

    def test_delete_foreign_number(self):
        for number in self.FOREIGN_NUMBERS:
            self.assertRaises(ValueError,
                              self.helper._delete_phonenumber,
                              number,
                              re_raise=True)

    def test_delete_notbw_number(self):
        for number in self.NOT_BW_NUMBER:
            self.assertRaises(BandwidthAccountAPIException,
                              self.helper._delete_phonenumber,
                              number,
                              re_raise=True)

    def test_invalid_country_code(self):
        self.assertRaises(ValueError,
                          self.helper._buy_phonenumber_with_areacode,
                          area_code='186',
                          country_code='IND')


class BWMessagingTestCases(unittest.TestCase):
    # provide a valid BW number
    VALID_BW_NUMBER = '+18334095439'

    # this is test service, will respond with generic message
    VALID_TO_NUMBER = '+15072003115'
    # use any number that does not belong to BW
    # below is my sendhub number
    INVALID_BW_NUMBER = '+14087036579'

    def setUp(self):
        # self.bw_client = SHBandwidthClient()
        self.bw_client = SHBandwidthClient(debug=ENABLE_BW_DEBUGGING)

    def tearDown(self):
        pass

    def test_success_sms(self):
        """ verifies only message is sent - if issued msgid """
        msg_id = self.bw_client.send_sms(
            from_number=self.VALID_BW_NUMBER,
            to_number=self.VALID_TO_NUMBER,
            msg='{}-{}'.format(inspect.stack()[0][3],
                               datetime.datetime.now())
        )
        self.assertIsNotNone(msg_id)

    def test_success_mms(self):
        pass

    def test_sms_invalid_number(self):
        self.assertIsNotNone(self.bw_client.send_sms(
            from_number=self.INVALID_BW_NUMBER,
            to_number=self.VALID_TO_NUMBER,
            msg='{}-{}'.format(inspect.stack()[0][3],
                               datetime.datetime.now())
        ))

    def test_mms_invalid_number(self):
        pass

    def test_group_sms_success(self):
        pass

    def test_group_mms_success(self):
        pass

    def test_group_sms_fail(self):
        pass

    def test_group_mms_fail(self):
        pass

    def test_msg_info(self):
        self.assertRaises(NotImplementedError,
                          self.bw_client.get_message_info,
                          'dummy')


class PhoneNumberTestCases(unittest.TestCase):
    INVALID_NUMBER = ['', 0, 1234, 34567, '89101112ab']

    def test_invalid_numbers(self):
        for number in self.INVALID_NUMBER:
            self.assertRaises(ValueError,
                              phonenumber_as_e164,
                              number)


class PhoneNumberListAllTestCases(unittest.TestCase):

    def setUp(self):
        self.bw_client = SHBandwidthClient(debug=ENABLE_BW_DEBUGGING)

    def tearDown(self):
        pass

    def test_list_numbers_size1(self):
        try:
            numbers = self.bw_client.list_active_numbers(size=1)
        except Exception as e:
            self.fail('exception unexpectedly: {}'.format(e))
        else:
            for number in numbers:
                logging.info("number: {}".format(number))

    def test_list_numbers_size2(self):
        try:
            numbers = self.bw_client.list_active_numbers(size=2)
        except Exception as e:
            self.fail('exception unexpectedly: {}'.format(e))
        else:
            for number in numbers:
                logging.info("number: {}".format(number))

    def test_list_numbers_size500(self):
        try:
            numbers = self.bw_client.list_active_numbers(size=500)
        except Exception as e:
            self.fail('exception unexpectedly: {}'.format(e))
        else:
            for number in numbers:
                logging.info("number: {}".format(number))

    def test_list_numbers_size1000(self):
        try:
            numbers = self.bw_client.list_active_numbers(size=1000)
        except Exception as e:
            self.fail('exception unexpectedly: {}'.format(e))
        else:
            for number in numbers:
                logging.info("number: {}".format(number))

    def test_list_numbers_sizedefault(self):
        try:
            numbers = self.bw_client.list_active_numbers()
        except Exception as e:
            self.fail('exception unexpectedly: {}'.format(e))
        else:
            for number in numbers:
                logging.info("number: {}".format(number))

    def test_list_numbers_invalidsite(self):
        # in this case bandwidth returns 0 numbers
        try:
            numbers = self.bw_client.list_active_numbers(site_id=1)
        except Exception as e:
            self.fail('exception unexpectedly: {}'.format(e))
        else:
            self.assertEqual(sum(1 for x in numbers), 0)


class PhoneNumberInServiceTestCases(unittest.TestCase):
    INVALID_NUMBERS = ['', 0, 1234, 34567, '89101112ab', ]
    NOT_BW_NUMBERS = ['+14087036579', '(408)703-6579']
    BW_NUMBERS = ['(833) 409-5439', '+18334095439']

    def setUp(self):
        self.bw_client = SHBandwidthClient()

    def tearDown(self):
        pass

    def test_invalid_numbers(self):
        for number in self.INVALID_NUMBERS:
            self.assertRaises(ValueError,
                              self.bw_client.in_service,
                              number)

    def test_not_bw_numbers(self):
        for number in self.NOT_BW_NUMBERS:
            self.assertFalse(self.bw_client.in_service(number))

    def test_bw_numbers(self):
        for number in self.BW_NUMBERS:
            self.assertTrue(self.bw_client.in_service(number))


class PhoneNumberCountTestCases(unittest.TestCase):
    def setUp(self):
        self.bw_client = SHBandwidthClient()

    def tearDown(self):
        pass

    def test_count(self):
        try:
            count = self.bw_client.get_active_number_count()
        except Exception as e:
            self.fail("Unexpected exception: {}".format(e))
        else:
            logging.info("Count: {}".format(count))

        self.assertGreaterEqual(count, 0)

    def test_count_invalid_site(self):
        self.assertRaises(BandwidthAccountAPIException,
                          self.bw_client.get_active_number_count,
                          site_id=1)


class GetSiteInfoTestCases(unittest.TestCase):
    def setUp(self):
        self.bw_client = SHBandwidthClient()

    def tearDown(self):
        pass

    def test_siteid(self):
        # get valid number
        numbers = self.bw_client.list_active_numbers(size=1)
        number = next(numbers)

        try:
            siteinfo = self.bw_client.get_siteinfo_for_number(number)
        except Exception as e:
            self.fail("Unexpected exception: {}".format(e))
        else:
            logging.info("site information: {}".format(siteinfo))

    def test_siteid_invalid_number(self):
        self.assertRaises(ValueError,
                          self.bw_client.get_siteinfo_for_number,
                          'hello')

    def test_siteid_notbw_number(self):
        self.assertRaises(BandwidthAccountAPIException,
                          self.bw_client.get_siteinfo_for_number,
                          '+19254797926')


class GetNumberInfoTestCases(unittest.TestCase):
    def setUp(self):
        self.bw_client = SHBandwidthClient(debug=ENABLE_BW_DEBUGGING)

    def tearDown(self):
        pass

    def test_phone_info(self):
        # get valid number
        numbers = self.bw_client.list_active_numbers(size=1)
        number = next(numbers)

        try:
            info = self.bw_client.get_number_info(number)
        except Exception as e:
            self.fail("Unexpected exception: {}".format(e))
        else:
            logging.info("number information: {}".format(info))

    def test_phoneinfo_invalid_number(self):
        self.assertRaises(ValueError,
                          self.bw_client.get_number_info,
                          'hello')

    def test_phoneinfo_notbw_number(self):
        self.assertRaises(BandwidthAccountAPIException,
                          self.bw_client.get_number_info,
                          '+19254797926')


class DownloadMediaTestCases(unittest.TestCase):
    """
       tests valid/invalid cases for downloading MMS file.
    """
    def setUp(self):
        self.bw_client = SHBandwidthClient(debug=ENABLE_BW_DEBUGGING)

    # call this proper URI
    def download(self, url):
        self.bw_client.get_media(url)

    def test_download_empty_uri(self):
        self.assertIsNone(self.bw_client.get_media(''))

    def test_download_invalid_uri(self):
        self.assertIsNone(self.bw_client.get_media('https://messaging.bandwidth.com/api/v2/users/5004525/media/abcself.jpg'))  # noqa

    def test_download_invalid_hostdir(self):
        self.assertRaises(ValueError,
                          self.bw_client.get_media,
                          '',
                          '/home/ubuntu/aaaabbbbbccccssss/')


# for independently testing delete test cases
class BandwidthTollFreeSimpleTestCase:
    def setUp(self):
        self.bw_client = SHBandwidthClient()

    def tearDown(self):
        pass

    def __init__(self):
        self.helper = BandwidthAccountHelpers()

    def test_search_one_tfnumber(self):
        number = self.helper._search_tollfree_phonenumber(
            quantity=1
        )
        logging.info("Found Toll Free Number: {}".format(number))


# for independently testing delete test cases
class BandwidthDeleteCases:
    INVALID_NUMBER = ['', 0, 1234, 34567, '89101112ab']
    NOT_BW_NUMBER = ['+19254797926', '+14087036579', '(408)703-6579', 4087036579]  # noqa
    FOREIGN_NUMBERS = ['+44 1509 813888', '+91 9999999999', 9199999999999]

    # Ensure that this is valid list
    DELETE_LIST = []

    def __init__(self):
        self.helper = BandwidthAccountHelpers()

    def _test_delete_number(self, num_list):
        for number in num_list:
            try:
                self.helper._delete_phonenumber(number)
                logging.info("Deleted {} from account".format(number))
            except ValueError as e:
                logging.info('Received ValueError exception: {}:{}'.
                             format(e, number))
            except BandwidthAccountAPIException as e:
                logging.info('Received BandwidthAccountAPIException '
                             'exception: {}:{}'.format(e, number))

    def test_delete_invalid_numbers(self):
        self._test_delete_number(self.INVALID_NUMBER)
        self._test_delete_number(self.NOT_BW_NUMBER)
        self._test_delete_number(self.FOREIGN_NUMBERS)

    def test_delete_valid_numbers(self):
        self._test_delete_number(self.DELETE_LIST)

    def run(self):
        self.test_delete_invalid_numbers()
        logging.info("=====================================")
        self.test_delete_valid_numbers()


class BandwidthDeleteNumber:
    def __init__(self, number):
        self.helper = BandwidthAccountHelpers()
        try:
            self.helper._delete_phonenumber(number)
            logging.info("Deleted {} from account".format(number))
        except ValueError as e:
            logging.info('Received ValueError exception: {}:{}'.
                         format(e, number))
        except BandwidthAccountAPIException as e:
            logging.info('Received BandwidthAccountAPIException '
                         'exception: {}:{}'.format(e, number))
            raise


class VerifyOrders:
    order_list = ['1ea6057d-d97a-453e-b22c-c09dea403da8',
                  '161aec20-4758-4336-b0e0-0e2ffc270cc1',
                  '9da5eb20-0a14-4d83-ae76-2f6441a56d75']

    def __init__(self):
        self.bw_client = SHBandwidthClient()
        for orderid in self.order_list:
            verify_orderinfo(self.bw_client, orderid)


class BuyDeleteLocalPhoneNumber:
    def __init__(self):
        self.helper = BandwidthAccountHelpers()

    def __call__(self):
        logging.info("Starting Buying and then deleting test")
        phonenum = self.helper._buy_phonenumber_with_areacode(
            area_code=SUCCESS_AREA_CODE
        )
        assert(phonenum != None)  # noqa
        logging.info("Bought number: {}".format(phonenum))

        if phonenum:
            self.helper._delete_phonenumber(phonenum)
        logging.info("Released phone number: {}".format(phonenum))


class SearchBuyPhoneNumber:
    def __init__(self):
        self.helper = BandwidthAccountHelpers()

    def search_and_buy_and_delete(self):
        search_number = self.helper._search_phonenumber_with_areacode(
            area_code=SUCCESS_AREA_CODE
        )
        assert(search_number != None)  # noqa
        logging.info("searched phone number: {}, going to buy".
                     format(search_number))
        bought_number = self.helper._buy_phonenumber_with_phonenum(
            search_number
        )
        assert(bought_number != None)  # noqa
        logging.info("Bought number: {}".format(bought_number))

        if bought_number:
            self.helper._delete_phonenumber(bought_number)


def run_specific_tests():
    """ runs specific tests in suites defined in test classes """
    # test_classes_to_run = [BWMessagingTestCases]
    test_classes_to_run = [DownloadMediaTestCases]
    loader = unittest.TestLoader()
    runner = unittest.TextTestRunner()
    test_list = []
    for test_cls in test_classes_to_run:
        test = loader.loadTestsFromTestCase(test_cls)
        test_list.append(test)

    test_suite = unittest.TestSuite(test_list)
    results = runner.run(test_suite)
    logging.info("results of tests: {}".format(results))


def run_all_tests():
    unittest.main()

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))

    # VerifyOrders()
    # BandwidthDeleteCases().run()
    # SearchBuyPhoneNumber().search_and_buy_and_delete()
    # BuyDeleteLocalPhoneNumber()()

    # run_specific_tests()
    run_all_tests()
    # BandwidthTollFreeSimpleTestCase().test_search_one_tfnumber()
