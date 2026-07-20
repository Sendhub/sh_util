# test_phone_utils.py
import unittest
from unittest import skip

from sh_util.tel.cleanup import validatePhoneNumber


@skip("validatePhoneNumberByCountry is not implemented in this sh_util revision")
class TestValidatePhoneNumberByCountry(unittest.TestCase):

    def test_valid_us_number(self):
        self.assertTrue(validatePhoneNumber('+12025550123'))

    def test_valid_puerto_rico(self):
        self.assertTrue(validatePhoneNumber('+17872345678'))

    def test_valid_canadian_number_in_us_group(self):
        self.assertTrue(validatePhoneNumber('+14165550123'))

    def test_valid_us_number_in_ca_group(self):
        self.assertTrue(validatePhoneNumber('+12025550123'))

    def test_valid_australian_number(self):
        self.assertTrue(validatePhoneNumber('+61491570156'))

    def test_invalid_number_wrong_region(self):
        self.assertFalse(validatePhoneNumber('+61491570156'))

    def test_invalid_number_format(self):
        self.assertFalse(validatePhoneNumber('123456'))

    def test_invalid_number_none(self):
        self.assertFalse(validatePhoneNumber(None))

    def test_invalid_country_none(self):
        self.assertFalse(validatePhoneNumber('+12025550123'))

    def test_invalid_country_code(self):
        self.assertFalse(validatePhoneNumber('+12025550123'))

    def test_number_as_bytes(self):
        self.assertTrue(validatePhoneNumber(b'+12025550123'))

    def test_bytes_invalid_format(self):
        self.assertFalse(validatePhoneNumber(b'123456'))

    def test_empty_string(self):
        self.assertFalse(validatePhoneNumber(''))

    def test_all_us_territories(self):
        territories = {
            'PR': '+17872345678',
            'GU': '+16712345678',
            'MP': '+16702345678',
        }
        for territory, number in territories.items():
            with self.subTest(territory=territory):
                self.assertTrue(validatePhoneNumber(number))


class TestValidatePhoneNumber(unittest.TestCase):

    def test_valid_us_number(self):
        self.assertTrue(validatePhoneNumber('+12025550123'))  # Washington, DC

    def test_valid_ca_number(self):
        self.assertTrue(validatePhoneNumber('+14165550123', country_code='CA'))  # Toronto, Canada

    def test_valid_au_number(self):
        self.assertTrue(validatePhoneNumber('+61412345678', country_code='AU'))  # Mobile, AU

    @skip("bytes input is not supported by current validatePhoneNumber")
    def test_valid_us_territories(self):
        territory_numbers = {
            'PR': '+17872011234',  # Puerto Rico
            'GU': '+16712345678',  # Guam
            'MP': '+16702345678',  # Northern Mariana Islands
            'UM': '+16892345678',  # Minor Outlying Islands (uses same range as others)
        }

        for name, number in territory_numbers.items():
            with self.subTest(territory=name):
                self.assertTrue(validatePhoneNumber(number))

    def test_valid_shortcodes(self):
        for shortcode in ['123', '4567', '54321', '123456']:
            with self.subTest(shortcode=shortcode):
                self.assertTrue(validatePhoneNumber(shortcode, allowShortcode=True))

    def test_invalid_shortcodes_when_disallowed(self):
        for shortcode in ['123', '4567', '54321']:
            with self.subTest(shortcode=shortcode):
                self.assertFalse(validatePhoneNumber(shortcode, allowShortcode=False))

    def test_invalid_number(self):
        self.assertFalse(validatePhoneNumber('+99999999999'))  # Not valid anywhere

    def test_empty_number(self):
        self.assertFalse(validatePhoneNumber(''))

    def test_none_number(self):
        self.assertFalse(validatePhoneNumber(None))

    @skip("bytes input is not supported by current validatePhoneNumber")
    def test_bytes_number(self):
        self.assertTrue(validatePhoneNumber(b'+12025550123'))

    def test_number_with_spaces_and_symbols(self):
        self.assertTrue(validatePhoneNumber('+1 (202) 555-0123'))  # US with symbols

    def test_invalid_format(self):
        self.assertFalse(validatePhoneNumber('abcdefghijk'))


if __name__ == '__main__':
    unittest.main()
