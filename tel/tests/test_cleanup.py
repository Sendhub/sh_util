# test_phone_utils.py
import unittest

from sh_util.tel import validatePhoneNumber, validatePhoneNumberByCountry


class TestValidatePhoneNumberByCountry(unittest.TestCase):
    def test_valid_us_number(self):
        self.assertTrue(validatePhoneNumberByCountry("+12025550123", "US"))

    def test_valid_puerto_rico(self):
        self.assertTrue(validatePhoneNumberByCountry("+17872345678", "US"))

    def test_valid_canadian_number_in_us_group(self):
        self.assertTrue(validatePhoneNumberByCountry("+14165550123", "US"))

    def test_valid_us_number_in_ca_group(self):
        self.assertTrue(validatePhoneNumberByCountry("+12025550123", "CA"))

    def test_valid_australian_number(self):
        self.assertTrue(validatePhoneNumberByCountry("+61491570156", "AU"))

    def test_invalid_number_wrong_region(self):
        self.assertFalse(validatePhoneNumberByCountry("+61491570156", "US"))  # AU number in US region group

    def test_invalid_number_format(self):
        self.assertFalse(validatePhoneNumberByCountry("123456", "US"))

    def test_invalid_number_none(self):
        self.assertFalse(validatePhoneNumberByCountry(None, "US"))

    def test_invalid_country_none(self):
        self.assertFalse(validatePhoneNumberByCountry("+12025550123", None))

    def test_invalid_country_code(self):
        self.assertFalse(validatePhoneNumberByCountry("+12025550123", "XX"))  # Not in REGION_GROUPS

    def test_number_as_bytes(self):
        self.assertTrue(validatePhoneNumberByCountry(b"+12025550123", "US"))

    def test_bytes_invalid_format(self):
        self.assertFalse(validatePhoneNumberByCountry(b"123456", "US"))

    def test_empty_string(self):
        self.assertFalse(validatePhoneNumberByCountry("", "US"))

    def test_all_us_territories(self):
        territories = {
            "PR": "+17872345678",  # Puerto Rico
            "GU": "+16712345678",  # Guam
            "MP": "+16702345678",  # Northern Mariana Islands
        }
        for territory, number in territories.items():
            with self.subTest(territory=territory):
                self.assertTrue(validatePhoneNumberByCountry(number, "US"))


class TestValidatePhoneNumber(unittest.TestCase):
    def test_valid_us_number(self):
        self.assertTrue(validatePhoneNumber("+12025550123"))  # Washington, DC

    def test_valid_ca_number(self):
        self.assertTrue(validatePhoneNumber("+14165550123"))  # Toronto, Canada

    def test_valid_au_number(self):
        self.assertTrue(validatePhoneNumber("+61412345678"))  # Mobile, AU

    def test_valid_us_territories(self):
        territory_numbers = {
            "PR": "+17872011234",  # Puerto Rico
            "GU": "+16712345678",  # Guam
            "MP": "+16702345678",  # Northern Mariana Islands
            "UM": "+16892345678",  # Minor Outlying Islands (uses same range as others)
        }

        for name, number in territory_numbers.items():
            with self.subTest(territory=name):
                self.assertTrue(validatePhoneNumber(number))

    def test_valid_shortcodes(self):
        for shortcode in ["123", "4567", "54321", "123456"]:
            with self.subTest(shortcode=shortcode):
                self.assertTrue(validatePhoneNumber(shortcode, allowShortcode=True))

    def test_invalid_shortcodes_when_disallowed(self):
        for shortcode in ["123", "4567", "54321"]:
            with self.subTest(shortcode=shortcode):
                self.assertFalse(validatePhoneNumber(shortcode, allowShortcode=False))

    def test_invalid_number(self):
        self.assertFalse(validatePhoneNumber("+99999999999"))  # Not valid anywhere

    def test_empty_number(self):
        self.assertFalse(validatePhoneNumber(""))

    def test_none_number(self):
        self.assertFalse(validatePhoneNumber(None))

    def test_bytes_number(self):
        self.assertTrue(validatePhoneNumber(b"+12025550123"))

    def test_number_with_spaces_and_symbols(self):
        self.assertTrue(validatePhoneNumber("+1 (202) 555-0123"))  # US with symbols

    def test_invalid_format(self):
        self.assertFalse(validatePhoneNumber("abcdefghijk"))


if __name__ == "__main__":
    unittest.main()
