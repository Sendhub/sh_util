# test_phone_utils.py
import unittest

from sh_util.tel import validate_phone_number, validate_phone_number_by_country
from sh_util.tel.cleanup import (
    cleanup_phone_number,
    display_number,
    is_special_twilio_number,
    is_toll_free_number,
)


class TestValidatePhoneNumberByCountry(unittest.TestCase):
    def test_valid_us_number(self):
        self.assertTrue(validate_phone_number_by_country("+12025550123", "US"))

    def test_valid_puerto_rico(self):
        self.assertTrue(validate_phone_number_by_country("+17872345678", "US"))

    def test_valid_canadian_number_in_us_group(self):
        self.assertTrue(validate_phone_number_by_country("+14165550123", "US"))

    def test_valid_us_number_in_ca_group(self):
        self.assertTrue(validate_phone_number_by_country("+12025550123", "CA"))

    def test_valid_australian_number(self):
        self.assertTrue(validate_phone_number_by_country("+61491570156", "AU"))

    def test_invalid_number_wrong_region(self):
        self.assertFalse(validate_phone_number_by_country("+61491570156", "US"))  # AU number in US region group

    def test_invalid_number_format(self):
        self.assertFalse(validate_phone_number_by_country("123456", "US"))

    def test_invalid_number_none(self):
        self.assertFalse(validate_phone_number_by_country(None, "US"))

    def test_invalid_country_none(self):
        self.assertFalse(validate_phone_number_by_country("+12025550123", None))

    def test_invalid_country_code(self):
        self.assertFalse(validate_phone_number_by_country("+12025550123", "XX"))  # Not in REGION_GROUPS

    def test_bytes_undecodable_returns_false(self):
        self.assertFalse(validate_phone_number_by_country(b"\xff\xfe", "US"))

    def test_unparseable_number_raises_caught_and_returns_false(self):
        self.assertFalse(validate_phone_number_by_country("not-a-number-at-all", "US"))

    def test_number_as_bytes(self):
        self.assertTrue(validate_phone_number_by_country(b"+12025550123", "US"))

    def test_bytes_invalid_format(self):
        self.assertFalse(validate_phone_number_by_country(b"123456", "US"))

    def test_empty_string(self):
        self.assertFalse(validate_phone_number_by_country("", "US"))

    def test_all_us_territories(self):
        territories = {
            "PR": "+17872345678",  # Puerto Rico
            "GU": "+16712345678",  # Guam
            "MP": "+16702345678",  # Northern Mariana Islands
        }
        for territory, number in territories.items():
            with self.subTest(territory=territory):
                self.assertTrue(validate_phone_number_by_country(number, "US"))


class TestValidatePhoneNumber(unittest.TestCase):
    def test_valid_us_number(self):
        self.assertTrue(validate_phone_number("+12025550123"))  # Washington, DC

    def test_valid_ca_number(self):
        self.assertTrue(validate_phone_number("+14165550123"))  # Toronto, Canada

    def test_valid_au_number(self):
        self.assertTrue(validate_phone_number("+61412345678"))  # Mobile, AU

    def test_valid_us_territories(self):
        territory_numbers = {
            "PR": "+17872011234",  # Puerto Rico
            "GU": "+16712345678",  # Guam
            "MP": "+16702345678",  # Northern Mariana Islands
            "UM": "+16892345678",  # Minor Outlying Islands (uses same range as others)
        }

        for name, number in territory_numbers.items():
            with self.subTest(territory=name):
                self.assertTrue(validate_phone_number(number))

    def test_valid_shortcodes(self):
        for shortcode in ["123", "4567", "54321", "123456"]:
            with self.subTest(shortcode=shortcode):
                self.assertTrue(validate_phone_number(shortcode, allow_shortcode=True))

    def test_invalid_shortcodes_when_disallowed(self):
        for shortcode in ["123", "4567", "54321"]:
            with self.subTest(shortcode=shortcode):
                self.assertFalse(validate_phone_number(shortcode, allow_shortcode=False))

    def test_invalid_number(self):
        self.assertFalse(validate_phone_number("+99999999999"))  # Not valid anywhere

    def test_empty_number(self):
        self.assertFalse(validate_phone_number(""))

    def test_none_number(self):
        self.assertFalse(validate_phone_number(None))

    def test_bytes_number(self):
        self.assertTrue(validate_phone_number(b"+12025550123"))

    def test_number_with_spaces_and_symbols(self):
        self.assertTrue(validate_phone_number("+1 (202) 555-0123"))  # US with symbols

    def test_invalid_format(self):
        self.assertFalse(validate_phone_number("abcdefghijk"))


class TestCleanupPhoneNumber(unittest.TestCase):
    def test_shortcode_is_left_unchanged(self):
        for shortcode in ["123", "4567", "54321", "123456"]:
            with self.subTest(shortcode=shortcode):
                self.assertEqual(cleanup_phone_number(shortcode), shortcode)

    def test_us_number_formatted_to_e164(self):
        self.assertEqual(cleanup_phone_number("2025550123", region="US"), "+12025550123")

    def test_au_prefixed_number_switches_region(self):
        result = cleanup_phone_number("+61491570156", region="US")
        self.assertEqual(result, "+61491570156")


class TestIsSpecialTwilioNumber(unittest.TestCase):
    def test_known_special_numbers_return_true(self):
        for number in ["+7378742833", "+2562533", "+8656696", "+266696687", ""]:
            with self.subTest(number=number):
                self.assertTrue(is_special_twilio_number(number))

    def test_regular_number_returns_false(self):
        self.assertFalse(is_special_twilio_number("+12025550123"))


class TestDisplayNumber(unittest.TestCase):
    def test_us_number_national_format(self):
        result = display_number("+12025550123", region="US")
        self.assertIn("202", result)

    def test_au_number_international_format(self):
        result = display_number("+61491570156", region="AU")
        self.assertTrue(result.startswith("+61"))

    def test_unparseable_number_falls_back_to_hyphenated_grouping(self):
        # A string with no digits at all fails phonenumbers.parse
        # (NumberParseException), so display_number falls back to a simple
        # 3-3-rest hyphenated grouping of the raw characters.
        result = display_number("abcdefghij", region="US")
        self.assertEqual(result, "abc-def-ghij")

    def test_unparseable_short_string_does_not_hit_index_error_branch(self):
        # Python string slicing never raises IndexError (out-of-range slices
        # just yield shorter/empty strings), so the `except IndexError:
        # formatted_number = number` fallback inside display_number's
        # NumberParseException handler is effectively unreachable dead code -
        # a too-short unparseable string still produces a (sparser) hyphenated
        # grouping rather than the original value.
        result = display_number("ab", region="US")
        self.assertEqual(result, "ab--")


class TestIsTollFreeNumber(unittest.TestCase):
    def test_toll_free_number_returns_true(self):
        self.assertTrue(is_toll_free_number("+18005551234"))

    def test_regular_number_returns_false(self):
        self.assertFalse(is_toll_free_number("+12025550123"))

    def test_unparseable_number_returns_false(self):
        self.assertFalse(is_toll_free_number("not-a-number"))


if __name__ == "__main__":
    unittest.main()
