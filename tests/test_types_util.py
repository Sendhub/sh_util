"""Unit tests for ``sh_util.types_util``.

Two quirks are pinned deliberately, because callers depend on them:
``is_number`` accepts anything ``float()`` accepts (so decimal strings pass even
though ``isdigit()`` rejects them), and ``is_integer`` guards on truthiness first,
which makes ``0`` and ``"0"``-like falsy values report False.
"""

import pytest
from sh_util.types_util import is_integer, is_number


class TestIsNumber:
    @pytest.mark.parametrize("value", [0, 1, -5, 1.5, -0.25, complex(1, 2), True, False])
    def test_numeric_types(self, value):
        assert is_number(value) is True

    @pytest.mark.parametrize("value", ["0", "123", "-4", "1.5", "+2", " 7 ", "1e3"])
    def test_numeric_strings(self, value):
        # Non-digit strings still pass via the float() fallback.
        assert is_number(value) is True

    @pytest.mark.parametrize("value", ["abc", "", "1.2.3", "12a", None, [], {}, object()])
    def test_non_numeric_values(self, value):
        assert is_number(value) is False

    def test_none_is_rejected(self):
        assert is_number(None) is False

    def test_bytes_digits_are_accepted_via_float(self):
        assert is_number(b"12") is True


class TestIsInteger:
    @pytest.mark.parametrize("value", [1, -5, 42, True])
    def test_truthy_integers(self, value):
        assert is_integer(value) is True

    @pytest.mark.parametrize("value", ["1", "123", "+456", "-456", " 789 "])
    def test_integer_strings(self, value):
        assert is_integer(value) is True

    def test_whitespace_is_stripped_before_checking(self):
        assert is_integer("  42  ") is True

    @pytest.mark.parametrize("value", ["abc", "1.5", "+", "-", "1a", "+-1"])
    def test_non_integer_strings(self, value):
        assert is_integer(value) is False

    @pytest.mark.parametrize("value", [1.5, -0.5, None, [], {}, object()])
    def test_non_integer_values(self, value):
        assert is_integer(value) is False

    @pytest.mark.parametrize("value", [0, 0.0, "", False, None, []])
    def test_falsy_values_short_circuit_to_false(self, value):
        # The function opens with ``if maybe_num:``, so every falsy input returns
        # False — including the integer 0, which *is* an integer.
        assert is_integer(value) is False

    def test_zero_as_a_string_is_recognised(self):
        # "0" is truthy as a string, so it reaches the isdigit() check, unlike
        # the integer 0 above.
        assert is_integer("0") is True

    def test_float_string_is_rejected_even_when_whole(self):
        assert is_integer("5.0") is False
