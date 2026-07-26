"""Unit tests for the helpers defined directly in ``sh_util.text``.

``ensure_ascii`` strips accents via NFKD normalisation, ``toSingleLine`` collapses
whitespace (used to tidy multi-line SQL), and ``stringify`` recursively converts
numbers to strings without touching booleans.
"""

import pytest
from sh_util.text import ensure_ascii, stringify, toSingleLine


class TestEnsureAscii:
    def test_plain_ascii_is_unchanged(self):
        assert ensure_ascii("hello") == "hello"

    def test_accents_are_decomposed_and_stripped(self):
        assert ensure_ascii("café") == "cafe"

    def test_multiple_accented_characters(self):
        assert ensure_ascii("Ünïcôdé") == "Unicode"

    def test_unmappable_characters_are_dropped(self):
        assert ensure_ascii("emoji 🎉 here") == "emoji  here"

    def test_empty_string(self):
        assert ensure_ascii("") == ""

    @pytest.mark.parametrize("value", [None, 42, b"bytes", ["list"]])
    def test_non_strings_pass_through_untouched(self, value):
        assert ensure_ascii(value) is value


class TestToSingleLine:
    def test_collapses_newlines_and_indentation(self):
        assert toSingleLine("SELECT *\n  FROM t\n  WHERE x = 1") == "SELECT * FROM t WHERE x = 1"

    def test_strips_leading_and_trailing_whitespace(self):
        assert toSingleLine("  padded  ") == "padded"

    def test_collapses_runs_of_spaces(self):
        assert toSingleLine("a     b") == "a b"

    def test_tabs_are_whitespace_too(self):
        assert toSingleLine("a\t\tb") == "a b"

    def test_already_single_line_is_unchanged(self):
        assert toSingleLine("a b c") == "a b c"

    def test_whitespace_only_becomes_empty(self):
        assert toSingleLine("  \n\t ") == ""


class TestStringify:
    def test_int_becomes_a_string(self):
        assert stringify(5) == "5"

    def test_float_becomes_a_string(self):
        assert stringify(2.5) == "2.5"

    def test_booleans_are_preserved(self):
        # bool is an int subclass, so this is an explicit carve-out.
        assert stringify(True) is True
        assert stringify(False) is False

    def test_strings_pass_through(self):
        assert stringify("already") == "already"

    def test_none_passes_through(self):
        assert stringify(None) is None

    def test_dict_values_are_converted(self):
        assert stringify({"a": 1, "b": 2.5}) == {"a": "1", "b": "2.5"}

    def test_numeric_dict_keys_are_converted(self):
        assert stringify({1: "v"}) == {"1": "v"}

    def test_string_keys_are_left_alone(self):
        assert stringify({"k": 1}) == {"k": "1"}

    def test_lists_are_converted_elementwise(self):
        assert stringify([1, 2.5, "x", True]) == ["1", "2.5", "x", True]

    def test_nested_structures(self):
        assert stringify({"a": [1, {"b": 2}]}) == {"a": ["1", {"b": "2"}]}

    def test_input_is_not_mutated(self):
        original = {"a": 1, "nested": [2]}
        stringify(original)
        assert original == {"a": 1, "nested": [2]}

    def test_returns_a_real_list_not_a_map(self):
        assert isinstance(stringify([1]), list)

    def test_tuples_are_left_alone(self):
        # Only dicts and lists are recursed into.
        assert stringify((1, 2)) == (1, 2)

    def test_empty_containers(self):
        assert stringify({}) == {}
        assert stringify([]) == []
