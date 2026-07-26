"""Unit tests for ``sh_util.text.case``.

Converts between camelCase and snake_case, including recursive dict-key
rewriting in both directions. Only *keys* are converted — values pass through
untouched.

Two asymmetries are pinned: ``dict_keys_to_snake_case`` dispatches on exact
``type()`` (so a float raises rather than passing through) and mutates the dict it
is given, whereas ``dict_keys_to_camel_case`` uses ``isinstance``, builds new
containers, and guards against reference cycles.
"""

import pytest
from sh_util.text.case import (
    camel_to_snake,
    dict_keys_to_camel_case,
    dict_keys_to_snake_case,
    snake_to_camel,
)


class _WithToDict:
    def to_dict(self):
        return {"a_b": 1, "cD": 2}


class TestCamelToSnake:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("snakesOnAPlane", "snakes_on_a_plane"),
            ("SnakesOnAPlane", "snakes_on_a_plane"),
            ("snakes_on_a_plane", "snakes_on_a_plane"),
            ("IPhoneHysteria", "i_phone_hysteria"),
            ("iPhoneHysteria", "i_phone_hysteria"),
            ("GUltraNestedCamelCAse", "g_ultra_nested_camel_c_ase"),
        ],
    )
    def test_documented_conversions(self, value, expected):
        assert camel_to_snake(value) == expected

    def test_empty_string(self):
        assert camel_to_snake("") == ""

    def test_already_lowercase_is_unchanged(self):
        assert camel_to_snake("plain") == "plain"

    def test_digits_are_treated_as_word_characters(self):
        assert camel_to_snake("field1Value") == "field1_value"


class TestSnakeToCamel:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("snake_case", "snakeCase"),
            ("a_b_c", "aBC"),
            ("plain", "plain"),
            ("", ""),
            ("field_1", "field1"),
        ],
    )
    def test_conversions(self, value, expected):
        assert snake_to_camel(value) == expected

    def test_round_trip_for_simple_names(self):
        assert snake_to_camel(camel_to_snake("someKeyName")) == "someKeyName"


class TestDictKeysToSnakeCase:
    def test_converts_top_level_keys(self):
        assert dict_keys_to_snake_case({"aCamelCaseKey": "aCamelValue"}) == {"a_camel_case_key": "aCamelValue"}

    def test_values_are_left_alone(self):
        assert dict_keys_to_snake_case({"aKey": "aCamelValue"})["a_key"] == "aCamelValue"

    def test_converts_nested_keys(self):
        result = dict_keys_to_snake_case({"c": {"dNestedCamelCaseKey": "dNeCa", "e": "e"}})
        assert result == {"c": {"d_nested_camel_case_key": "dNeCa", "e": "e"}}

    def test_walks_into_lists(self):
        assert dict_keys_to_snake_case([{"aB": 1}]) == [{"a_b": 1}]

    def test_mutates_the_input_dict_in_place(self):
        original = {"aB": 1}
        dict_keys_to_snake_case(original)
        assert original == {"a_b": 1}

    @pytest.mark.parametrize("value", ["aB", 1, True, None])
    def test_scalars_pass_through(self, value):
        assert dict_keys_to_snake_case(value) == value

    def test_objects_exposing_to_dict_are_converted(self):
        assert dict_keys_to_snake_case(_WithToDict()) == {"a_b": 1, "c_d": 2}

    def test_float_is_rejected(self):
        # Dispatch is on exact type, and float is not in the scalar list, so it
        # falls all the way through to the unsupported-type guard.
        with pytest.raises(Exception, match="unsupported type"):
            dict_keys_to_snake_case(1.5)

    def test_empty_dict(self):
        assert dict_keys_to_snake_case({}) == {}


class TestDictKeysToCamelCase:
    def test_converts_top_level_keys(self):
        assert dict_keys_to_camel_case({"snake_key": 1}) == {"snakeKey": 1}

    def test_converts_nested_keys(self):
        assert dict_keys_to_camel_case({"outer_key": {"inner_key": 1}}) == {"outerKey": {"innerKey": 1}}

    def test_does_not_mutate_the_input(self):
        original = {"snake_key": 1}
        dict_keys_to_camel_case(original)
        assert original == {"snake_key": 1}

    def test_none_returns_none(self):
        assert dict_keys_to_camel_case(None) is None

    @pytest.mark.parametrize("value", ["a_b", 1, 1.5, True])
    def test_primitives_pass_through(self, value):
        assert dict_keys_to_camel_case(value) == value

    def test_non_string_keys_are_preserved(self):
        assert dict_keys_to_camel_case({1: "v"}) == {1: "v"}

    def test_lists_are_walked(self):
        assert dict_keys_to_camel_case([{"a_b": 1}, {"c_d": 2}]) == [{"aB": 1}, {"cD": 2}]

    def test_tuples_stay_tuples(self):
        assert dict_keys_to_camel_case(({"a_b": 1},)) == ({"aB": 1},)

    def test_sets_become_lists(self):
        assert sorted(dict_keys_to_camel_case({"a_b", "c_d"})) == ["a_b", "c_d"]

    def test_objects_exposing_to_dict_are_converted(self):
        assert dict_keys_to_camel_case(_WithToDict()) == {"aB": 1, "cD": 2}

    def test_unknown_objects_are_stringified(self):
        assert dict_keys_to_camel_case(object()).startswith("<object")

    def test_circular_dict_is_replaced_with_a_marker(self):
        cyclic = {"self_ref": None}
        cyclic["self_ref"] = cyclic
        assert dict_keys_to_camel_case(cyclic) == {"selfRef": "<circular_reference>"}

    def test_circular_list_is_replaced_with_a_marker(self):
        cyclic = [1]
        cyclic.append(cyclic)
        assert dict_keys_to_camel_case(cyclic) == [1, "<circular_reference>"]

    def test_shared_but_acyclic_references_are_not_flagged(self):
        # The seen-set is unwound after each branch, so the same dict appearing
        # twice side by side must convert twice rather than trip the guard.
        shared = {"a_b": 1}
        assert dict_keys_to_camel_case([shared, shared]) == [{"aB": 1}, {"aB": 1}]

    def test_empty_dict(self):
        assert dict_keys_to_camel_case({}) == {}
