"""Unit tests for ``sh_util.sh_http.request_common.extract_parameters``.

Pulls a tuple of values out of a dict-like ``query_dict`` for the given
parameter names, substituting ``empty_value`` for any that are absent.
"""

from sh_util.sh_http.request_common import extract_parameters


class TestExtractParameters:
    def test_extracts_present_parameters_in_order(self):
        query_dict = {"a": "1", "b": "2", "c": "3"}
        assert extract_parameters(query_dict, ["b", "a"]) == ("2", "1")

    def test_missing_parameter_defaults_to_none(self):
        query_dict = {"a": "1"}
        assert extract_parameters(query_dict, ["a", "missing"]) == ("1", None)

    def test_missing_parameter_uses_custom_empty_value(self):
        query_dict = {}
        assert extract_parameters(query_dict, ["x"], empty_value="") == ("",)

    def test_empty_parameters_returns_empty_tuple(self):
        assert extract_parameters({"a": "1"}, []) == ()

    def test_returns_a_tuple(self):
        assert isinstance(extract_parameters({"a": "1"}, ["a"]), tuple)

    def test_works_with_a_plain_dict_and_a_querydict_like_mapping(self):
        class FakeQueryDict(dict):
            pass

        query_dict = FakeQueryDict(a="1")
        assert extract_parameters(query_dict, ["a", "b"]) == ("1", None)
