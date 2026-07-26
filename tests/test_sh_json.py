"""Unit tests for ``sh_util.sh_json``.

``encode`` normalises a structure before handing it to simplejson: it replaces
cyclic references with a placeholder, stringifies mapping keys, turns datetimes
into epoch-millisecond strings, and falls back to ``repr()`` for anything else.
"""

import datetime

import pytest
import simplejson
from sh_util.sh_json import _normalize, default_encoder, encode


class TestDefaultEncoder:
    def test_naive_datetime_becomes_epoch_millis_string(self):
        assert default_encoder(datetime.datetime(2020, 1, 1)) == "1577836800000"

    def test_date_is_supported(self):
        assert default_encoder(datetime.date(2020, 1, 1)) == "1577836800000"

    def test_microseconds_contribute_millis(self):
        assert default_encoder(datetime.datetime(2020, 1, 1, 0, 0, 0, 500000)) == "1577836800500"

    def test_timezone_offset_is_normalised_to_utc(self):
        aware = datetime.datetime(2020, 1, 1, 12, tzinfo=datetime.timezone(datetime.timedelta(hours=5)))
        naive = datetime.datetime(2020, 1, 1, 12)
        # The +05:00 wall time is five hours earlier in UTC than the same naive clock.
        assert int(default_encoder(aware)) == int(default_encoder(naive)) - 5 * 3600 * 1000

    def test_objects_without_timetuple_are_listified(self):
        assert default_encoder(iter([1, 2, 3])) == [1, 2, 3]

    def test_non_iterable_object_raises(self):
        # ``encode`` relies on this raising so it can fall back to repr().
        class Opaque:
            pass

        with pytest.raises(TypeError):
            default_encoder(Opaque())


class TestEncodeScalars:
    @pytest.mark.parametrize("value", [None, True, False, 0, 1, -3, 2.5, "text", ""])
    def test_scalars_round_trip(self, value):
        assert simplejson.loads(encode(value)) == value

    def test_returns_a_string(self):
        assert isinstance(encode({"a": 1}), str)


class TestEncodeContainers:
    def test_dict(self):
        assert simplejson.loads(encode({"a": 1, "b": [2, 3]})) == {"a": 1, "b": [2, 3]}

    def test_list(self):
        assert simplejson.loads(encode([1, "two", None])) == [1, "two", None]

    def test_tuples_become_lists(self):
        assert simplejson.loads(encode((1, (2, 3)))) == [1, [2, 3]]

    def test_sets_are_sorted_for_determinism(self):
        assert simplejson.loads(encode({3, 1, 2})) == [1, 2, 3]

    def test_mapping_keys_are_stringified(self):
        assert simplejson.loads(encode({1: "a", None: "b"})) == {"1": "a", "None": "b"}

    def test_deeply_nested_structure(self):
        payload = {"a": [{"b": (1, 2)}, {3, 4}]}
        assert simplejson.loads(encode(payload)) == {"a": [{"b": [1, 2]}, [3, 4]]}

    def test_bytes_are_expanded_to_byte_values(self):
        # bytes is excluded from the Sequence branch, so it reaches the default
        # encoder and is listified into its integer byte values.
        assert simplejson.loads(encode(b"hi")) == [104, 105]

    def test_empty_containers(self):
        assert simplejson.loads(encode({"d": {}, "l": [], "s": set()})) == {"d": {}, "l": [], "s": []}


class TestEncodeCycles:
    def test_self_referential_dict(self):
        cyclic = {}
        cyclic["self"] = cyclic
        assert simplejson.loads(encode(cyclic)) == {"self": "<CIRCULAR>"}

    def test_self_referential_list(self):
        cyclic = [1]
        cyclic.append(cyclic)
        assert simplejson.loads(encode(cyclic)) == [1, "<CIRCULAR>"]

    def test_mutually_referential_structures(self):
        left, right = {}, {}
        left["right"] = right
        right["left"] = left
        assert simplejson.loads(encode(left)) == {"right": {"left": "<CIRCULAR>"}}

    def test_repeated_sibling_is_not_mistaken_for_a_cycle(self):
        # The seen-set is unwound on the way back out, so the same object
        # appearing twice side by side must serialise twice.
        shared = {"k": "v"}
        assert simplejson.loads(encode([shared, shared])) == [{"k": "v"}, {"k": "v"}]


class TestEncodeFallbacks:
    def test_unknown_object_falls_back_to_repr(self):
        class Weird:
            def __repr__(self):
                return "<W>"

        assert simplejson.loads(encode(Weird())) == "<W>"

    def test_datetime_inside_a_container(self):
        assert simplejson.loads(encode({"d": datetime.datetime(2020, 1, 1)})) == {"d": "1577836800000"}

    def test_normalize_without_a_default_encoder_uses_repr(self):
        class Weird:
            def __repr__(self):
                return "<W>"

        assert _normalize(Weird()) == "<W>"

    def test_normalize_seeds_its_own_seen_set(self):
        assert _normalize({"a": 1}) == {"a": 1}
