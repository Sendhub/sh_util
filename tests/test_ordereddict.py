"""Unit tests for ``sh_util.ordereddict.OrderedDict``.

This is a legacy insertion-ordered mapping that predates dict ordering. It
subclasses ``dict`` while tracking key order in a parallel ``_keys`` list, so the
tests focus on the two staying in sync — that list is what ``__iter__``,
``keys()``, ``popitem()`` and the sort/reverse helpers all read from.
"""

import pickle

import pytest
from sh_util.ordereddict import OrderedDict


class TestConstruction:
    def test_empty(self):
        assert list(OrderedDict().items()) == []

    def test_from_pairs_preserves_order(self):
        d = OrderedDict([("b", 2), ("a", 1), ("c", 3)])
        assert list(d.keys()) == ["b", "a", "c"]

    def test_from_kwargs(self):
        d = OrderedDict(a=1)
        assert d["a"] == 1

    def test_more_than_one_positional_arg_is_rejected(self):
        with pytest.raises(TypeError):
            OrderedDict([("a", 1)], [("b", 2)])

    def test_fromkeys_uses_default_value(self):
        d = OrderedDict.fromkeys(["x", "y"])
        assert list(d.keys()) == ["x", "y"]
        assert d["x"] is None

    def test_fromkeys_with_explicit_value(self):
        d = OrderedDict.fromkeys(["x", "y"], 0)
        assert list(d.values()) == [0, 0]


class TestOrdering:
    def test_insertion_order_is_preserved(self):
        d = OrderedDict()
        for key in "zyxw":
            d[key] = key.upper()
        assert list(d) == ["z", "y", "x", "w"]

    def test_reassigning_a_key_does_not_move_it(self):
        d = OrderedDict([("a", 1), ("b", 2)])
        d["a"] = 99
        assert list(d.keys()) == ["a", "b"]
        assert d["a"] == 99

    def test_reversed_iterates_backwards(self):
        d = OrderedDict([("a", 1), ("b", 2), ("c", 3)])
        assert list(reversed(d)) == ["c", "b", "a"]

    def test_sort_orders_keys_in_place(self):
        d = OrderedDict([("c", 3), ("a", 1), ("b", 2)])
        d.sort()
        assert list(d.keys()) == ["a", "b", "c"]
        assert list(d.values()) == [1, 2, 3]

    def test_reverse_flips_key_order_in_place(self):
        d = OrderedDict([("a", 1), ("b", 2), ("c", 3)])
        d.reverse()
        assert list(d.keys()) == ["c", "b", "a"]


class TestMutation:
    def test_delitem_drops_the_key_from_the_order(self):
        d = OrderedDict([("a", 1), ("b", 2), ("c", 3)])
        del d["b"]
        assert list(d.keys()) == ["a", "c"]
        assert "b" not in d

    def test_delitem_on_missing_key_raises(self):
        with pytest.raises(KeyError):
            del OrderedDict()["nope"]

    def test_clear_empties_both_the_dict_and_the_key_order(self):
        d = OrderedDict([("a", 1), ("b", 2)])
        d.clear()
        assert len(d) == 0
        assert list(d) == []

    def test_reusable_after_clear(self):
        d = OrderedDict([("a", 1)])
        d.clear()
        d["z"] = 26
        assert list(d.items()) == [("z", 26)]

    def test_popitem_returns_the_last_pair(self):
        d = OrderedDict([("a", 1), ("b", 2)])
        assert d.popitem() == ("b", 2)
        assert list(d.keys()) == ["a"]

    def test_popitem_on_empty_raises_keyerror(self):
        with pytest.raises(KeyError):
            OrderedDict().popitem()

    def test_pop_removes_the_key_from_the_order(self):
        d = OrderedDict([("a", 1), ("b", 2)])
        assert d.pop("a") == 1
        assert list(d.keys()) == ["b"]

    def test_setdefault_appends_only_new_keys(self):
        d = OrderedDict([("a", 1)])
        assert d.setdefault("a", 99) == 1
        assert d.setdefault("b", 2) == 2
        assert list(d.keys()) == ["a", "b"]

    def test_update_appends_in_argument_order(self):
        d = OrderedDict([("a", 1)])
        d.update([("b", 2), ("c", 3)])
        assert list(d.keys()) == ["a", "b", "c"]


class TestCopyAndRepr:
    def test_copy_preserves_order_and_is_independent(self):
        d = OrderedDict([("b", 2), ("a", 1)])
        clone = d.copy()
        assert list(clone.keys()) == ["b", "a"]
        clone["c"] = 3
        assert "c" not in d

    def test_copy_returns_the_same_class(self):
        assert isinstance(OrderedDict([("a", 1)]).copy(), OrderedDict)

    def test_repr_shows_pairs_in_order(self):
        assert repr(OrderedDict([("b", 2), ("a", 1)])) == "OrderedDict({'b': 2, 'a': 1})"

    def test_repr_of_empty(self):
        assert repr(OrderedDict()) == "OrderedDict({})"


class TestPickling:
    def test_roundtrip_preserves_order(self):
        d = OrderedDict([("c", 3), ("a", 1), ("b", 2)])
        restored = pickle.loads(pickle.dumps(d))
        assert list(restored.keys()) == ["c", "a", "b"]
        assert restored == d

    def test_reduce_excludes_the_internal_key_list(self):
        # ``_keys`` is rebuilt from the pairs on unpickling; shipping it in the
        # instance dict too would double-register every key.
        _cls, args, inst_dict = OrderedDict([("a", 1)]).__reduce__()
        assert args == ([["a", 1]],)
        assert "_keys" not in inst_dict
