"""Unit tests for ``sh_util.functional``.

Covers the sequence helpers, the three memoization decorators, and
``safer_hash``. The distributed memoizer talks to memcache, so its client and
``pylibmc`` error type are stubbed rather than reached.

Two behaviours are pinned deliberately because callers rely on them:
memoized results are deep-copied on the way out (so callers cannot corrupt the
cache), and non-kwarg functions have their keyword arguments dropped rather than
forwarded.
"""

import inspect
import pickle
import time as time_module
from unittest import mock

import pytest
from sh_util import functional as functional_module
from sh_util.functional import (
    Distmemoizewithexpiry,
    Memoizewithexpiry,
    curry,
    distinct,
    filterByClass,
    flatten,
    memoize,
    safer_hash,
)


class TestFlatten:
    def test_flattens_nested_lists(self):
        assert list(flatten([1, [2, [3, [4]]], 5])) == [1, 2, 3, 4, 5]

    def test_empty_input(self):
        assert list(flatten([])) == []

    def test_already_flat_input_is_unchanged(self):
        assert list(flatten([1, 2, 3])) == [1, 2, 3]

    def test_strings_are_treated_as_scalars(self):
        # Strings are iterable, so without the explicit guard this would explode
        # into individual characters.
        assert list(flatten(["ab", ["cd"]])) == ["ab", "cd"]

    def test_mixed_iterable_types(self):
        assert list(flatten([(1, 2), {3}, [4]])) == [1, 2, 3, 4]

    def test_empty_nested_iterables_disappear(self):
        assert list(flatten([1, [], [[]], 2])) == [1, 2]


class TestDistinct:
    def test_removes_duplicates_preserving_first_appearance(self):
        assert distinct([3, 1, 3, 2, 1]) == [3, 1, 2]

    def test_empty_input(self):
        assert distinct([]) == []

    def test_all_unique_is_unchanged(self):
        assert distinct(["a", "b"]) == ["a", "b"]

    def test_works_on_a_generator(self):
        assert distinct(x % 3 for x in range(10)) == [0, 1, 2]


class TestFilterByClass:
    def test_keeps_only_matching_instances(self):
        assert filterByClass(int, [1, "a", 2, None]) == [1, 2]

    def test_bools_count_as_ints(self):
        assert filterByClass(int, [True, 1]) == [True, 1]

    def test_accepts_a_class_tuple(self):
        assert filterByClass((int, str), [1, "a", None]) == [1, "a"]

    def test_no_matches_returns_empty(self):
        assert filterByClass(float, [1, "a"]) == []


class TestCurry:
    def test_calling_with_all_arguments_invokes_immediately(self):
        assert curry(lambda a, b: a + b)(1, 2) == 3

    def test_partial_application(self):
        assert curry(lambda a, b: a + b)(1)(2) == 3

    def test_three_stage_currying(self):
        assert curry(lambda a, b, c: a + b + c)(1)(2)(3) == 6

    def test_partial_application_in_uneven_chunks(self):
        assert curry(lambda a, b, c: (a, b, c))(1, 2)(3) == (1, 2, 3)

    def test_explicit_argc_overrides_introspection(self):
        # *args functions report co_argcount 0, so the arity must be supplied.
        assert curry(lambda *args: sum(args), argc=2)(1, 2) == 3

    def test_zero_arity_function_is_called_at_once(self):
        assert curry(lambda: "done")() == "done"


class TestMemoize:
    def test_caches_by_arguments(self):
        calls = []

        @memoize
        def add(a, b):
            calls.append((a, b))
            return a + b

        assert add(1, 2) == 3
        assert add(1, 2) == 3
        assert calls == [(1, 2)]

    def test_distinct_arguments_are_cached_separately(self):
        @memoize
        def identity(a):
            return a

        assert identity(1) == 1
        assert identity(2) == 2
        assert len(identity._cached) == 2

    def test_result_is_deep_copied_so_callers_cannot_corrupt_the_cache(self):
        @memoize
        def build():
            return {"items": [1]}

        first = build()
        first["items"].append(2)
        assert build() == {"items": [1]}

    def test_kwargs_are_forwarded_when_the_function_accepts_them(self):
        @memoize
        def sink(a, **kw):
            return (a, kw)

        assert sink(1, flag=True) == (1, {"flag": True})

    def test_kwargs_are_dropped_when_the_function_does_not_accept_them(self):
        # The key includes the kwargs, so the call is cached under them, but the
        # wrapped function is invoked positionally only.
        @memoize
        def positional_only(a):
            return a

        assert positional_only(5, ignored="x") == 5

    def test_falls_back_to_getfullargspec_when_signature_fails(self, monkeypatch):
        def boom(_func):
            raise ValueError("no signature for you")

        monkeypatch.setattr(inspect, "signature", boom)

        @memoize
        def add(a, b):
            return a + b

        assert add(1, 2) == 3


class TestMemoizewithexpiry:
    def test_caches_within_the_ttl(self):
        calls = []

        @Memoizewithexpiry(60)
        def add(a, b):
            calls.append(1)
            return a + b

        assert add(1, 2) == 3
        assert add(1, 2) == 3
        assert len(calls) == 1

    def test_recomputes_once_the_ttl_has_elapsed(self):
        calls = []

        @Memoizewithexpiry(0)
        def now(a):
            calls.append(1)
            return a

        now(1)
        time_module.sleep(0.01)
        now(1)
        assert len(calls) == 2

    def test_result_is_deep_copied(self):
        @Memoizewithexpiry(60)
        def build():
            return {"k": [1]}

        build()["k"].append(2)
        assert build() == {"k": [1]}

    def test_kwargs_forwarded_only_when_accepted(self):
        @Memoizewithexpiry(60)
        def sink(a, **kw):
            return (a, kw)

        assert sink(1, x=2) == (1, {"x": 2})

    def test_clean_cache_evicts_entries_stamped_in_the_future(self, caplog):
        # ``_clean_cache`` compares ``cached_ts - now > ttl``, which only holds for
        # a timestamp in the future — ordinary entries are never evicted here (the
        # real expiry check lives in the wrapper). Pinning the current behaviour.
        memoizer = Memoizewithexpiry(10)
        memoizer._cached["stale"] = (time_module.time() + 10_000, "value")
        memoizer._cached["fresh"] = (time_module.time(), "value")

        memoizer._clean_cache()

        assert "stale" not in memoizer._cached
        assert "fresh" in memoizer._cached

    def test_clean_cache_runs_at_decoration_time(self):
        memoizer = Memoizewithexpiry(10)
        memoizer._cached["stale"] = (time_module.time() + 10_000, "value")

        memoizer(lambda: None)

        assert memoizer._cached == {}


class TestDistmemoizewithexpiry:
    """The distributed variant layers memcache in front of the local cache."""

    @staticmethod
    def _client(monkeypatch, get_return=None, get_error=None, set_error=None):
        client = mock.Mock()
        if get_error is not None:
            client.get.side_effect = get_error
        else:
            client.get.return_value = get_return
        if set_error is not None:
            client.set.side_effect = set_error
        monkeypatch.setattr(functional_module, "cli", lambda: client)
        return client

    def test_computes_and_stores_on_a_cache_miss(self, monkeypatch):
        client = self._client(monkeypatch, get_return=None)
        calls = []

        @Distmemoizewithexpiry(60)
        def add(a, b):
            calls.append(1)
            return a + b

        assert add(1, 2) == 3
        assert len(calls) == 1
        client.set.assert_called_once()

    def test_uses_a_fresh_memcache_hit_without_calling_the_function(self, monkeypatch):
        self._client(monkeypatch, get_return=(time_module.time(), "cached"))
        calls = []

        @Distmemoizewithexpiry(60)
        def expensive(a):
            calls.append(1)
            return "computed"

        assert expensive(1) == "cached"
        assert calls == []

    def test_stale_memcache_hit_is_discarded_and_recomputed(self, monkeypatch):
        self._client(monkeypatch, get_return=(time_module.time() - 10_000, "old"))

        @Distmemoizewithexpiry(60)
        def expensive(a):
            return "computed"

        assert expensive(1) == "computed"

    def test_memcache_get_failure_falls_back_to_computing(self, monkeypatch):
        import pylibmc

        self._client(monkeypatch, get_error=pylibmc.Error("get down"))

        @Distmemoizewithexpiry(60)
        def expensive(a):
            return "computed"

        assert expensive(1) == "computed"

    def test_memcache_set_failure_is_swallowed(self, monkeypatch):
        import pylibmc

        self._client(monkeypatch, get_return=None, set_error=pylibmc.Error("set down"))

        @Distmemoizewithexpiry(60)
        def expensive(a):
            return "computed"

        # The value is still returned from the local cache.
        assert expensive(1) == "computed"

    def test_local_cache_short_circuits_memcache(self, monkeypatch):
        client = self._client(monkeypatch, get_return=None)

        @Distmemoizewithexpiry(60)
        def expensive(a):
            return "computed"

        expensive(1)
        expensive(1)
        # Second call never reaches memcache.
        assert client.get.call_count == 1

    def test_kwargs_forwarded_only_when_accepted(self, monkeypatch):
        self._client(monkeypatch, get_return=None)

        @Distmemoizewithexpiry(60)
        def sink(a, **kw):
            return (a, kw)

        assert sink(1, x=2) == (1, {"x": 2})

    def test_result_is_deep_copied(self, monkeypatch):
        self._client(monkeypatch, get_return=None)

        @Distmemoizewithexpiry(60)
        def build():
            return {"k": [1]}

        build()["k"].append(2)
        assert build() == {"k": [1]}


class TestSaferHash:
    @pytest.mark.parametrize("value", [None, True, False, 0, 1, -5, 3.5])
    def test_scalars_are_hashable(self, value):
        assert isinstance(safer_hash(value), int)

    def test_strings_and_bytes_hash_equally_by_content(self):
        assert safer_hash(b"abc") == safer_hash(bytearray(b"abc"))

    def test_str_and_bytes_of_equal_content_collide(self):
        # Both take the "_str" branch under the same tag, and CPython hashes an
        # ASCII str and the equivalent bytes identically — so these are not
        # distinguishable by safer_hash. Pinning it so a change is deliberate.
        assert safer_hash("abc") == safer_hash(b"abc")

    def test_identical_structures_hash_the_same(self):
        assert safer_hash({"a": [1, 2]}) == safer_hash({"a": [1, 2]})

    def test_dict_key_order_does_not_matter(self):
        assert safer_hash({"a": 1, "b": 2}) == safer_hash({"b": 2, "a": 1})

    def test_set_member_order_does_not_matter(self):
        assert safer_hash({1, 2, 3}) == safer_hash({3, 2, 1})

    def test_differing_structures_hash_differently(self):
        assert safer_hash([1, 2]) != safer_hash([2, 1])

    def test_lists_and_tuples_of_equal_content_hash_alike(self):
        # Both take the Sequence branch, which does not record the concrete type.
        assert safer_hash([1, 2]) == safer_hash((1, 2))

    def test_nested_structures(self):
        assert isinstance(safer_hash({"a": [{"b": {1, 2}}, (3, 4)]}), int)

    def test_self_referential_list_does_not_recurse_forever(self):
        cyclic = [1]
        cyclic.append(cyclic)
        assert isinstance(safer_hash(cyclic), int)

    def test_self_referential_dict_does_not_recurse_forever(self):
        cyclic = {}
        cyclic["self"] = cyclic
        assert isinstance(safer_hash(cyclic), int)

    def test_hashable_object_uses_its_own_hash(self):
        class Hashable:
            def __hash__(self):
                return 4242

        assert safer_hash(Hashable()) == safer_hash(Hashable())

    def test_unhashable_object_falls_back_to_repr(self):
        class Unhashable:
            __hash__ = None

            def __repr__(self):
                return "<stable-repr>"

        assert safer_hash(Unhashable()) == safer_hash(Unhashable())

    def test_object_with_neither_hash_nor_repr_falls_back_to_id(self):
        class Hostile:
            __hash__ = None

            def __repr__(self):
                raise RuntimeError("no repr for you")

        instance = Hostile()
        # Falls back to id(), so it is stable per-object but differs per-instance.
        assert safer_hash(instance) == safer_hash(instance)

    def test_pickle_is_not_required_for_hashing(self):
        # safer_hash exists precisely because pickling arbitrary objects fails;
        # confirm it copes with something pickle cannot handle.
        unpicklable = lambda: None  # noqa: E731
        with pytest.raises((pickle.PicklingError, AttributeError, TypeError)):
            pickle.dumps(unpicklable)
        assert isinstance(safer_hash(unpicklable), int)
