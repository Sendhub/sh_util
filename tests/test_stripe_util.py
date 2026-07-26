"""Unit tests for ``sh_util.stripe_util``.

``stripe_object_to_dict`` walks ``stripe_obj._values`` (the set of attribute
names Stripe considers "set" on the object) and recursively serializes any
nested ``StripeObject``/list values. It's built against the classic
stripe-python API where ``StripeObject`` tracked its keys in a `_values` set;
the version pinned here doesn't populate that set itself, so tests build
``StripeObject`` instances and set ``._values`` by hand.

``dict_to_stripe_object`` defers to ``stripe_util.stripe_service`` (the
*other* ``stripe_util`` — the vendored submodule at ``utils/stripe_util``,
not this file). That package isn't on the import path from here, so the
``ImportError`` re-raise is exercised for real; the success path is covered
by injecting a fake module into ``sys.modules``.
"""

import sys
import types

import pytest
from stripe._stripe_object import StripeObject

from sh_util.stripe_util import dict_to_stripe_object, stripe_object_to_dict


def _stripe_object(**values):
    obj = StripeObject.construct_from({}, "sk_test_x")
    for key, value in values.items():
        object.__setattr__(obj, key, value)
    obj._values = set(values.keys())
    return obj


class TestStripeObjectToDict:
    def test_flat_object_becomes_a_plain_dict(self):
        obj = _stripe_object(id="cus_123", name="Bob")
        assert stripe_object_to_dict(obj) == {"id": "cus_123", "name": "Bob"}

    def test_nested_stripe_object_is_recursively_converted(self):
        child = _stripe_object(id="addr_1", city="Reno")
        parent = _stripe_object(id="cus_123", address=child)
        assert stripe_object_to_dict(parent) == {
            "id": "cus_123",
            "address": {"id": "addr_1", "city": "Reno"},
        }

    def test_list_of_stripe_objects_is_recursively_converted(self):
        item1 = _stripe_object(id="li_1")
        item2 = _stripe_object(id="li_2")
        parent = _stripe_object(id="inv_1", lines=[item1, item2])
        assert stripe_object_to_dict(parent) == {
            "id": "inv_1",
            "lines": [{"id": "li_1"}, {"id": "li_2"}],
        }

    def test_list_of_plain_values_passes_through_unchanged(self):
        parent = _stripe_object(id="cus_1", tags=["a", "b", "c"])
        assert stripe_object_to_dict(parent) == {"id": "cus_1", "tags": ["a", "b", "c"]}

    def test_plain_scalar_values_pass_through_unchanged(self):
        obj = _stripe_object(id="cus_1", amount=500, active=True, metadata=None)
        assert stripe_object_to_dict(obj) == {"id": "cus_1", "amount": 500, "active": True, "metadata": None}

    def test_empty_object_becomes_an_empty_dict(self):
        assert stripe_object_to_dict(_stripe_object()) == {}


class TestDictToStripeObject:
    def test_missing_stripe_service_dependency_raises_a_descriptive_import_error(self):
        # The failed `import stripe_util.stripe_service` below caches a namespace-package
        # stand-in for `stripe_util` in sys.modules (since utils/ is on sys.path, `stripe_util`
        # resolves as a namespace package rooted one directory above the real vendored
        # submodule). Left in place, that stale entry breaks the *real* stripe_util
        # submodule's own tests later in the same process, so it's popped in `finally`.
        try:
            with pytest.raises(ImportError, match="stripe_util.stripe_service is required"):
                dict_to_stripe_object("{'id': 'cus_1'}")
        finally:
            for name in [key for key in sys.modules if key == "stripe_util" or key.startswith("stripe_util.")]:
                del sys.modules[name]

    def test_parses_and_delegates_to_the_stripe_service_when_available(self, monkeypatch):
        sentinel = object()
        captured = {}

        class FakeStripeAPICloverService:
            def convert_to_stripe_object(self, data_dict):
                captured["data_dict"] = data_dict
                return sentinel

        fake_stripe_util = types.ModuleType("stripe_util")
        fake_stripe_service = types.ModuleType("stripe_util.stripe_service")
        fake_stripe_service.StripeAPICloverService = FakeStripeAPICloverService
        monkeypatch.setitem(sys.modules, "stripe_util", fake_stripe_util)
        monkeypatch.setitem(sys.modules, "stripe_util.stripe_service", fake_stripe_service)

        result = dict_to_stripe_object("{'id': 'cus_1', 'amount': 500}")

        assert result is sentinel
        assert captured["data_dict"] == {"id": "cus_1", "amount": 500}
