"""
Stripe utilities module.

This module provides utility functions for working with Stripe objects, including
conversion between Stripe objects and Python dictionaries.
"""

__author__ = "brock"

import ast


def stripe_object_to_dict(stripe_obj):
    """
    Converts a Stripe object to a Python dictionary.

    This function recursively serializes Stripe objects into Python dictionaries,
    ensuring that nested Stripe objects and lists are properly handled.

    Args:
        stripe_obj: The Stripe object to convert.

    Returns:
        dict: A Python dictionary representation of the Stripe object.
    """
    # Import moved here to avoid missing package dependency
    from stripe import StripeObject

    def _serialize(_o):
        # Checking the type of object and serializing accordingly.
        if isinstance(_o, StripeObject):
            return stripe_object_to_dict(_o)
        if isinstance(_o, list):
            return [_serialize(i) for i in _o]
        return _o

    _d = dict()
    for k in sorted(stripe_obj._values):
        _v = getattr(stripe_obj, k)
        _v = _serialize(_v)
        _d[k] = _v
    return _d


def dict_to_stripe_object(data):
    """
    Converts a Python dictionary to a Stripe object.

    This function parses a dictionary string into a dictionary and uses the
    Stripe API service to convert it into a Stripe object.

    Args:
        data (str): The dictionary string to convert.

    Returns:
        StripeObject: The resulting Stripe object.
    """

    # Import moved here to avoid hard dependency at import time.
    try:
        from stripe_util.stripe_service import StripeAPICloverService
    except ImportError as exc:
        raise ImportError("stripe_util.stripe_service is required for dict_to_stripe_object") from exc

    # Parsing the dictionary string into a Python dictionary.
    data_dict = ast.literal_eval(data)

    # Using the Stripe API service to convert the dictionary to a Stripe object.
    obj = StripeAPICloverService()
    stripeObj = obj.convert_to_stripe_object(data_dict)

    return stripeObj
