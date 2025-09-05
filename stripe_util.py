__author__ = 'brock'

import ast

from stripe import StripeObject

from stripe_util.stripe_service import StripeAPIBasilService


def stripe_object_to_dict(stripeObj):

    def _serialize(o):
        if isinstance(o, StripeObject):
            return stripe_object_to_dict(o)
        if isinstance(o, list):
            return [_serialize(i) for i in o]
        return o

    d = dict()
    for k in sorted(stripeObj._values):
        v = getattr(stripeObj, k)
        v = _serialize(v)
        d[k] = v
    return d


def dict_to_stripe_object(data):

    dataDict = ast.literal_eval(data)

    # Updated to Stripe SDK 12.5.0 2025-08-27.Basil
    obj = StripeAPIBasilService()
    stripeObj = obj.convert_to_stripe_object(dataDict)

    return stripeObj
