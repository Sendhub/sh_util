__author__ = 'brock'

import ast
import stripe as _stripe

def stripe_object_to_dict(stripeObj):

    from stripe import StripeObject

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

    stripeObj = _stripe.convert_to_stripe_object(dataDict,
                                               _stripe.api_key)

    return stripeObj