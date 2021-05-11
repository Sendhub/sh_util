"""stripe utils """
__author__ = 'brock'
# pylint: disable=C0415,E1101,E0611,W0212
import ast
import stripe as _stripe


def stripe_object_to_dict(stripe_obj):
    """converts stripe object to python dictionary """
    from stripe import StripeObject

    def _serialize(_o):
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
    """converts python dictionary to stripe object"""
    data_dict = ast.literal_eval(data)

    stripe_obj = _stripe.convert_to_stripe_object(data_dict, _stripe.api_key)
    return stripe_obj
