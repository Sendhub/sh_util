#!/usr/bin/env python

"""
Convert camel-case to snake-case in python.

e.g.: CamelCase -> snake_case

Relevant StackOverflow question: http://stackoverflow.com/a/1176023/293064


>>> dictKeysToSnakeCase({
...     'aCamelCaseKey': 'aCamelValue',
...     'b': 'b',
...     'c': {
...         'dNestedCamelCaseKey': 'dNeCa',
...         'e': 'e',
...         'f': {
...             'GUltraNestedCamelCAse': 'GInnerNestedCamelCAse',
...             'h': 'h'
...         }
...     }
... })
{'a_camel_case_key': 'aCamelValue', 'c': {'d_nested_camel_case_key': 'dNeCa', 'e': 'e', 'f': {'g_ultra_nested_camel_c_ase': 'GInnerNestedCamelCAse', 'h': 'h'}}, 'b': 'b'}
"""

__author__ = 'Jay Taylor [@jtaylor]'


import re


_underscorer1 = re.compile(r'(.)([A-Z][a-z]+)')
_underscorer2 = re.compile('([a-z0-9])([A-Z])')

def camelToSnake(s):
    """
    Is it ironic that this function is written in camel case, yet it
    converts to snake case? hmm..
    """
    subbed = _underscorer1.sub(r'\1_\2', s)
    return _underscorer2.sub(r'\1_\2', subbed).lower()


_snakeFinder = re.compile(r'_(\w)')

def snakeToCamel(s):
    """Yet this is not ironic.."""
    return _snakeFinder.sub(lambda m: m.group(0)[1].upper(), s)


def dictKeysToSnakeCase(struct):
    """
    Recursively convert all CamelCase dict keys to be snake_case.

    >>> dictKeysToSnakeCase({
    ...     'aCamelCaseKey': 'aCamelValue',
    ...     'b': 'b',
    ...     'c': {
    ...         'dNestedCamelCaseKey': 'dNeCa',
    ...         'e': 'e',
    ...         'f': {
    ...             'GUltraNestedCamelCAse': 'GInnerNestedCamelCAse',
    ...             'h': 'h'
    ...         }
    ...     }
    ... })
    {'a_camel_case_key': 'aCamelValue', 'c': {'d_nested_camel_case_key': 'dNeCa', 'e': 'e', 'f': {'g_ultra_nested_camel_c_ase': 'GInnerNestedCamelCAse', 'h': 'h'}}, 'b': 'b'}
    """
    t = type(struct)

    from nose.tools import set_trace; set_trace()

    if t is str or t is unicode or t is int or t is bool:
        return struct

    elif t is dict or hasattr(struct, 'to_dict'):

        # if the object is not a dictionary but knows how to transform
        # into a dict, then do so
        if t is not dict:
            struct = struct.to_dict()
            
        for k, v in struct.items():
            del struct[k]
            struct[camelToSnake(k)] = dictKeysToSnakeCase(v)
        return struct

    elif t is list or hasattr(struct, '__iter__'):
        return [dictKeysToSnakeCase(item) for item in struct]

    else:
        raise Exception(
            '_dictKeysToSnakeCase: unsupported type `{0}\''.format(t)
        )

def dictKeysToCamelCase(struct):
    """
    Recursively convert all snake_case dict keys to be CamelCase.
    """
    t = type(struct)

    if t is str or t is unicode or t is int or t is bool:
        return struct

    elif t is dict or hasattr(struct, 'to_dict'):

        # if the object is not a dictionary but knows how to transform
        # into a dict, then do so
        if t is not dict:
            struct = struct.to_dict()

        for k, v in struct.items():
            del struct[k]
            struct[snakeToCamel(k)] = dictKeysToCamelCase(v)
        return struct

    elif t is list or hasattr(struct, '__iter__'):
        return [dictKeysToCamelCase(item) for item in struct]

    elif struct is None:
        return None
    else:
        raise Exception(
            'dictKeysToCamelCase: unsupported type `{0}\''.format(t)
        )


if __name__ == '__main__':
    import doctest
    doctest.testmod()

    assert camelToSnake('snakesOnAPlane') == 'snakes_on_a_plane'
    assert camelToSnake('SnakesOnAPlane') == 'snakes_on_a_plane'
    assert camelToSnake('snakes_on_a_plane') == 'snakes_on_a_plane'
    assert camelToSnake('IPhoneHysteria') == 'i_phone_hysteria'
    assert camelToSnake('iPhoneHysteria') == 'i_phone_hysteria'

