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
{
    'a_camel_case_key': 'aCamelValue',
    'c': {
        'd_nested_camel_case_key': 'dNeCa',
        'e': 'e',
        'f': {
            'g_ultra_nested_camel_c_ase': 'GInnerNestedCamelCAse',
            'h': 'h'
        }
    },
    'b': 'b'
}
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
    {
        'a_camel_case_key': 'aCamelValue',
        'c': {
            'd_nested_camel_case_key': 'dNeCa',
            'e': 'e',
            'f': {
                'g_ultra_nested_camel_c_ase': 'GInnerNestedCamelCAse',
                'h': 'h'
                }
            },
        'b': 'b'
    }
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
            struct[camelToSnake(k)] = dictKeysToSnakeCase(v)
        return struct

    elif t is list or hasattr(struct, '__iter__'):
        return [dictKeysToSnakeCase(item) for item in struct]

    elif struct is None:
        return None

    else:
        raise Exception(
            '_dictKeysToSnakeCase: unsupported type `{0}\''.format(t)
        )


# def dictKeysToCamelCase(struct, seen=None):
#     """
#     Recursively convert all snake_case dict keys to camelCase.
#     Supports dicts, lists, objects with .to_dict(), and primitives.
#     Handles circular references safely.
#     """
#     if seen is None:
#         seen = set()

#     # Avoid infinite recursion for circular references
#     obj_id = id(struct)
#     if obj_id in seen:
#         return "<circular_reference>"
#     seen.add(obj_id)

#     if struct is None:
#         return None

#     if isinstance(struct, (str, int, float, bool)):
#         return struct

#     if isinstance(struct, dict):
#         return {snakeToCamel(k): dictKeysToCamelCase(v, seen) for k, v in struct.items()}

#     if isinstance(struct, list):
#         return [dictKeysToCamelCase(item, seen) for item in struct]

#     # If the object has a to_dict method, convert and recurse
#     if hasattr(struct, "to_dict") and callable(struct.to_dict):
#         return dictKeysToCamelCase(struct.to_dict(), seen)

#     # Unknown type
#     return str(struct)  # fallback: convert to string to avoid exceptions

def dictKeysToCamelCase(struct, seen=None):
    """
    Recursively convert snake_case dict keys to camelCase.
    Uses `seen` to detect true circular references. `seen` is a set of ids.
    We add id(obj) before recursing and remove it after finishing that branch,
    which avoids false positives on shared (non-circular) objects.
    """
    if seen is None:
        seen = set()

    if struct is None:
        return None

    # primitives — return as-is
    if isinstance(struct, (str, int, float, bool)):
        return struct

    obj_id = id(struct)

    # If we've already started processing this exact object on the current stack,
    # it's a circular reference.
    if obj_id in seen:
        return "<circular_reference>"

    # Dicts
    if isinstance(struct, dict):
        seen.add(obj_id)
        try:
            result = {}
            for k, v in struct.items():
                new_key = snakeToCamel(k) if isinstance(k, str) else k
                result[new_key] = dictKeysToCamelCase(v, seen)
            return result
        finally:
            seen.remove(obj_id)

    # Lists
    if isinstance(struct, list):
        seen.add(obj_id)
        try:
            return [dictKeysToCamelCase(item, seen) for item in struct]
        finally:
            seen.remove(obj_id)

    # Tuples -> return tuple
    if isinstance(struct, tuple):
        seen.add(obj_id)
        try:
            return tuple(dictKeysToCamelCase(item, seen) for item in struct)
        finally:
            seen.remove(obj_id)

    # Sets -> convert to list (preserves elements, order lost)
    if isinstance(struct, set):
        seen.add(obj_id)
        try:
            return [dictKeysToCamelCase(item, seen) for item in struct]
        finally:
            seen.remove(obj_id)

    # Objects exposing to_dict()
    if hasattr(struct, "to_dict") and callable(struct.to_dict):
        seen.add(obj_id)
        try:
            return dictKeysToCamelCase(struct.to_dict(), seen)
        finally:
            seen.remove(obj_id)

    # Fallback — stringify unknown objects
    return str(struct)


if __name__ == '__main__':
    import doctest
    doctest.testmod()

    assert camelToSnake('snakesOnAPlane') == 'snakes_on_a_plane'
    assert camelToSnake('SnakesOnAPlane') == 'snakes_on_a_plane'
    assert camelToSnake('snakes_on_a_plane') == 'snakes_on_a_plane'
    assert camelToSnake('IPhoneHysteria') == 'i_phone_hysteria'
    assert camelToSnake('iPhoneHysteria') == 'i_phone_hysteria'
