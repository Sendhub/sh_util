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

__author__ = "Jay Taylor [@jtaylor]"


import re

_underscorer1 = re.compile(r"(.)([A-Z][a-z]+)")
_underscorer2 = re.compile("([a-z0-9])([A-Z])")


_snakeFinder = re.compile(r"_(\w)")


def camel_to_snake(s):
    """
    Is it ironic that this function is written in camel case, yet it
    converts to snake case? hmm..
    """
    subbed = _underscorer1.sub(r"\1_\2", s)
    return _underscorer2.sub(r"\1_\2", subbed).lower()


def snake_to_camel(s):
    """Yet this is not ironic.."""
    return _snakeFinder.sub(lambda m: m.group(0)[1].upper(), s)


def dict_keys_to_snake_case(struct):
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

    if t is str or t is int or t is bool:
        return struct

    elif t is dict or hasattr(struct, "to_dict"):
        # If the object is not a dictionary but knows how to transform into a dict, then do so
        if t is not dict:
            struct = struct.to_dict()

        for k, v in list(struct.items()):
            del struct[k]
            struct[camel_to_snake(k)] = dict_keys_to_snake_case(v)
        return struct

    elif t is list or hasattr(struct, "__iter__"):
        return [dict_keys_to_snake_case(item) for item in struct]

    elif struct is None:
        return None

    else:
        raise TypeError(f"_dictKeysToSnakeCase: unsupported type `{t}'")


def dict_keys_to_camel_case(struct, seen=None):
    """
    Recursively converting snake_case dict keys to camelCase.

    Uses `seen` to detect true circular references. `seen` is a set of ids.
    We are adding `id(obj)` before recursing and removing it after finishing that branch, which is avoiding false positives on shared (non-circular) objects.
    """

    if seen is None:
        seen = set()

    if struct is None:
        return None

    # Returning primitives as-is
    if isinstance(struct, (str, int, float, bool)):
        return struct

    obj_id = id(struct)

    # Detecting circular references on the current stack
    if obj_id in seen:
        return "<circular_reference>"

    # Handling dicts
    if isinstance(struct, dict):
        seen.add(obj_id)
        try:
            result = {}
            for k, v in struct.items():
                new_key = snake_to_camel(k) if isinstance(k, str) else k
                result[new_key] = dict_keys_to_camel_case(v, seen)
            return result
        finally:
            seen.remove(obj_id)

    # Handling lists
    if isinstance(struct, list):
        seen.add(obj_id)
        try:
            return [dict_keys_to_camel_case(item, seen) for item in struct]
        finally:
            seen.remove(obj_id)

    # Returning tuples
    if isinstance(struct, tuple):
        seen.add(obj_id)
        try:
            return tuple(dict_keys_to_camel_case(item, seen) for item in struct)
        finally:
            seen.remove(obj_id)

    # Converting sets to lists (preserving elements; order may be lost)
    if isinstance(struct, set):
        seen.add(obj_id)
        try:
            return [dict_keys_to_camel_case(item, seen) for item in struct]
        finally:
            seen.remove(obj_id)

    # Handling objects exposing `to_dict()`
    if hasattr(struct, "to_dict") and callable(struct.to_dict):
        seen.add(obj_id)
        try:
            return dict_keys_to_camel_case(struct.to_dict(), seen)
        finally:
            seen.remove(obj_id)

    # Stringifying unknown objects
    return str(struct)


if __name__ == "__main__":
    import doctest

    doctest.testmod()

    assert camel_to_snake("snakesOnAPlane") == "snakes_on_a_plane"
    assert camel_to_snake("SnakesOnAPlane") == "snakes_on_a_plane"
    assert camel_to_snake("snakes_on_a_plane") == "snakes_on_a_plane"
    assert camel_to_snake("IPhoneHysteria") == "i_phone_hysteria"
    assert camel_to_snake("iPhoneHysteria") == "i_phone_hysteria"
