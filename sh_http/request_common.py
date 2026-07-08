"""
Django HTTP utilities.

This module provides small helpers for HTTP-related tasks used across the
project.
"""

__author__ = "Jay Taylor [@jtaylor]"


def extract_parameters(query_dict, parameters, empty_value=None):
    """
    Parsing out a tuple of the specified parameters from `query_dict`.

    Args:
        query_dict: The dictionary-like object containing query parameters.
        parameters: An iterable of parameter names to extract.
        empty_value: The value to use when a parameter is not present.

    Returns:
        tuple: A tuple with extracted values in the same order as `parameters`.
    """

    out = []
    for parameter in parameters:
        if parameter in query_dict:
            out.append(query_dict[parameter])
        else:
            out.append(empty_value)
    return tuple(out)
