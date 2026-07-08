"""
This module provides utility functions for type checking and validation.

Functions:
    isNumber(maybe_num): Check if the argument is a number.
    isInteger(maybe_num): Check if the argument is an integer.

Updated to be compatible with Python 3.13.8.
Release Date: 2025-Oct-07
"""

IntTypes = (int,)
NumberTypes = (int, float, complex)


def isNumber(maybe_num):
    """
    Check if the argument is a number.

    Args:
        maybe_num: The value to check. Can be of any type.

    Returns:
        bool: True if the argument is a number, False otherwise.

    Notes:
        - Strings that represent numeric values (e.g., '123') are also considered numbers.
        - Attempts to convert the value to a float if initial checks fail.
    """

    ret = maybe_num is not None and (isinstance(maybe_num, NumberTypes) or (isinstance(maybe_num, str) and maybe_num.isdigit()))

    if not ret:
        try:
            float(maybe_num)
            ret = True
        except (ValueError, TypeError):
            pass

    return ret


def isInteger(maybe_num):
    """
    Check if the argument is an integer.

    Args:
        maybe_num: The value to check. Can be of any type.

    Returns:
        bool: True if the argument is an integer, False otherwise.

    Notes:
        - Strings that represent integers (e.g., '123', '+456') are also considered integers.
        - Strips leading/trailing whitespace from strings before checking.
    """

    ret = False

    if maybe_num:
        ret = isinstance(maybe_num, IntTypes)

        if not ret and isinstance(maybe_num, str):
            maybe_num = maybe_num.strip()
            ret = maybe_num.isdigit() or (maybe_num[0] in "+-" and maybe_num[1:].isdigit())

    return ret
