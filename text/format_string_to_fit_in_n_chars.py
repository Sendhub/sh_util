"""
Textual helpers for formatting and trimming strings.

This module provides utilities for formatting strings to fit within a
specified character limit, and for trimming tokens when formatted output
exceeds the allowed length.
"""

__author__ = "Jay Taylor [@jtaylor]"


import math
from functools import reduce


def squeeze_sms_message(string, *args):
    """Squeezing a message to fit within the SMS character limit.

    Args:
        string (str): The message template to format.
        *args: Positional arguments applied to ``string.format``.

    Returns:
        str: The formatted message, trimmed to the configured SMS length.
    """

    maxLength = 160

    try:
        import settings

        maxLength = settings.MAX_SMS_MESSAGE_LENGTH
    except (ImportError, AttributeError):
        pass

    return format_string_to_fit_in_n_chars(*([string, maxLength] + list(args)))


def format_string_to_fit_in_n_chars(string, max_number_of_characters, *args):
    """
    Formatting a string and ensuring it remains within a maximum length.

    This function is formatting ``string`` with the provided positional
    arguments and is attempting to trim the longest substitution tokens if
    the result exceeds ``max_number_of_characters``.

    Args:
        string (str): The format string.
        max_number_of_characters (int): Maximum allowed characters.
        *args: Values to be substituted into ``string``.

    Returns:
        str: A formatted string not exceeding ``max_number_of_characters``.

    Raises:
        TypeError: If insufficient args are provided or ``string`` is longer
            than ``max_number_of_characters``.
        Exception: If formatting cannot be reduced to fit within the limit.
    """

    if len(args) == 0:
        raise TypeError(f"format_string_to_fit_in_n_chars() takes 2 or more arguments ({len(args)} given)")  # noqa

    max_number_of_characters = int(max_number_of_characters)

    # Checking initial conditions.
    if len(string) > max_number_of_characters:
        raise TypeError(f"format_string_to_fit_in_n_chars() argument 1 must not exceed the length indicated by argument 2 ({len(string)} > {max_number_of_characters})")

    # First trying the naive strategy of formatting without trimming.
    test = string.format(*args)
    if len(test) <= max_number_of_characters:
        return test

    if len(args) > 0:
        exceeded_by = len(test) - 160
        trimmed_args = _trim_longest_tokens_to_reduce_length(args, exceeded_by)
        test = string.format(*trimmed_args)

    if len(test) > max_number_of_characters:
        raise Exception(f"Failed to format string {string} to fit inside of {max_number_of_characters} characters")

    return test


def _trim_percentage_off_tail(s, pct):
    """Trimming a string by a percentage of its original length.

    Args:
        s (str): The input string.
        pct (float): Fraction to trim (0.0 - 1.0).

    Returns:
        str: The trimmed string, using '..' to indicate truncation when
        applicable.
    """
    s_len = len(s)
    if s_len > 0:
        if s_len < 3:
            return s
        offset = int(math.floor(s_len - (s_len * pct)))
        s = f"{s[0:offset]}.."
    return s


def _trim_longest_tokens_to_reduce_length(tokens, reduce_by_n_chars):
    """
    Trimming tokens starting with the longest until a target reduction is reached.

    This function is using a simple, brute-force approach to iteratively
    shorten the longest tokens until the cumulative length reduction meets
    ``reduce_by_n_chars``.

    Args:
        tokens (list[str]): List of strings to be considered for trimming.
        reduce_by_n_chars (int): Number of characters to reduce in total.

    Returns:
        list[str]: A list of tokens possibly trimmed to achieve the target
        reduction.

    Raises:
        TypeError: If ``tokens`` is empty.
    """
    # NB: this is a brute force type of approach, I'm sure it will be
    # being used; it can be improved with further work.
    if len(tokens) == 0:
        raise TypeError("trim_longest_tokens_to_reduce_length() does not accept empty lists")

    start_length = reduce(lambda a, b: a + len(b), tokens, 0)
    n_characters_cut = 0
    unique_tokens = set(tokens)
    step = len(tokens)

    shrunk = tokens

    while step > 0 and n_characters_cut < reduce_by_n_chars:
        pct = 0.05

        while pct < 0.86 and n_characters_cut < reduce_by_n_chars:
            # Calculating the index offset of the top n tokens to trim.
            n = int(math.ceil(len(unique_tokens) / (step * 1.0)))

            top = sorted(unique_tokens, key=lambda x: len(x), reverse=True)[0:n]
            # Printing the current top tokens being considered.

            transformed = {t: _trim_percentage_off_tail(t, pct) for t in top}

            # Reintegrating with the original list.
            shrunk = [transformed.get(t, t) for t in tokens]

            # The doubling strategy had been considered as an optimization
            # but a linear approach is being used instead (slower but
            # yielding nicer and more precise results).
            pct += 0.05

            updated_length = reduce(lambda a, b: a + len(b), shrunk, 0)
            n_characters_cut = start_length - updated_length

            # Printing the number of characters cut so far.

        step -= 1

    return shrunk
