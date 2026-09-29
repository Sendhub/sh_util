"""
Utilities for splitting text into SMS-sized fragments.

This module provides a helper to split a long string into multiple fragments that are suitable for SMS messages.
Fragments will prefer breaking at word boundaries when possible.
"""

__author__ = "brock"

import re


def split_string(str_to_split, fragment_length=160, max_fragments=-1):
    """
    Split a string into fragments up to a given length.

    The function returns a list of fragments where each fragment is at most ``fragmentLength`` characters.
    When possible, splitting is performed on word boundaries.
    If ``maxFragments`` is set to a non-negative value the remaining text will be returned as the last fragment once that limit is reached.

    Args:
        strToSplit (str): The string to split.
        fragmentLength (int): Maximum length of each fragment. Defaults to 160.
        maxFragments (int): Maximum number of fragments to produce; ``-1`` means no limit.

    Returns:
        list: A list of string fragments.
    """

    def reverse(r_list):
        """
        Reverse a list and return the reversed copy.

        This helper is returning a new list that is the reversed version of the input list.
        """

        temp = r_list[:]
        temp.reverse()
        return temp

    fragments = []

    i = 0
    s = 0
    wordBoundaryRe = re.compile(r"(\s)", re.DOTALL | re.IGNORECASE | re.M)  # noqa

    # Making as many fragments as necessary when `maxFragments` is -1
    while i < max_fragments or max_fragments == -1:
        if max_fragments != -1 and i + 1 == max_fragments:
            # Reaching the maximum number of fragments, returning the
            # rest of the string as the last fragment regardless of length
            fragment = str_to_split[s:]
            fragments.append(fragment)
        else:
            # Getting the next fragment
            fragment = str_to_split[s : s + fragment_length]

            if fragment == "":
                break

            # Checking the end of the slice for a word boundary.
            # Assuming that the last space from the end is the word boundary.
            m = wordBoundaryRe.search("".join(reverse(list(fragment))))
            if m is not None:
                fragment = fragment[: len(fragment) - m.start()]
            s = s + len(fragment)
            fragments.append(fragment)

        i += 1

    return fragments
