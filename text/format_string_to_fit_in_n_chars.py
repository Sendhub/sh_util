# encoding: utf-8

"""
Textual helpers.
"""

__author__ = 'Jay Taylor [@jtaylor]'


import math


def squeeze_sms_message(string, *args):
    """Squeezes a message to fit inside 160 characters."""
    maxLength = 160

    try:
        import settings
        maxLength = settings.MAX_SMS_MESSAGE_LENGTH
    except (ImportError, AttributeError):
        pass

    return format_string_to_fit_in_n_chars(*([string, maxLength] + list(args)))

def format_string_to_fit_in_n_chars(
    string,
    max_number_of_characters,
    *args
):
    """
    Format a string and make a valiant effort to ensure that is remains within
    a certain maximum length.

    arg1 = string to format
    arg2 = number of characters allowed in string
    arg[2:] = args which will be passed to str.format.
    """
    if len(args) == 0:
        raise TypeError('format_string_to_fit_in_n_chars() takes 2 or more arguments ({0} given)'.format(len(args)))

    max_number_of_characters = int(max_number_of_characters)

    # Validate initial conditions.
    if len(string) > max_number_of_characters:
        raise TypeError(
            'format_string_to_fit_in_n_chars() argument 1 must not exceed the length indicated by argument 2 ({0} > {1}'
            .format(len(string), max_number_of_characters)
        )

    # First try the naiive strategy of just hoping that everything works out.
    test = string.format(*args)
    if len(test) <= max_number_of_characters:
        return test

    if len(args) > 0:
        exceeded_by = len(test) - 160
        trimmed_args = _trim_longest_tokens_to_reduce_length(args, exceeded_by)
        test = string.format(*trimmed_args)

    if len(test) > max_number_of_characters:
        raise Exception(
            'Failed to format string {{0}} to fit inside of {1} characters'
            .format(string, max_number_of_characters)
        )

    return test

def _trim_percentage_off_tail(s, pct):
    """
    Trims a string down by a specific percentage of it's original length.
    """
    s_len = len(s)
    if s_len > 0:
        if s_len < 3:
            return s
        offset = int(math.floor(s_len - (s_len * pct)))
        s = '{0}..'.format(s[0: offset])
    return s

def _trim_longest_tokens_to_reduce_length(tokens, reduce_by_n_chars):
    """
    Trim a list of words starting with the longer words until a target
    number of characters reduction has been reached.
    """
    # NB: this is a brute force type of approach, I'm sure it will be
    # improved if someone spends some time on it.
    if len(tokens) == 0:
        raise TypeError('trim_longest_tokens_to_reduce_length() does not accept empty lists')

    start_length = reduce(lambda a, b: a + len(b), tokens, 0)
    n_characters_cut = 0
    unique_tokens = set(tokens)
    step = len(tokens)

    #print 'start_len=',start_length,'need_to_reduce_by=',reduce_by_n_chars

    shrunk = tokens

    while step > 0 and n_characters_cut < reduce_by_n_chars:
        pct = 0.05

        while pct < 0.86 and n_characters_cut < reduce_by_n_chars:
            # Calculate the index offset of the top n records desired.
            n = int(math.ceil(len(unique_tokens) / (step * 1.0)))

            top = sorted(unique_tokens, key=lambda x: len(x), reverse=True)[0:n]
            #print 'top=',top

            transformed = dict(map(lambda t: (t, _trim_percentage_off_tail(t, pct)), top))

            # Reintegrate with original list.
            shrunk = map(lambda t: transformed.get(t, t), tokens)

            # The doubling strategy here yields reasonable results and cuts
            # down on the number of iterations by quite a bit.
            #pct += pct

            # Let's try a linear approach instead (slower but yields nicer
            # and more precise results.)
            pct += 0.05

            updated_length = reduce(lambda a, b: a + len(b), shrunk, 0)
            n_characters_cut = start_length - updated_length

            #print 'numcut=',n_characters_cut

        step -= 1

    return shrunk

