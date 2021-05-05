"""types utils """
# pylint: disable=C0103,W0611
import types  # noqa

IntTypes = (int, int)
NumberTypes = (int, int,
               float, complex)


def isNumber(maybe_num):
    """ check if the argument is number"""
    ret = (maybe_num is not None and
           (isinstance(maybe_num, NumberTypes) or
            (isinstance(maybe_num, str) and maybe_num.isdigit())))

    if ret is False:
        try:
            float(maybe_num)
            ret = True
        except (ValueError, TypeError):
            pass
    return ret


def isInteger(maybe_num):
    """ check if the argument is integer"""
    ret = False

    if maybe_num:
        ret = isinstance(maybe_num, IntTypes)

        if not ret and isinstance(maybe_num, str):
            maybe_num = maybe_num.strip()
            ret = maybe_num.isdigit() or \
                (maybe_num[0] in "+-" and maybe_num[1:].isdigit())

    return ret
