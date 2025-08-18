"""
Updated the below code to be compatible with python 3.11.13
"""

IntTypes = (int,)
NumberTypes = (int, float, complex)

def isNumber(maybe_num):
    ret = (maybe_num is not None and
           (isinstance(maybe_num, NumberTypes) or
            (isinstance(maybe_num, str) and maybe_num.isdigit())))

    if not ret:
        try:
            float(maybe_num)
            ret = True
        except (ValueError, TypeError):
            pass
    return ret

def isInteger(maybe_num):
    ret = False

    if maybe_num:
        ret = isinstance(maybe_num, IntTypes)

        if not ret and isinstance(maybe_num, str):
            maybe_num = maybe_num.strip()
            ret = maybe_num.isdigit() or \
                  (maybe_num[0] in "+-" and maybe_num[1:].isdigit())

    return ret
