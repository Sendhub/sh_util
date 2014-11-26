import types

IntTypes = (types.IntType, types.LongType)
NumberTypes = (types.IntType, types.LongType,
               types.FloatType, types.ComplexType)

def isNumber(maybe_num):
    ret = (maybe_num is not None and
           (isinstance(maybe_num, NumberTypes) or
            (isinstance(maybe_num, basestring) and maybe_num.isdigit())))

    if ret is False:
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

        if not ret and isinstance(maybe_num, basestring):
            maybe_num = maybe_num.strip()
            ret = maybe_num.isdigit() or \
                  (maybe_num[0] in "+-" and maybe_num[1:].isdigit())

    return ret
