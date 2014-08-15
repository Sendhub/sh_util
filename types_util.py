import types

IntTypes = (types.IntType, types.LongType)
NumberTypes = (types.IntType, types.LongType,
               types.FloatType, types.ComplexType)

def isNumber(n):
    ret = (n is not None and
           (isinstance(n, NumberTypes) or
            (isinstance(n, basestring) and n.isdigit())))

    if ret is False:
        try:
            float(n)
            ret = True
        except (ValueError, TypeError):
            pass
    return ret

def isInteger(n):
    return n is not None and \
           (isinstance(n, IntTypes) or
            (isinstance(n, basestring) and
             n.isdigit()))