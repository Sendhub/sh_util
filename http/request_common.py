"""
Django HTTP Utilities
"""
__author__ = 'Jay Taylor [@jtaylor]'

def extract_parameters(query_dict, parameters, empty_value=None):
    """
    Parses out a tuple of the the specified parameters from the passed
    query_dict. For params that aren't found, the value will be None.

    @return tuple of the same length as the sequence of parameters.
    """
    out = []
    for parameter in parameters:
        if parameter in query_dict:
            out.append(query_dict[parameter])
        else:
            out.append(empty_value)
    return tuple(out)
