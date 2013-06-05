
__author__ = 'Jay Taylor [@jtaylor]'

"""
Django HTTP Utilities
"""

def extract_parameters(query_dict, parameters, empty_value=None):
    """
    Parses out a tuple of the the specified parameters from the passed
    query_dict. For params that aren't found, the value will be None.

    @return tuple of the same length as the sequence of parameters.
    """
    out = []
    for p in parameters:
        if p in query_dict:
            out.append(query_dict[p])
        else:
            out.append(empty_value)
    return tuple(out)
