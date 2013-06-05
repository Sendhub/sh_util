# encoding: utf-8

"""Countable List class."""

__author__ = 'Jay Taylor [@jtaylor]'


class CountableList(list):
    """
    Countable List class.

    This is used primarily to short-circuit the high query-volume which happens
    by default with TastyPie.
    """

    def __init__(self, theList, countValue, meta=None):
        """
        Pass in the list as well as the desired count value.

        @param meta dict, defaults to {}.  Additional miscellaneous meta-data.
        """
        self.countValue = countValue
        self.extend(theList)
        self.meta = {} if meta is None else meta

    def count(self):
        """@return the number of records."""
        return self.countValue

