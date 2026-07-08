"""
This module defines an `OrderedDict` class, which extends the functionality of Python's built-in `dict`.
It maintains the order of keys as they are inserted, providing additional methods for sorting and reversing.
"""

__author__ = "Jay Taylor [@jtaylor]"

from collections.abc import MutableMapping


class OrderedDict(dict, MutableMapping):
    """
    A dictionary subclass that maintains the order of keys as they are inserted.

    Provides additional methods for sorting and reversing the order of keys.
    """

    def __init__(self, *args, **kwds):
        """
        Initializes the `OrderedDict` instance.

        Args:
            *args: Positional arguments to initialize the dictionary.
            **kwds: Keyword arguments to initialize the dictionary.

        Raises:
            TypeError: If more than one positional argument is provided.
        """
        if len(args) > 1:
            raise TypeError("expected at 1 argument, got %d", len(args))
        if not hasattr(self, "_keys"):
            self._keys = []
        self.update(*args, **kwds)

    def clear(self):
        """
        Clears all items from the dictionary.
        """
        del self._keys[:]
        dict.clear(self)

    def __setitem__(self, key, value):
        """
        Sets the value for a key in the dictionary.

        Args:
            key: The key to set.
            value: The value to associate with the key.
        """
        if key not in self:
            self._keys.append(key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        """
        Deletes a key-value pair from the dictionary.

        Args:
            key: The key to delete.
        """
        dict.__delitem__(self, key)
        self._keys.remove(key)

    def __iter__(self):
        """
        Returns an iterator over the keys in the dictionary.
        """
        return iter(self._keys)

    def __reversed__(self):
        """
        Returns a reversed iterator over the keys in the dictionary.
        """
        return reversed(self._keys)

    def popitem(self):
        """
        Removes and returns the last key-value pair from the dictionary.

        Returns:
            tuple: The last key-value pair.

        Raises:
            KeyError: If the dictionary is empty.
        """
        if not self:
            raise KeyError
        key = self._keys.pop()
        value = dict.pop(self, key)
        return key, value

    def __reduce__(self):
        """
        Helper method for pickling the `OrderedDict` instance.

        Returns:
            tuple: The state of the instance for pickling.
        """
        items = [[k, self[k]] for k in self]
        inst_dict = vars(self).copy()
        inst_dict.pop("_keys", None)
        return (self.__class__, (items,), inst_dict)

    # Methods with indirect access via the above methods

    setdefault = MutableMapping.setdefault
    update = MutableMapping.update
    pop = MutableMapping.pop
    keys = MutableMapping.keys
    values = MutableMapping.values
    items = MutableMapping.items

    def __repr__(self):
        """
        Returns a string representation of the `OrderedDict` instance.

        Returns:
            str: The string representation of the dictionary.
        """
        pairs = ", ".join(map("%r: %r".__mod__, list(self.items())))
        return "{}({{{}}})".format(self.__class__.__name__, pairs)

    def copy(self):
        """
        Creates a shallow copy of the `OrderedDict` instance.

        Returns:
            OrderedDict: A shallow copy of the dictionary.
        """
        return self.__class__(self)

    def sort(self):
        """
        Sorting the keys in the dictionary.
        """
        self._keys.sort()

    def reverse(self):
        """
        Reversing the order of keys in the dictionary.
        """
        self._keys.reverse()

    @classmethod
    def fromkeys(cls, iterable, value=None):
        """
        Creates a new `OrderedDict` instance from an iterable of keys.

        Args:
            iterable: An iterable of keys.
            value: The value to associate with each key. Defaults to None.

        Returns:
            OrderedDict: A new `OrderedDict` instance.
        """
        _d = cls()
        for key in iterable:
            _d[key] = value
        return _d
