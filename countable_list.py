"""
Countable List module.

This module defines the `CountableList` class, which is used to optimize query performance.

Author:
    Jay Taylor [@jtaylor]
"""


class CountableList(list):
    """
    Countable List class.

    This class is designed to optimize high query-volume scenarios, such as those encountered with TastyPie.
    """

    def __init__(self, the_list, count_value, meta=None):
        """
        Initializes a CountableList instance.

        Args:
            the_list (list): The list of items to initialize the CountableList with.
            count_value (int): The desired count value.
            meta (dict, optional): Additional metadata. Defaults to an empty dictionary.
        """

        super().__init__()
        self.count_value = count_value
        self.extend(the_list)
        self.meta = {} if meta is None else meta

    def count(self):
        """
        Returns the number of records.

        Returns:
            int: The count value.
        """

        return self.count_value
