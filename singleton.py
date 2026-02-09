"""
Singleton module with inheritance capability.

This module provides a Singleton class that supports inheritance, ensuring that
subclasses create their own unique singleton instances.

Originally found here: http://code.activestate.com/recipes/52558/
"""

__author__ = 'Jay Taylor [@jtaylor]'


class Singleton:
    """
    Singleton base class.

    This class ensures that only one instance of a class is created. Subclasses
    can also create their own unique singleton instances by comparing class types.
    """

    # The one, true Singleton.
    __single = None

    def __new__(classtype, *args, **kwargs):
        """
        Creating or returning the singleton instance.

        This method checks if a singleton instance already exists for the class.
        If not, it creates a new instance. Subclasses are handled by comparing
        class types to ensure they create their own singleton instances.

        Args:
            classtype: The class type for which the singleton is being created.
            *args: Positional arguments for the class constructor.
            **kwargs: Keyword arguments for the class constructor.

        Returns:
            Singleton: The singleton instance of the class.
        """

        # Checking if the singleton instance already exists for the class.
        if classtype != type(classtype.__single):  # noqa
            classtype.__single = object.__new__(classtype, *args, **kwargs)

        return classtype.__single

    def __init__(self):
        """
        Initializing the Singleton instance.

        This method is intentionally left empty as the Singleton pattern ensures
        that initialization happens only once.
        """
        pass


if __name__ == '__main__':
    # Import only needed for this demo/test code
    import logging

    class Subsingleton(Singleton):
        """
        Subsingleton class.

        This class demonstrates the inheritance capability of the Singleton base
        class, allowing subclasses to create their own unique singleton instances.
        """
        pass

    o1 = Singleton('foo')
    o1.display()
    o2 = Singleton('bar')
    o2.display()
    o3 = Subsingleton('foobar')
    o3.display()
    o4 = Subsingleton('barfoo')
    o4.display()

    # Logging the results of singleton comparisons.
    logging.info(f'o1 = o2: {o1 == o2}')
    logging.info(f'o1 = o3: {o1 == o3}')
    logging.info(f'o3 = o4: {o3 == o4}')
    logging.info(f'o1 is a singleton? {isinstance(o1, Singleton)}')
    logging.info(f'o3 is a singleton? {isinstance(o3, Singleton)}')
    logging.info(f'o1 is a subsingleton? {isinstance(o1, Subsingleton)}')  # noqa
    logging.info(f'o3 is a subsingleton? {isinstance(o3, Subsingleton)}')  # noqa
