# -*- coding: utf-8 -*-

"""
"Singleton capable of inheritance"

Originally found here: http://code.activestate.com/recipes/52558/
"""

__author__ = 'Jay Taylor [@jtaylor]'


class Singleton(object):
    # The one, true Singleton.
    __single = None

    def __new__(classtype, *args, **kwargs):
        """
        Check to see if a __single exists already for this class
        Compare class types instead of just looking for None so
        that subclasses will create their own __single objects.
        """
        if classtype != type(classtype.__single):
            classtype.__single = object.__new__(classtype, *args, **kwargs)

        return classtype.__single

    def __init__(self):
        pass


if __name__ == '__main__':
    import logging

    class Subsingleton(Singleton):
        pass

    o1 = Singleton('foo')
    o1.display()
    o2 = Singleton('bar')
    o2.display()
    o3 = Subsingleton('foobar')
    o3.display()
    o4 = Subsingleton('barfoo')
    o4.display()
    logging.info('o1 = o2: {0}'.format(o1 == o2))
    logging.info('o1 = o3: {0}'.format(o1 == o3))
    logging.info('o3 = o4: {0}'.format(o3 == o4))
    logging.info('o1 is a singleton? {0}'.format(isinstance(o1, Singleton)))
    logging.info('o3 is a singleton? {0}'.format(isinstance(o3, Singleton)))
    logging.info('o1 is a subsingleton? {0}'.format(isinstance(o1, Subsingleton)))
    logging.info('o3 is a subsingleton? {0}'.format(isinstance(o3, Subsingleton)))

