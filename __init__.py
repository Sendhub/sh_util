# -*- coding: utf-8 -*-

"""Things which don't seem to cleanly fit anywhere else."""

def lineno():
    """Returns the current line number from the invoker's context."""
    import inspect
    return inspect.currentframe().f_back.f_lineno


def toId(x):
    """Get an id out of the object if possible."""
    if isinstance(x, int) or isinstance(x, long):
        return x
    if hasattr(x, 'id'):
        return x.id
    return x


def findVariableByNameInFrame(name, depth=1):
    """
    Attempt to find a variables with a certain name at a certain depth of stack
    frame.
    """
    import sys
    frame = sys._getframe(2)
    selfSearch = filter(lambda (k, v): k == 'self', frame.f_locals.items())
    return selfSearch[0][1] if len(selfSearch) > 0 else None


def uniq(seq):
    """
    @see http://stackoverflow.com/a/480227/293064

    @return list containing only the unique elements with the original ordering preserved.
    """
    seen = set()
    seen_add = seen.add
    return [x for x in seq if x not in seen and not seen_add(x)]


def getFullyQualifiedClassName(o):
    """Get the fully qualified name for a class or object."""
    # Get at underlying class if `o` is an instance.
    if type(o) != type:
        o = o.__class__

    return '{0}.{1}'.format(o.__module__, o.__name__)


# Dynamically import a module resource.
dynImport = lambda path: reduce(
    lambda module, next: getattr(module, next),
    path.split('.')[1:],
    __import__(path[0:path.index('.')])
)

