"""
Utility functions and miscellaneous helpers for various tasks.
"""

from importlib import import_module


def lineno():
    """
    Returns the current line number from the invoker's context.

    This function uses the `inspect` module to retrieve the caller's line number.
    """

    import inspect

    return inspect.currentframe().f_back.f_lineno


def toId(_x):
    """
    Extracting an ID from the object if possible.

    Args:
        _x: The object to extract the ID from. Can be an integer or an object with an `id` attribute.

    Returns:
        The ID as an integer or the original object if no ID is found.
    """

    if isinstance(_x, int):
        return _x
    if hasattr(_x, "id"):
        return _x.id
    return _x


def findVariableByNameInFrame(name, depth=1):
    """
    Attempting to find a variable with a certain name at a specific stack frame depth.

    Args:
        name: The name of the variable to search for.
        depth: The stack frame depth to search in. Defaults to 1.

    Returns:
        The variable if found, otherwise None.
    """

    import sys

    frame = sys._getframe(2)
    selfSearch = [k for k in list(frame.f_locals.items()) if k == "self"]
    return selfSearch[0][1] if len(selfSearch) > 0 else None


def uniq(seq):
    """
    Generating a list containing only unique elements while preserving the original order.

    Args:
        seq: The sequence to process.

    Returns:
        A list of unique elements in the original order.

    References:
        http://stackoverflow.com/a/480227/293064
    """

    seen = set()
    seen_add = seen.add
    return [x for x in seq if x not in seen and not seen_add(x)]


def get_fully_qualified_class_name(obj) -> str:
    """
    Returning the fully qualified name (module + class) of a class or object.

    Args:
        obj: The class or object to retrieve the name for.

    Returns:
        The fully qualified name as a string.

    Examples:
        >>> get_fully_qualified_class_name(str)
        'builtins.str'
        >>> get_fully_qualified_class_name("hello")
        'builtins.str'
    """

    cls = obj if isinstance(obj, type) else obj.__class__
    module = getattr(cls, "__module__", None) or "<unknown_module>"
    name = getattr(cls, "__qualname__", getattr(cls, "__name__", "<unknown_class>"))

    return f"{module}.{name}"


def dynImport(path: str):
    """
    Dynamically importing a module or attribute by its string path.

    Args:
        path: The dot-separated path to the module or attribute.

    Returns:
        The imported module or attribute.
    """

    module_path, _, attr = path.rpartition(".")
    module = import_module(module_path)
    return getattr(module, attr)


__all__ = ["dynImport"]
