"""Unit tests for the helpers defined directly in ``sh_util/__init__.py``.

``find_variable_by_name_in_frame`` has a pinned quirk (see its own test class
below): it always returns ``None``. The membership check compares a
``(key, value)`` tuple from ``frame.f_locals.items()`` against the string
``"self"``, which is never equal regardless of what's actually in the frame.
"""

import sh_util
from sh_util import (
    dyn_import,
    find_variable_by_name_in_frame,
    get_fully_qualified_class_name,
    lineno,
    to_id,
    uniq,
)


class TestLineno:
    def test_returns_an_int(self):
        assert isinstance(lineno(), int)

    def test_consecutive_calls_report_consecutive_source_lines(self):
        first = lineno()
        second = lineno()
        assert second == first + 1


class TestToId:
    def test_int_is_returned_unchanged(self):
        assert to_id(5) == 5

    def test_object_with_id_attribute_returns_the_id(self):
        class HasId:
            id = 42

        assert to_id(HasId()) == 42

    def test_object_without_id_attribute_returns_the_object_itself(self):
        obj = object()
        assert to_id(obj) is obj

    def test_string_without_id_attribute_returns_the_string_itself(self):
        assert to_id("no-id-here") == "no-id-here"


class TestFindVariableByNameInFrame:
    def test_always_returns_none_even_when_self_is_present(self):
        class Caller:
            def call(self):
                return find_variable_by_name_in_frame("self")

        assert Caller().call() is None

    def test_always_returns_none_with_an_explicit_depth(self):
        assert find_variable_by_name_in_frame("self", depth=2) is None


class TestUniq:
    def test_removes_duplicates_preserving_order(self):
        assert uniq([1, 2, 1, 3, 2, 4]) == [1, 2, 3, 4]

    def test_empty_sequence_returns_empty_list(self):
        assert uniq([]) == []

    def test_no_duplicates_returns_equivalent_list(self):
        assert uniq([1, 2, 3]) == [1, 2, 3]

    def test_works_on_strings_as_a_sequence_of_characters(self):
        assert uniq("mississippi") == ["m", "i", "s", "p"]


class TestGetFullyQualifiedClassName:
    def test_builtin_type(self):
        assert get_fully_qualified_class_name(str) == "builtins.str"

    def test_builtin_instance(self):
        assert get_fully_qualified_class_name("hello") == "builtins.str"

    def test_custom_class(self):
        class MyClass:
            pass

        assert get_fully_qualified_class_name(MyClass) == f"{__name__}.{MyClass.__qualname__}"

    def test_custom_instance(self):
        class MyClass:
            pass

        assert get_fully_qualified_class_name(MyClass()) == f"{__name__}.{MyClass.__qualname__}"


class TestDynImport:
    def test_imports_an_attribute_from_a_module(self):
        import os.path

        assert dyn_import("os.path.join") is os.path.join

    def test_imports_a_function_from_a_top_level_module(self):
        import json

        assert dyn_import("json.dumps") is json.dumps


class TestDunderAll:
    def test_dyn_import_is_the_only_exported_name(self):
        assert sh_util.__all__ == ["dyn_import"]
