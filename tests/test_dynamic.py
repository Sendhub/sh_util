"""Unit tests for ``sh_util.dynamic``.

``dynamically_generate_transformed_attributes`` builds a throwaway mixin class
carrying ``<attr><suffix>`` properties that return a transformed view of another
attribute, so a model can declare them without hand-writing each one.
"""

from sh_util.dynamic import dynamically_generate_transformed_attributes, with_str_attrs


class TestWithStrAttrs:
    def test_generates_a_str_property(self):
        class Model(with_str_attrs("id")):
            def __init__(self, id):
                self.id = id

        assert Model(29).id_str == "29"

    def test_original_attribute_is_untouched(self):
        class Model(with_str_attrs("id")):
            def __init__(self, id):
                self.id = id

        model = Model(29)
        assert model.id == 29
        assert isinstance(model.id_str, str)

    def test_handles_several_attributes(self):
        class Model(with_str_attrs("id", "user_id")):
            def __init__(self, id, user_id):
                self.id = id
                self.user_id = user_id

        model = Model(29, 30)
        assert (model.id_str, model.user_id_str) == ("29", "30")

    def test_property_reflects_later_mutation(self):
        class Model(with_str_attrs("id")):
            def __init__(self, id):
                self.id = id

        model = Model(1)
        model.id = 2
        assert model.id_str == "2"

    def test_no_attributes_produces_a_bare_mixin(self):
        mixin = with_str_attrs()
        assert not [name for name in vars(mixin) if name.endswith("_str")]


class TestDynamicallyGenerateTransformedAttributes:
    def test_custom_transform_and_suffix(self):
        class Model(dynamically_generate_transformed_attributes(lambda v: v * 2, "_doubled", "n")):
            def __init__(self, n):
                self.n = n

        assert Model(21).n_doubled == 42

    def test_suffix_may_be_a_callable(self):
        # A callable suffix receives the attribute name and returns the new one,
        # so the generated name need not be a simple concatenation.
        reverse_name = lambda attr: attr[::-1]  # noqa: E731

        class Model(dynamically_generate_transformed_attributes(str, reverse_name, "id", "foo")):
            def __init__(self, id, foo):
                self.id = id
                self.foo = foo

        model = Model(30, "bar")
        assert model.di == "30"
        assert model.oof == "bar"

    def test_generated_members_are_properties_not_methods(self):
        generated = dynamically_generate_transformed_attributes(str, "_str", "id")
        assert isinstance(vars(generated)["id_str"], property)

    def test_each_call_returns_an_independent_class(self):
        first = dynamically_generate_transformed_attributes(str, "_str", "a")
        second = dynamically_generate_transformed_attributes(str, "_str", "b")
        assert first is not second
        assert "a_str" in vars(first)
        assert "a_str" not in vars(second)

    def test_missing_source_attribute_raises_on_access(self):
        class Model(with_str_attrs("absent")):
            pass

        try:
            Model().absent_str
        except AttributeError:
            pass
        else:
            raise AssertionError("expected AttributeError for a missing source attribute")

    def test_transform_exceptions_propagate(self):
        def explode(_value):
            raise ValueError("bad transform")

        class Model(dynamically_generate_transformed_attributes(explode, "_x", "n")):
            def __init__(self):
                self.n = 1

        try:
            Model().n_x
        except ValueError as err:
            assert str(err) == "bad transform"
        else:
            raise AssertionError("expected the transform's ValueError to propagate")
