"""Unit tests for ``sh_util.singleton.Singleton``.

Each subclass gets its own singleton instance: ``__new__`` name-mangles to
``_Singleton__single``, and the assignment inside ``__new__`` happens on
``cls`` — so a subclass gets its own attribute the first time it's
instantiated, shadowing (rather than overwriting) whatever the base class or
a sibling subclass holds.
"""

from sh_util.singleton import Singleton


class TestSingletonPerClass:
    def test_repeated_instantiation_of_the_same_subclass_returns_the_same_object(self):
        class Alpha(Singleton):
            pass

        assert Alpha() is Alpha()

    def test_sibling_subclasses_get_independent_instances(self):
        class Beta(Singleton):
            pass

        class Gamma(Singleton):
            pass

        beta = Beta()
        gamma = Gamma()
        assert beta is not gamma
        assert type(beta) is Beta
        assert type(gamma) is Gamma

    def test_new_returns_an_instance_of_the_requested_class(self):
        class Delta(Singleton):
            pass

        assert isinstance(Delta(), Delta)

    def test_direct_instantiation_of_the_base_class_is_idempotent(self):
        assert Singleton() is Singleton()

    def test_init_does_not_raise(self):
        class Epsilon(Singleton):
            pass

        assert Epsilon().__init__() is None
