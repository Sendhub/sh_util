"""Unit tests for ``sh_util.fake_request``.

``FakeRequest`` is a serializable stand-in for a Django request, used where a
real request cannot be pickled onto a queue. ``AnonymousUser`` is a local
fallback that only exists when Django is absent — which is the case in this
repo's environment, so the fallback below is the live implementation.
"""

import pytest
from sh_util.fake_request import AnonymousUser, FakeRequest


class _Request:
    """Minimal stand-in for the Django request attributes FakeRequest reads."""

    def __init__(self, **overrides):
        self.GET = {"a": "1"}
        self.POST = {"b": "2"}
        self.REQUEST = {"c": "3"}
        self.path = "/some/path"
        self.body = "payload"
        self.user = "real-user"
        self.__dict__.update(overrides)

    def is_secure(self):
        return True

    def get_host(self):
        return "example.com"

    def build_absolute_uri(self):
        return "https://example.com/some/path"


class TestAnonymousUserDefaults:
    def test_field_defaults(self):
        user = AnonymousUser()
        assert user.id is None
        assert user.username == ""
        assert user.is_staff is False
        assert user.is_active is False
        assert user.is_superuser is False

    def test_str(self):
        assert str(AnonymousUser()) == "AnonymousUser"

    def test_all_instances_are_equal(self):
        assert AnonymousUser() == AnonymousUser()

    def test_not_equal_to_other_types(self):
        assert AnonymousUser() != "AnonymousUser"

    def test_ne_is_the_inverse_of_eq(self):
        assert not (AnonymousUser() != AnonymousUser())

    def test_hash_is_constant(self):
        assert hash(AnonymousUser()) == hash(AnonymousUser()) == 1

    def test_usable_as_a_dict_key(self):
        assert {AnonymousUser(): "v"}[AnonymousUser()] == "v"

    def test_is_anonymous(self):
        assert AnonymousUser.is_anonymous() is True

    def test_is_not_authenticated(self):
        assert AnonymousUser.is_authenticated() is False

    def test_groups_is_empty(self):
        assert AnonymousUser().groups == []

    def test_group_permissions_is_an_empty_set(self):
        assert AnonymousUser.get_group_permissions() == set()


class TestAnonymousUserUnsupportedOperations:
    """Everything that would need a real user record raises."""

    @pytest.mark.parametrize(
        "call",
        [
            lambda u: u.save(),
            lambda u: u.delete(),
            lambda u: u.set_password("pw"),
            lambda u: u.check_password("pw"),
            lambda u: u.user_permissions,
            lambda u: u.get_all_permissions(),
            lambda u: u.has_perm("perm"),
            lambda u: u.has_module_perms("module"),
        ],
    )
    def test_raises_not_implemented(self, call):
        with pytest.raises(NotImplementedError):
            call(AnonymousUser())

    def test_has_perms_on_empty_list_is_vacuously_true(self):
        assert AnonymousUser().has_perms([]) is True

    def test_has_perms_propagates_the_has_perm_failure(self):
        with pytest.raises(NotImplementedError):
            AnonymousUser().has_perms(["a"])

    def test_has_perms_returns_true_when_every_check_passes(self):
        class Permissive(AnonymousUser):
            def has_perm(self, perm, obj=None):
                return True

        assert Permissive().has_perms(["a", "b"]) is True

    def test_has_perms_short_circuits_on_the_first_failure(self):
        checked = []

        class Denying(AnonymousUser):
            def has_perm(self, perm, obj=None):
                checked.append(perm)
                return False

        assert Denying().has_perms(["a", "b"]) is False
        assert checked == ["a"]


class TestFakeRequestWithoutARequest:
    def test_defaults(self):
        fake = FakeRequest()
        assert fake.is_secure() is False
        assert fake.get_host() == ""
        assert fake.path == ""
        assert fake.body == ""
        assert fake.build_absolute_uri() == ""

    def test_user_defaults_to_anonymous(self):
        assert FakeRequest().user == AnonymousUser()

    def test_query_dicts_default_to_empty(self):
        fake = FakeRequest()
        assert fake.GET == {}
        assert fake.POST == {}
        assert fake.REQUEST == {}

    def test_keyword_arguments_supply_values(self):
        fake = FakeRequest(path="/kw", body="kw-body")
        assert fake.path == "/kw"
        assert fake.body == "kw-body"

    def test_callable_keyword_arguments_are_invoked(self):
        fake = FakeRequest(is_secure=lambda: True, get_host=lambda: "kw-host")
        assert fake.is_secure() is True
        assert fake.get_host() == "kw-host"

    def test_falsy_keyword_value_is_kept_rather_than_defaulted(self):
        assert FakeRequest(path="").path == ""


class TestFakeRequestFromARequest:
    def test_copies_scalar_attributes(self):
        fake = FakeRequest(_Request())
        assert fake.path == "/some/path"
        assert fake.body == "payload"
        assert fake.user == "real-user"

    def test_invokes_callable_attributes(self):
        fake = FakeRequest(_Request())
        assert fake.is_secure() is True
        assert fake.get_host() == "example.com"

    def test_captures_the_absolute_uri(self):
        assert FakeRequest(_Request()).build_absolute_uri() == "https://example.com/some/path"

    def test_copies_the_query_dicts(self):
        fake = FakeRequest(_Request())
        assert fake.GET == {"a": "1"}
        assert fake.POST == {"b": "2"}
        assert fake.REQUEST == {"c": "3"}

    def test_query_dicts_are_snapshots_not_references(self):
        # The point of FakeRequest is to outlive the request, so later mutation
        # of the original must not be visible.
        request = _Request()
        fake = FakeRequest(request)
        request.GET["a"] = "mutated"
        assert fake.GET == {"a": "1"}

    def test_request_attributes_win_over_keyword_arguments(self):
        fake = FakeRequest(_Request(), path="/ignored")
        assert fake.path == "/some/path"

    def test_keyword_argument_fills_a_gap_in_the_request(self):
        request = _Request()
        del request.path
        assert FakeRequest(request, path="/from-kw").path == "/from-kw"

    def test_missing_attribute_falls_back_to_the_default(self):
        request = _Request()
        del request.body
        assert FakeRequest(request).body == ""
