# -*- coding: utf-8 -*-

"""
Fake Django request object, useful for passing serializable request-like
objects around.
"""

__author__ = 'Jay Taylor [@jtaylor]'

try:
    from django.contrib.auth.models import AnonymousUser
except ImportError:
    class AnonymousUser():
        """
        Anonymous User class implementation.
        """
        id = None
        username = ''
        is_staff = False
        is_active = False
        is_superuser = False
        _groups = []
        _user_permissions = []

        def __init__(self):
            pass

        def __unicode__(self):
            return 'AnonymousUser'

        def __str__(self):
            return str(self).encode('utf-8')

        def __eq__(self, other):
            return isinstance(other, self.__class__)

        def __ne__(self, other):
            return not self.__eq__(other)

        def __hash__(self):
            return 1  # instances always return the same hash value

        def save(self):
            """
            Save
            """
            raise NotImplementedError

        def delete(self):
            """
            Delete
            """
            raise NotImplementedError

        def set_password(self, raw_password):
            """
            Set Password
            """
            raise NotImplementedError

        def check_password(self, raw_password):
            """
            Check Password
            """
            raise NotImplementedError

        def _get_groups(self):
            """
            Get groups
            """
            return self._groups
        groups = property(_get_groups)

        def _get_user_permissions(self):
            """
            Get uSer Permission
            """
            raise NotImplementedError
        user_permissions = property(_get_user_permissions)

        @staticmethod
        def get_group_permissions():
            """
            Get group permission
            """
            return set()

        def get_all_permissions(self, obj=None):
            """
            Get all permission
            """
            raise NotImplementedError

        def has_perm(self, perm, obj=None):
            """
            check permission
            """
            raise NotImplementedError

        def has_perms(self, perm_list, obj=None):
            """
            check permission
            """
            for perm in perm_list:
                if not self.has_perm(perm, obj):
                    return False
            return True

        def has_module_perms(self, module):
            """
            Has module permission
            """
            raise NotImplementedError

        @staticmethod
        def is_anonymous():
            """
            check identity
            """
            return True

        @staticmethod
        def is_authenticated():
            """
            check if authentic
            """
            return False


class FakeRequest():
    """
    This encapsulates some of the static properties of a request which are
    required for VoiceCalls to work properly.  This is required because Django
    Request objects cannot be serialized.
    """

    def __init__(self, request=None, **kw):
        """Initialize a new FakeRequest instance."""

        def _get_attribute_value(attribute_name, default=None):
            """
            Attempts to extract the named attribute from the request.  if the
            attribute value is callable, the attribute will be invoked and the
            value returned.
            """
            if (request is not None and hasattr(request, attribute_name)) or attribute_name in kw:
                attribute = getattr(request, attribute_name) if \
                    hasattr(request, attribute_name) else kw.get(attribute_name)
                if callable(attribute):
                    return attribute()
                else:
                    return attribute
            else:
                return default

        self._is_secure = _get_attribute_value('is_secure', False)
        self._get_host = _get_attribute_value('get_host', '')
        self.path = _get_attribute_value('path', '')
        self.user = _get_attribute_value('user', AnonymousUser())
        self.body = _get_attribute_value('body', '')
        self._build_absolute_uri = request.build_absolute_uri() if \
            request is not None else ''

        for attr in ('GET', 'POST', 'REQUEST'):
            if request is None:
                setattr(self, attr, {})
            else:
                setattr(self, attr,
                    dict(
                        (k, v) for k, v in list(getattr(request, attr).items())
                    ))

    def is_secure(self):
        """Part of django Request objects."""
        return self._is_secure

    def get_host(self):
        """Part of django Request objects."""
        return self._get_host

    def build_absolute_uri(self):
        """Copy of value from original request, when possible."""
        return self._build_absolute_uri
