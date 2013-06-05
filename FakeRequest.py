# -*- coding: utf-8 -*-

"""
Fake Django request object, useful for passing serializable request-like
objects around.
"""

__author__ = 'Jay Taylor [@jtaylor]'

try:
    from django.contrib.auth.models import AnonymousUser
except ImportError:
    class AnonymousUser(object):
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
            return unicode(self).encode('utf-8')

        def __eq__(self, other):
            return isinstance(other, self.__class__)

        def __ne__(self, other):
            return not self.__eq__(other)

        def __hash__(self):
            return 1 # instances always return the same hash value

        def save(self):
            raise NotImplementedError

        def delete(self):
            raise NotImplementedError

        def set_password(self, raw_password):
            raise NotImplementedError

        def check_password(self, raw_password):
            raise NotImplementedError

        def _get_groups(self):
            return self._groups
        groups = property(_get_groups)

        def _get_user_permissions(self):
            raise NotImplementedError
        user_permissions = property(_get_user_permissions)

        def get_group_permissions(self, obj=None):
            return set()

        def get_all_permissions(self, obj=None):
            raise NotImplementedError

        def has_perm(self, perm, obj=None):
            raise NotImplementedError

        def has_perms(self, perm_list, obj=None):
            for perm in perm_list:
                if not self.has_perm(perm, obj):
                    return False
            return True

        def has_module_perms(self, module):
            raise NotImplementedError

        def is_anonymous(self):
            return True

        def is_authenticated(self):
            return False


class FakeRequest(object):
    """
    This encapsulates some of the static properties of a request which are
    required for VoiceCalls to work properly.  This is required because Django
    Request objects cannot be serialized.
    """

    def __init__(self, request=None, **kw):
        """Initialize a new FakeRequest instance."""

        def _getAttributeValue(attributeName, default=None):
            """
            Attempts to extract the named attribute from the request.  if the
            attribute value is callable, the attribute will be invoked and the
            value returned.
            """
            if (request is not None and hasattr(request, attributeName)) or \
                attributeName in kw:
                attribute = getattr(request, attributeName) if \
                    hasattr(request, attributeName) else kw.get(attributeName)
                if callable(attribute):
                    return attribute()
                else:
                    return attribute
            else:
                return default

        self._is_secure = _getAttributeValue('is_secure', False)
        self._get_host = _getAttributeValue('get_host', '')
        self.path = _getAttributeValue('path', '')
        self.user = _getAttributeValue('user', AnonymousUser())
        self.body = _getAttributeValue('body', '')
        self._build_absolute_uri = request.build_absolute_uri() if \
            request is not None else ''

        for attr in ('GET', 'POST', 'REQUEST'):
            if request is None:
                setattr(self, attr, {})
            else:
                setattr(
                    self,
                    attr,
                    dict([
                        (k, v) for k, v in getattr(request, attr).items()
                    ])
                )

    def is_secure(self):
        """Part of django Request objects."""
        return self._is_secure

    def get_host(self):
        """Part of django Request objects."""
        return self._get_host

    def build_absolute_uri(self):
        """Copy of value from original request, when possible."""
        return self._build_absolute_uri

