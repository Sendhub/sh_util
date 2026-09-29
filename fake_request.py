"""
Fake Django request object, useful for passing serializable request-like objects around.
"""

__author__ = "Jay Taylor [@jtaylor]"

try:
    from django.contrib.auth.models import AnonymousUser
except ImportError:

    class AnonymousUser:
        """
        Represents an anonymous user with default properties and methods.
        """

        id = None
        username = ""
        is_staff = False
        is_active = False
        is_superuser = False
        _groups = []
        _user_permissions = []

        def __init__(self):
            """
            Intentionally a no-op: all attributes are class-level defaults, so
            there is nothing instance-specific to initialize.
            """
            pass

        def __str__(self):
            return "AnonymousUser"

        def __eq__(self, other):
            return isinstance(other, self.__class__)

        def __ne__(self, other):
            return not self.__eq__(other)

        def __hash__(self):
            return 1  # Instances are always returning the same hash value.

        def save(self):
            """
            Raising NotImplementedError when attempting to save.
            """
            raise NotImplementedError

        def delete(self):
            """
            Raising NotImplementedError when attempting to delete.
            """
            raise NotImplementedError

        def set_password(self, raw_password):
            """
            Raising NotImplementedError when setting a password.
            """
            raise NotImplementedError

        def check_password(self, raw_password):
            """
            Raising NotImplementedError when checking a password.
            """
            raise NotImplementedError

        def _get_groups(self):
            """
            Returning the groups associated with the user.
            """
            return self._groups

        groups = property(_get_groups)

        def _get_user_permissions(self):
            """
            Raising NotImplementedError when getting user permissions.
            """
            raise NotImplementedError

        user_permissions = property(_get_user_permissions)

        @staticmethod
        def get_group_permissions():
            """
            Returning an empty set of group permissions.
            """
            return set()

        def get_all_permissions(self, obj=None):
            """
            Raising NotImplementedError when getting all permissions.
            """
            raise NotImplementedError

        def has_perm(self, perm, obj=None):
            """
            Raising NotImplementedError when checking a specific permission.
            """
            raise NotImplementedError

        def has_perms(self, perm_list, obj=None):
            """
            Checking if the user has all permissions in the provided list.
            """
            for perm in perm_list:
                if not self.has_perm(perm, obj):
                    return False
            return True

        def has_module_perms(self, module):
            """
            Raising NotImplementedError when checking module permissions.
            """
            raise NotImplementedError

        @staticmethod
        def is_anonymous():
            """
            Returning True to indicate the user is anonymous.
            """
            return True

        @staticmethod
        def is_authenticated():
            """
            Returning False to indicate the user is not authenticated.
            """
            return False


class FakeRequest:
    """
    Encapsulates static properties of a request required for VoiceCalls to work properly.
    This is necessary because Django Request objects cannot be serialized.
    """

    def __init__(self, request=None, **kw):
        """
        Initializes a new FakeRequest instance.

        Args:
            request: The original Django request object (optional).
            **kw: Additional keyword arguments for request attributes.
        """

        def _get_attribute_value(attribute_name, default=None):
            """
            Attempts to extract the named attribute from the request. If the
            attribute value is callable, the attribute will be invoked and the
            value returned.

            Args:
                attribute_name: The name of the attribute to retrieve.
                default: The default value to return if the attribute is not found.

            Returns:
                The value of the attribute or the default value.
            """
            if (request is not None and hasattr(request, attribute_name)) or attribute_name in kw:
                attribute = getattr(request, attribute_name) if hasattr(request, attribute_name) else kw.get(attribute_name)
                if callable(attribute):
                    return attribute()
                else:
                    return attribute
            else:
                return default

        self._is_secure = _get_attribute_value("is_secure", False)
        self._get_host = _get_attribute_value("get_host", "")
        self.path = _get_attribute_value("path", "")
        self.user = _get_attribute_value("user", AnonymousUser())
        self.body = _get_attribute_value("body", "")
        self._build_absolute_uri = request.build_absolute_uri() if request is not None else ""

        for attr in ("GET", "POST", "REQUEST"):
            if request is None:
                setattr(self, attr, {})
            else:
                setattr(self, attr, dict(getattr(request, attr).items()))

    def is_secure(self):
        """
        Indicates whether the request is secure.

        Returns:
            True if the request is secure, False otherwise.
        """
        return self._is_secure

    def get_host(self):
        """
        Retrieves the host of the request.

        Returns:
            The host as a string.
        """
        return self._get_host

    def build_absolute_uri(self):
        """
        Retrieves the absolute URI from the original request, if available.

        Returns:
            The absolute URI as a string.
        """
        return self._build_absolute_uri
