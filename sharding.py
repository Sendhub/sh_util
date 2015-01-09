# -*- coding: utf-8 -*-

"""Sharding resources."""

__author__ = 'Jay Taylor [@jtaylor]'


class ShardedAuthenticationMiddleware(object):
    """Shard selection django middleware."""
    @staticmethod
    def process_request(request):
        """Override for the stock django authentication middleware."""

        # Importing ShUser makes sure monkeypatching is loaded
        try:
            from main.models import ShUser  # NOQA
        except Exception:
            pass

        from django.contrib.auth import get_user
        from django.utils.functional import SimpleLazyObject

        assert hasattr(request, 'session'), 'The Django authentication ' \
            'middleware requires session middleware to be installed. Edit ' \
            'your MIDDLEWARE_CLASSES setting to insert ' \
            '"django.contrib.sessions.middleware.SessionMiddleware".'

        request.user = SimpleLazyObject(lambda: get_user(request))

        return None
