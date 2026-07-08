"""
Module: user_agent

This module provides utility functions to parse and extract information from the SendHub User Agent string.

Functions:
    - get_sendhub_user_agent_string(request):
        Retrieves the SendHub User Agent string from the request object.

    - get_sendhub_user_agent_props(request):
        Converts the SendHub User Agent string into a dictionary mapping keys to values.

    - get_client_app_build_number(request):
        Extracts the client app build number from the SendHub User Agent string.

    - get_client_platform_type(request):
        Determines the client platform type (e.g., iOS, Android, or web) from the SendHub User Agent string.

Constants:
    - _client_app_build_number_re: Regex pattern to match the app version and build number.
    - _client_app_platform_type_re: Regex pattern to match the platform type.
    - _client_ios_platform_type: Regex pattern to identify iOS platforms.
    - _client_android_platform_type: Regex pattern to identify Android platforms.
"""

import itertools
import re

# Matches the Build number in the user agent string.

_client_app_build_number_re = re.compile(r"AppVersion\:\:(?P<versionName>[^\s:]+) \((?P<buildNumber>[0-9]+)\)$", re.IGNORECASE)  # noqa
_client_app_platform_type_re = re.compile(r"Platform\:\:(?P<platformType>[a-z\s]+)/", re.IGNORECASE)  # noqa
_client_ios_platform_type = re.compile(r"iOS|iPhone|iPod|iPad", re.IGNORECASE)
_client_android_platform_type = re.compile(r"Android", re.IGNORECASE)


def get_sendhub_user_agent_string(request):
    """
    Retrieves the SendHub User Agent string from the request object.

    Args:
        request: The HTTP request object containing metadata.

    Returns:
        str: The SendHub User Agent string if present, otherwise None.
    """
    sh_user_agent_str = None
    if hasattr(request, "META") and "HTTP_X_SH_USER_AGENT" in request.META:
        sh_user_agent_str = request.META.get("HTTP_X_SH_USER_AGENT", None)
    return sh_user_agent_str


def get_sendhub_user_agent_props(request):
    """
    Converts the SendHub User Agent string into a dictionary mapping keys to values.

    Args:
        request: The HTTP request object containing the SendHub User Agent string.
            Example: 'Platform::iOS/OSVersion::6.1/AppVersion::2.9TF (0134)'

    Returns:
        dict: A dictionary mapping keys to values extracted from the User Agent string.
            Example: {'AppVersion': '2.9TF (0134)', 'OSVersion': '6.1', 'Platform': 'iOS'}
    """
    agent_str = get_sendhub_user_agent_string(request)
    props = {}
    if agent_str is not None:
        prop_pairs = agent_str.split("/")
        props_serial = []
        [props_serial.extend(pair.split("::")) for pair in prop_pairs]
        props = dict(itertools.zip_longest(*[iter(props_serial)] * 2, fillvalue=""))  # noqa
    return props


def get_client_app_build_number(request):
    """
    Extracts the client app build number from the SendHub User Agent string.

    Args:
        request: The HTTP request object containing the SendHub User Agent string.

    Returns:
        int: The client app build number if present, otherwise -1.
    """
    sh_user_agent_str = get_sendhub_user_agent_string(request)
    build_number = -1
    if sh_user_agent_str is not None:
        matches = _client_app_build_number_re.search(sh_user_agent_str)
        if matches is not None:
            build_number = int(matches.group("buildNumber"))

    return build_number


def get_client_platform_type(request):
    """
    Determines the client platform type (e.g., iOS, Android, or web) from the SendHub User Agent string.

    Args:
        request: The HTTP request object containing the SendHub User Agent string.

    Returns:
        str: The client platform type ('ios', 'android', or 'web').
    """
    sh_user_agent_str = get_sendhub_user_agent_string(request)
    platform_type = "web"

    if sh_user_agent_str is not None:
        matches = _client_app_platform_type_re.search(sh_user_agent_str)
        if matches is not None:
            platform_type = matches.group("platformType")
            if _client_ios_platform_type.match(platform_type):
                platform_type = "ios"
            elif _client_android_platform_type.match(platform_type):
                platform_type = "android"

    return platform_type
