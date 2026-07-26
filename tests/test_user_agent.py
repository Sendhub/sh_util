"""Unit tests for ``sh_util.user_agent``.

These helpers parse the custom ``X-SH-User-Agent`` header, shaped like
``Platform::iOS/OSVersion::6.1/AppVersion::2.9TF (0134)``. Everything reads the
header off a Django-style ``request.META`` mapping.
"""

from types import SimpleNamespace

import pytest
from sh_util.user_agent import (
    get_client_app_build_number,
    get_client_platform_type,
    get_sendhub_user_agent_props,
    get_sendhub_user_agent_string,
)

_IOS_AGENT = "Platform::iOS/OSVersion::6.1/AppVersion::2.9TF (0134)"
_ANDROID_AGENT = "Platform::Android/OSVersion::9/AppVersion::3.1 (0042)"


def _request(agent=None, with_meta=True):
    """Build a stand-in request.

    Args:
        agent: header value; omitted from META entirely when None.
        with_meta: when False the object has no ``META`` at all, mimicking a
            non-Django request object.
    """
    if not with_meta:
        return SimpleNamespace()
    meta = {} if agent is None else {"HTTP_X_SH_USER_AGENT": agent}
    return SimpleNamespace(META=meta)


class TestGetSendhubUserAgentString:
    def test_returns_the_header_value(self):
        assert get_sendhub_user_agent_string(_request(_IOS_AGENT)) == _IOS_AGENT

    def test_returns_none_when_header_absent(self):
        assert get_sendhub_user_agent_string(_request()) is None

    def test_returns_none_when_request_has_no_meta(self):
        assert get_sendhub_user_agent_string(_request(with_meta=False)) is None

    def test_empty_header_value_is_returned_as_is(self):
        assert get_sendhub_user_agent_string(_request("")) == ""


class TestGetSendhubUserAgentProps:
    def test_parses_all_pairs(self):
        assert get_sendhub_user_agent_props(_request(_IOS_AGENT)) == {
            "Platform": "iOS",
            "OSVersion": "6.1",
            "AppVersion": "2.9TF (0134)",
        }

    def test_returns_empty_dict_without_a_header(self):
        assert get_sendhub_user_agent_props(_request()) == {}

    def test_single_pair(self):
        assert get_sendhub_user_agent_props(_request("Platform::web")) == {"Platform": "web"}

    def test_odd_token_count_is_padded_with_empty_string(self):
        # A trailing key with no "::value" leaves an unpaired token, which
        # zip_longest fills rather than dropping.
        assert get_sendhub_user_agent_props(_request("Platform::iOS/Dangling")) == {
            "Platform": "iOS",
            "Dangling": "",
        }


class TestGetClientAppBuildNumber:
    def test_extracts_build_number_as_int(self):
        assert get_client_app_build_number(_request(_IOS_AGENT)) == 134

    def test_returns_minus_one_without_a_header(self):
        assert get_client_app_build_number(_request()) == -1

    def test_returns_minus_one_when_pattern_does_not_match(self):
        assert get_client_app_build_number(_request("Platform::iOS/OSVersion::6.1")) == -1

    def test_build_number_must_terminate_the_string(self):
        # The pattern is anchored with $; trailing content means no match.
        assert get_client_app_build_number(_request(_IOS_AGENT + "/Extra::x")) == -1

    def test_leading_zeros_are_stripped_by_int_conversion(self):
        assert get_client_app_build_number(_request("AppVersion::1.0 (0007)")) == 7


class TestGetClientPlatformType:
    @pytest.mark.parametrize(
        "agent,expected",
        [
            (_IOS_AGENT, "ios"),
            ("Platform::iPhone/AppVersion::1.0 (1)", "ios"),
            ("Platform::iPad/AppVersion::1.0 (1)", "ios"),
            ("Platform::iPod/AppVersion::1.0 (1)", "ios"),
            (_ANDROID_AGENT, "android"),
            ("Platform::android/AppVersion::1.0 (1)", "android"),
        ],
    )
    def test_recognised_platforms(self, agent, expected):
        assert get_client_platform_type(_request(agent)) == expected

    def test_defaults_to_web_without_a_header(self):
        assert get_client_platform_type(_request()) == "web"

    def test_defaults_to_web_when_platform_token_is_absent(self):
        assert get_client_platform_type(_request("OSVersion::6.1/AppVersion::1.0 (1)")) == "web"

    def test_unrecognised_platform_is_returned_verbatim(self):
        # Neither the iOS nor the Android pattern matches, so the raw captured
        # token falls through — it is not normalised to "web".
        assert get_client_platform_type(_request("Platform::Windows Phone/AppVersion::1.0 (1)")) == "Windows Phone"

    def test_platform_capture_requires_a_trailing_slash(self):
        assert get_client_platform_type(_request("Platform::iOS")) == "web"
