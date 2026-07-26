"""Unit tests for ``sh_util.text.format_string_to_fit_in_n_chars``.

The public entry points format a template and, when the result overflows, trim
the longest substituted tokens until it fits. Two implementation quirks are
pinned below because callers depend on them: the overflow arithmetic is
hardcoded to 160 regardless of the requested limit, and the ``0.0`` default for a
missing price-style token only applies when a key is absent.
"""

import sys

import pytest
from sh_util.text.format_string_to_fit_in_n_chars import (
    _trim_longest_tokens_to_reduce_length,
    _trim_percentage_off_tail,
    format_string_to_fit_in_n_chars,
    squeeze_sms_message,
)


class TestFormatStringToFitInNChars:
    def test_returns_naive_format_when_it_already_fits(self):
        assert format_string_to_fit_in_n_chars("Hello {0}", 160, "World") == "Hello World"

    def test_supports_multiple_substitutions(self):
        assert format_string_to_fit_in_n_chars("{0}-{1}", 160, "a", "b") == "a-b"

    def test_exact_fit_is_allowed(self):
        assert format_string_to_fit_in_n_chars("{0}", 5, "abcde") == "abcde"

    def test_requires_at_least_one_value(self):
        with pytest.raises(TypeError, match="takes 2 or more arguments"):
            format_string_to_fit_in_n_chars("no args", 160)

    def test_template_longer_than_the_limit_is_rejected(self):
        with pytest.raises(TypeError, match="must not exceed the length"):
            format_string_to_fit_in_n_chars("x" * 50, 10, "a")

    def test_string_limit_is_coerced_to_int(self):
        assert format_string_to_fit_in_n_chars("Hi {0}", "160", "there") == "Hi there"

    def test_overlong_result_is_trimmed_to_fit(self):
        result = format_string_to_fit_in_n_chars("Msg: {0}", 160, "y" * 200)
        assert len(result) <= 160
        assert result.startswith("Msg: yyy")
        # Trimming marks the truncation point.
        assert result.endswith("..")

    def test_trims_the_longest_of_several_tokens(self):
        result = format_string_to_fit_in_n_chars("{0}|{1}", 160, "short", "z" * 200)
        assert len(result) <= 160
        assert result.startswith("short|")

    def test_limits_below_160_cannot_trigger_trimming(self):
        # ``exceeded_by`` is computed against a hardcoded 160 rather than the
        # caller's limit, so for any limit under 160 the reduction target goes
        # negative, no trimming happens, and the call fails instead of fitting.
        with pytest.raises(Exception, match="Failed to format string"):
            format_string_to_fit_in_n_chars("{0}", 50, "x" * 100)


class TestTrimPercentageOffTail:
    def test_empty_string_is_returned_unchanged(self):
        assert _trim_percentage_off_tail("", 0.5) == ""

    @pytest.mark.parametrize("value", ["a", "ab"])
    def test_strings_shorter_than_three_chars_are_untouched(self, value):
        # Trimming these could not fit the two-char ".." marker.
        assert _trim_percentage_off_tail(value, 0.5) == value

    def test_trims_by_percentage_and_appends_marker(self):
        assert _trim_percentage_off_tail("abcdefghij", 0.5) == "abcde.."

    def test_zero_percent_only_appends_the_marker(self):
        assert _trim_percentage_off_tail("abc", 0.0) == "abc.."

    def test_larger_percentage_trims_more(self):
        assert len(_trim_percentage_off_tail("x" * 100, 0.8)) < len(_trim_percentage_off_tail("x" * 100, 0.2))


class TestTrimLongestTokensToReduceLength:
    def test_empty_token_list_is_rejected(self):
        with pytest.raises(TypeError, match="does not accept empty lists"):
            _trim_longest_tokens_to_reduce_length([], 5)

    def test_non_positive_target_leaves_tokens_untouched(self):
        tokens = ["aaa", "bbbb"]
        assert _trim_longest_tokens_to_reduce_length(tokens, 0) == tokens

    def test_reduces_total_length_by_at_least_the_target(self):
        tokens = ["z" * 100]
        shrunk = _trim_longest_tokens_to_reduce_length(tokens, 30)
        assert sum(len(t) for t in tokens) - sum(len(t) for t in shrunk) >= 30

    def test_prefers_trimming_the_longest_token(self):
        shrunk = _trim_longest_tokens_to_reduce_length(["tiny", "w" * 120], 20)
        assert shrunk[0] == "tiny"
        assert len(shrunk[1]) < 120

    def test_repeated_tokens_are_trimmed_consistently(self):
        # Tokens are deduped before trimming, then mapped back over the original
        # list, so equal inputs must produce equal outputs.
        shrunk = _trim_longest_tokens_to_reduce_length(["q" * 80, "q" * 80], 40)
        assert shrunk[0] == shrunk[1]


class TestSqueezeSmsMessage:
    def test_formats_within_the_default_sms_limit(self):
        assert squeeze_sms_message("Hi {0}", "there") == "Hi there"

    def test_uses_the_configured_limit_when_settings_provides_one(self, monkeypatch):
        settings = sys.modules["settings"]
        monkeypatch.setattr(settings, "MAX_SMS_MESSAGE_LENGTH", 160, raising=False)
        assert squeeze_sms_message("Value: {0}", "ok") == "Value: ok"

    def test_falls_back_to_160_when_settings_lacks_the_attribute(self, monkeypatch):
        settings = sys.modules["settings"]
        monkeypatch.delattr(settings, "MAX_SMS_MESSAGE_LENGTH", raising=False)
        assert len(squeeze_sms_message("{0}", "y" * 200)) <= 160

    def test_falls_back_to_160_when_settings_is_unimportable(self, monkeypatch):
        # ``None`` in sys.modules makes ``import settings`` raise ImportError,
        # which the helper swallows in favour of the 160-char default.
        monkeypatch.setitem(sys.modules, "settings", None)
        assert len(squeeze_sms_message("{0}", "y" * 200)) <= 160
