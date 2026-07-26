"""Unit tests for ``sh_util.text.split.split_string``.

Splits text into SMS-sized fragments, preferring word boundaries.

NOTE a significant quirk pinned below: the word-boundary search runs on *every*
slice, including one that already contains the whole remaining string. So a short
input that would fit in a single fragment is still split at its last space —
``split_string("hello world")`` returns ``["hello ", "world"]``, not
``["hello world"]``.
"""

import pytest
from sh_util.text.split import split_string


class TestSingleFragment:
    def test_text_with_no_whitespace_stays_whole(self):
        assert split_string("abcde", 10) == ["abcde"]

    def test_text_exactly_at_the_limit(self):
        assert split_string("abcde", 5) == ["abcde"]

    def test_empty_input_yields_no_fragments(self):
        assert split_string("", 10) == []

    def test_single_word_at_the_default_limit(self):
        assert split_string("supercalifragilistic") == ["supercalifragilistic"]


class TestWordBoundarySplitting:
    def test_short_text_is_still_split_at_its_last_space(self):
        # See the module docstring — this is the surprising behaviour.
        assert split_string("hello world") == ["hello ", "world"]

    def test_trailing_space_is_kept_on_the_leading_fragment(self):
        assert split_string("hello world")[0].endswith(" ")

    def test_narrow_limit_can_emit_a_whitespace_only_fragment(self):
        # With a 5-char window the second slice begins at the space, and the
        # boundary search leaves that space as a fragment of its own.
        assert split_string("hello world", 5) == ["hello", " ", "world"]

    def test_reassembling_the_fragments_restores_the_input(self):
        text = "the quick brown fox jumps over the lazy dog"
        assert "".join(split_string(text, 12)) == text


class TestHardSplitting:
    @pytest.mark.parametrize(
        "text,length,expected",
        [
            ("abcdefghij", 3, ["abc", "def", "ghi", "j"]),
            ("abcdefghij", 4, ["abcd", "efgh", "ij"]),
            ("abcdefghij", 5, ["abcde", "fghij"]),
        ],
    )
    def test_text_without_spaces_is_split_at_the_hard_limit(self, text, length, expected):
        assert split_string(text, length) == expected

    def test_no_fragment_exceeds_the_limit(self):
        for fragment in split_string("a" * 100, 7):
            assert len(fragment) <= 7

    def test_default_limit_is_160(self):
        fragments = split_string("x" * 400)
        assert [len(f) for f in fragments] == [160, 160, 80]


class TestMaxFragments:
    def test_remainder_is_returned_as_the_final_fragment(self):
        # Once the cap is reached the rest is emitted whole, even if it exceeds
        # the fragment length.
        assert split_string("one two three four", 5, 2) == ["one ", "two three four"]

    def test_single_fragment_cap_returns_everything(self):
        assert split_string("one two three", 5, 1) == ["one two three"]

    def test_cap_is_respected(self):
        assert len(split_string("a b c d e f g h", 3, 3)) == 3

    def test_negative_one_means_unlimited(self):
        assert len(split_string("a" * 50, 10, -1)) == 5

    def test_zero_cap_produces_nothing(self):
        # ``0`` is neither -1 nor positive, so the loop body never runs.
        assert split_string("anything", 5, 0) == []

    def test_cap_larger_than_needed_is_harmless(self):
        assert "".join(split_string("one two", 4, 99)) == "one two"
