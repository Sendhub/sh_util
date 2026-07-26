"""Unit tests for ``sh_util.crypto.hash_generator``.

``HashGenerator`` chains SHA-256 digests off a self-rotating salt to produce
hashes of arbitrary length. The salt seeds the global ``random`` state, so these
tests assert on shape and uniqueness rather than on exact values.
"""

import re
from unittest import mock

import pytest
from sh_util.crypto import hash_generator as hash_generator_module
from sh_util.crypto.hash_generator import HashGenerator, generate_hash_set

_HEX = re.compile(r"\A[0-9a-f]*\Z")


class TestHashGeneratorGenerate:
    def test_default_length_is_64(self):
        assert len(HashGenerator().generate()) == 64

    @pytest.mark.parametrize("length", [1, 10, 63, 64])
    def test_respects_lengths_within_one_digest(self, length):
        assert len(HashGenerator().generate(length)) == length

    @pytest.mark.parametrize("length", [65, 100, 200])
    def test_concatenates_digests_for_longer_lengths(self, length):
        # A single sha256 hexdigest is 64 chars, so anything longer forces the
        # loop to chain additional digests.
        assert len(HashGenerator().generate(length)) == length

    def test_zero_length_returns_empty_string(self):
        assert HashGenerator().generate(0) == ""

    def test_output_is_lowercase_hex(self):
        assert _HEX.match(HashGenerator().generate(128))

    def test_successive_calls_differ(self):
        generator = HashGenerator()
        assert generator.generate() != generator.generate()

    def test_salt_rotates_on_each_hash(self):
        generator = HashGenerator()
        before = generator._salt
        generator.generate(1)
        assert generator._salt != before


class TestHashGeneratorSalt:
    def test_extra_salt_is_accepted(self):
        assert len(HashGenerator(extra_salt="tenant-42").generate(32)) == 32

    def test_instances_are_independent(self):
        assert HashGenerator(extra_salt="a").generate() != HashGenerator(extra_salt="b").generate()

    def test_random_str_length_is_within_requested_bounds(self):
        generator = HashGenerator()
        value = generator._random_str(min_length=10, max_length=20)
        assert 10 <= len(value) < 20

    def test_next_salt_embeds_a_timestamp_separator(self):
        # The salt is "<random>:<iso8601>", so it always contains a colon.
        assert ":" in HashGenerator()._next_salt()


class TestGenerateHashSet:
    def test_zero_quantity_returns_empty_list(self):
        assert generate_hash_set(0) == []

    def test_returns_the_requested_quantity(self):
        assert len(generate_hash_set(5)) == 5

    def test_entries_are_unique(self):
        digests = generate_hash_set(10)
        assert len(set(digests)) == 10

    def test_honours_the_length_argument(self):
        assert all(len(d) == 16 for d in generate_hash_set(4, 16))

    def test_negative_quantity_is_rejected(self):
        with pytest.raises(AssertionError):
            generate_hash_set(-1)

    def test_numeric_string_quantity_is_accepted_by_the_assert(self):
        # The guard is ``int(quantity) >= 0`` but ``range(quantity)`` needs a real
        # int, so a string quantity passes the assert and then fails.
        with pytest.raises(TypeError):
            generate_hash_set("3")

    def test_duplicate_digests_are_topped_up(self, monkeypatch):
        # The set comprehension collapses duplicates, so the top-up loop has to
        # keep generating until the requested quantity of distinct values exists.
        fake = mock.Mock()
        fake.generate.side_effect = ["dup", "dup", "dup", "second", "third"]
        monkeypatch.setattr(hash_generator_module, "_generator", fake)

        digests = generate_hash_set(3)

        assert sorted(digests) == ["dup", "second", "third"]
        assert fake.generate.call_count == 5
