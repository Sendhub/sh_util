"""Unit tests for ``sh_util.temporal``.

NOTE: ``parse_iso8601_utc_datestring`` is currently broken for the very format its
docstring advertises. It strips 8 trailing characters to remove the timezone, but
the timezone is only 5 characters (``+0000``), so the seconds are eaten too and
``strptime`` always fails. See
``TestParseIso8601UtcDatestring.test_documented_format_is_rejected`` — these tests
pin the present behaviour rather than the intended behaviour.
"""

import re
import time
from datetime import datetime

import pytest
from sh_util.temporal import (
    epoch,
    parse_iso8601_utc_datestring,
    pretty_utc_timestamp,
    week_start_date_string,
)


class TestEpoch:
    def test_returns_an_int(self):
        assert isinstance(epoch(), int)

    def test_tracks_wall_clock_time(self):
        assert abs(epoch() - int(time.time())) <= 1

    def test_is_monotonic_across_calls(self):
        assert epoch() <= epoch()


class TestParseIso8601UtcDatestring:
    def test_rejects_strings_that_are_not_24_characters(self):
        with pytest.raises(Exception, match="must be 24 characters long"):
            parse_iso8601_utc_datestring("2010-04-13")

    def test_rejects_strings_longer_than_24_characters(self):
        with pytest.raises(Exception, match="must be 24 characters long"):
            parse_iso8601_utc_datestring("2010-04-13T15:29:40+0000extra")

    def test_documented_format_is_rejected(self):
        # BUG: the docstring's own example cannot be parsed. ``date_string[:-8]``
        # trims the 5-char offset *and* the ":40" seconds, leaving
        # "2010-04-13T15:29", which does not match "%Y-%m-%dT%H:%M:%S".
        # A ``[:-5]`` slice would be correct.
        with pytest.raises(ValueError, match="does not match format"):
            parse_iso8601_utc_datestring("2010-04-13T15:29:40+0000")

    def test_positive_offset_is_subtracted(self):
        # Only inputs whose first 16 chars happen to form a full timestamp get
        # through, hence this deliberately odd literal: chars 16-18 are discarded
        # by the over-wide slice and chars 19-23 are read as the offset.
        assert parse_iso8601_utc_datestring("2010-4-3T15:29:4ZZZ+0530") == datetime(2010, 4, 3, 9, 59, 4)

    def test_negative_offset_is_added(self):
        assert parse_iso8601_utc_datestring("2010-4-3T15:29:4ZZZ-0530") == datetime(2010, 4, 3, 20, 59, 4)

    def test_zero_offset_leaves_the_time_untouched(self):
        assert parse_iso8601_utc_datestring("2010-4-3T15:29:4ZZZ+0000") == datetime(2010, 4, 3, 15, 29, 4)

    def test_returns_a_naive_datetime(self):
        assert parse_iso8601_utc_datestring("2010-4-3T15:29:4ZZZ+0000").tzinfo is None


class TestPrettyUtcTimestamp:
    def test_formats_the_supplied_datetime(self):
        assert pretty_utc_timestamp(datetime(2026, 7, 25, 14, 30, 5)) == "2026-07-25 14:30:05 UTC"

    def test_defaults_to_now(self):
        assert re.match(r"\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC\Z", pretty_utc_timestamp())

    def test_pads_single_digit_components(self):
        assert pretty_utc_timestamp(datetime(2026, 1, 2, 3, 4, 5)) == "2026-01-02 03:04:05 UTC"


class TestWeekStartDateString:
    @pytest.mark.parametrize(
        "date,expected",
        [
            (datetime(2026, 7, 20), "2026-07-20"),  # a Monday maps to itself
            (datetime(2026, 7, 21), "2026-07-20"),  # Tuesday
            (datetime(2026, 7, 25), "2026-07-20"),  # Saturday
            (datetime(2026, 7, 26), "2026-07-20"),  # Sunday, end of the same week
            (datetime(2026, 7, 27), "2026-07-27"),  # next Monday rolls over
        ],
    )
    def test_returns_the_monday_of_that_week(self, date, expected):
        assert week_start_date_string(date) == expected

    def test_crosses_month_boundaries(self):
        assert week_start_date_string(datetime(2026, 8, 2)) == "2026-07-27"

    def test_crosses_year_boundaries(self):
        assert week_start_date_string(datetime(2027, 1, 1)) == "2026-12-28"

    def test_defaults_to_the_current_week(self):
        result = week_start_date_string()
        assert re.match(r"\A\d{4}-\d{2}-\d{2}\Z", result)
        assert datetime.strptime(result, "%Y-%m-%d").weekday() == 0
