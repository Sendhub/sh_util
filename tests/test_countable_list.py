"""Unit tests for ``sh_util.countable_list``.

``CountableList`` is a list whose ``count()`` reports a caller-supplied total
rather than counting its own members — it exists so a paginated page of results
can advertise the full result-set size without a second query.
"""

from sh_util.countable_list import CountableList


class TestCountableList:
    def test_behaves_as_a_list(self):
        assert CountableList([1, 2, 3], 99) == [1, 2, 3]

    def test_count_reports_the_supplied_total_not_the_length(self):
        page = CountableList([1, 2, 3], 99)
        assert len(page) == 3
        assert page.count() == 99

    def test_empty_page_can_still_report_a_total(self):
        page = CountableList([], 42)
        assert list(page) == []
        assert page.count() == 42

    def test_meta_defaults_to_an_empty_dict(self):
        assert CountableList([1], 1).meta == {}

    def test_meta_is_stored_when_provided(self):
        assert CountableList([1], 1, meta={"page": 2}).meta == {"page": 2}

    def test_meta_defaults_are_not_shared_between_instances(self):
        first = CountableList([], 0)
        first.meta["only"] = "mine"
        assert CountableList([], 0).meta == {}

    def test_accepts_any_iterable(self):
        assert CountableList(range(3), 3) == [0, 1, 2]

    def test_list_mutation_does_not_change_the_count(self):
        page = CountableList([1], 10)
        page.append(2)
        assert page.count() == 10
