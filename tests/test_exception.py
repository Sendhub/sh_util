"""Unit tests for ``sh_util.exception.ErrorResponse``.

A small JSON-serialisable error envelope. Both ``__str__`` and the legacy
``__unicode__`` dump the instance dict, so the field set is part of the wire
format and is asserted explicitly.
"""

import simplejson as json
from sh_util.exception import ErrorResponse


class TestErrorResponse:
    def test_message_is_required_and_others_default_to_empty(self):
        err = ErrorResponse("boom")
        assert err.message == "boom"
        assert err.dev_message == ""
        assert err.code == ""
        assert err.more_info == ""

    def test_all_fields_can_be_supplied(self):
        err = ErrorResponse("boom", dev_message="stacktrace", code="E42", more_info="https://docs")
        assert (err.message, err.dev_message, err.code, err.more_info) == (
            "boom",
            "stacktrace",
            "E42",
            "https://docs",
        )

    def test_str_serialises_every_field(self):
        assert json.loads(str(ErrorResponse("boom"))) == {
            "message": "boom",
            "dev_message": "",
            "code": "",
            "more_info": "",
        }

    def test_unicode_matches_str(self):
        err = ErrorResponse("boom", code="E1")
        assert err.__unicode__() == str(err)

    def test_positional_arguments_map_in_declaration_order(self):
        err = ErrorResponse("m", "d", "c", "i")
        assert json.loads(str(err)) == {
            "message": "m",
            "dev_message": "d",
            "code": "c",
            "more_info": "i",
        }

    def test_non_ascii_message_survives_serialisation(self):
        assert json.loads(str(ErrorResponse("¡boom!")))["message"] == "¡boom!"

    def test_attributes_added_later_appear_in_the_payload(self):
        # Serialisation reflects ``__dict__``, so it is not a fixed schema.
        err = ErrorResponse("boom")
        err.request_id = "req-1"
        assert json.loads(str(err))["request_id"] == "req-1"
