# tests/test_bandwidth.py
"""
Comprehensive pytest examples:
- unit tests for pure functions
- mocking external HTTP calls using unittest.mock.patch + MagicMock
- testing DB interactions with both MagicMock (unit) and sqlite3 in-memory (integration)
- parametrized tests and edge cases
"""

import sqlite3
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import requests

from sh_util.tel import bandwidth as bandwidth
from sh_util.tel import twilio_util
from sh_util.tel.twilio_util import (
    AreaCodeUnavailableError,
    sanitize,
    twilio_buy_toll_free_phone_number,
    twilio_phone_number_properties,
)

# ---------------------------
# Pure function tests
# ---------------------------


@pytest.mark.parametrize(
    "bits,seconds,expected",
    [
        (1000, 2, 500.0),
        (0, 1, 0.0),
        (5, 2, 2.5),
    ],
)
def test_calculate_bandwidth_normal(bits, seconds, expected):
    assert bandwidth.calculate_bandwidth(bits, seconds) == expected


def test_calculate_bandwidth_zero_seconds():
    with pytest.raises(ZeroDivisionError):
        bandwidth.calculate_bandwidth(1000, 0)


# ---------------------------
# Tests for fetch_number_info (external API)
# ---------------------------


def make_response(status_code=200, json_data=None):
    """Helper to create a MagicMock response object for requests.get"""
    m = MagicMock()
    m.status_code = status_code
    m.json = MagicMock(return_value=(json_data if json_data is not None else {}))
    return m


@patch("requests.get")
def test_fetch_number_info_success(mock_get):
    """API returns 200 and a valid payload"""
    payload = {"available": True, "price": 1.23}
    mock_get.return_value = make_response(200, payload)

    res = bandwidth.fetch_number_info("https://api.example", "12345")
    assert res["available"] is True
    assert pytest.approx(res["price"], rel=1e-6) == 1.23
    mock_get.assert_called_once()


@patch("requests.get")
def test_fetch_number_info_network_failure(mock_get):
    """Network-level RequestException is wrapped as ExternalAPIError"""
    mock_get.side_effect = requests.RequestException("boom")
    with pytest.raises(bandwidth.ExternalAPIError):
        bandwidth.fetch_number_info("https://api.example", "12345")


@patch("requests.get")
def test_fetch_number_info_non_200(mock_get):
    mock_get.return_value = make_response(500, {"available": False})
    with pytest.raises(bandwidth.ExternalAPIError):
        bandwidth.fetch_number_info("https://api.example", "12345")


@patch("requests.get")
def test_fetch_number_info_invalid_payload(mock_get):
    # missing 'available' key
    mock_get.return_value = make_response(200, {"price": 0.5})
    with pytest.raises(bandwidth.ExternalAPIError):
        bandwidth.fetch_number_info("https://api.example", "12345")


# ---------------------------
# Tests for store_purchase (DB interactions)
# ---------------------------


def test_store_purchase_with_magicmock_db():
    """Unit test: pass in a duck-typed DB object (MagicMock) and ensure it's used."""
    fake_db = MagicMock()
    fake_res = MagicMock()
    fake_res.lastrowid = 42
    fake_db.execute.return_value = fake_res
    # allow commit to be called and succeed
    fake_db.commit.return_value = None

    rowid = bandwidth.store_purchase(fake_db, "12345", {"a": 1})
    assert rowid == 42
    fake_db.execute.assert_called_once()
    fake_db.commit.assert_called_once()


def test_store_purchase_commit_exception_is_swallowed():
    """Duck-typed DB whose commit() raises - the exception is caught and ignored."""
    fake_db = MagicMock()
    fake_res = MagicMock()
    fake_res.lastrowid = 7
    fake_db.execute.return_value = fake_res
    fake_db.commit.side_effect = Exception("commit not supported")

    rowid = bandwidth.store_purchase(fake_db, "12345", {"a": 1})

    assert rowid == 7
    fake_db.commit.assert_called_once()


def test_store_purchase_falls_back_to_rowid_attribute():
    """When the result has no `lastrowid` but does have `rowid`, use that."""
    fake_db = MagicMock()
    fake_res = MagicMock(spec=["rowid"])
    fake_res.rowid = 55
    fake_db.execute.return_value = fake_res

    rowid = bandwidth.store_purchase(fake_db, "12345", {"a": 1})

    assert rowid == 55


def test_store_purchase_falls_back_to_zero_when_no_id_attribute():
    """When the result has neither `lastrowid` nor `rowid`, fall back to 0."""
    fake_db = MagicMock()
    fake_res = MagicMock(spec=[])
    fake_db.execute.return_value = fake_res

    rowid = bandwidth.store_purchase(fake_db, "12345", {"a": 1})

    assert rowid == 0


def test_store_purchase_sqlite_integration(tmp_path):
    """Integration-style test: use sqlite3 in-memory DB and verify insertion."""
    # create an in-memory DB and table
    conn = sqlite3.connect(":memory:")
    conn.execute("""
    CREATE TABLE purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone_number TEXT,
        metadata TEXT
    );
    """)
    conn.commit()

    rowid = bandwidth.store_purchase(conn, "555-0100", {"hello": "world"})
    assert isinstance(rowid, int)
    cur = conn.execute("SELECT phone_number, metadata FROM purchases WHERE id = ?", (rowid,))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "555-0100"
    assert "hello" in row[1]
    conn.close()


# ---------------------------
# Tests for purchase_number orchestration
# ---------------------------


@patch("sh_util.tel.bandwidth.fetch_number_info")
@patch("sh_util.tel.bandwidth.store_purchase")
def test_purchase_number_prefers_available(mock_store, mock_fetch):
    """
    If API says available=True and prefer_available=True, we should store and return purchased True.
    """
    mock_fetch.return_value = {"available": True, "price": 2.5}
    mock_store.return_value = 123
    result = bandwidth.purchase_number("https://api", "99", MagicMock(), prefer_available=True)
    assert result["purchased"] is True
    assert result["price"] == 2.5
    assert result["rowid"] == 123
    mock_fetch.assert_called_once()
    mock_store.assert_called_once()


@patch("sh_util.tel.bandwidth.fetch_number_info")
@patch("sh_util.tel.bandwidth.store_purchase")
def test_purchase_number_respects_prefer_available_flag(mock_store, mock_fetch):
    """
    If API says available=False but prefer_available=False, we should still purchase (force) — demo of option.
    """
    mock_fetch.return_value = {"available": False, "price": 7.0}
    mock_store.return_value = 99
    result = bandwidth.purchase_number("https://api", "77", MagicMock(), prefer_available=False)
    assert result["purchased"] is True
    assert result["rowid"] == 99


@patch("sh_util.tel.bandwidth.fetch_number_info")
def test_purchase_number_not_available_no_purchase(mock_fetch):
    """If prefer_available=True and not available, we should not purchase."""
    mock_fetch.return_value = {"available": False, "price": 3.33}
    fake_db = MagicMock()
    result = bandwidth.purchase_number("https://api", "12", fake_db, prefer_available=True)
    assert result["purchased"] is False
    assert result["rowid"] is None
    fake_db.execute.assert_not_called()


@patch("sh_util.tel.bandwidth.fetch_number_info")
def test_purchase_number_api_failure_bubbles_up(mock_fetch):
    mock_fetch.side_effect = bandwidth.ExternalAPIError("api died")
    with pytest.raises(bandwidth.ExternalAPIError):
        bandwidth.purchase_number("https://api", "0", MagicMock())


# ---------------------------
# Edge case: malformed price values
# ---------------------------


@patch("sh_util.tel.bandwidth.fetch_number_info")
@patch("sh_util.tel.bandwidth.store_purchase")
@pytest.mark.parametrize(
    "raw_price,expected_price",
    [
        ("10", 10.0),
        (10, 10.0),
        ("2.5", 2.5),
        (0, 0.0),
    ],
)
def test_purchase_number_coerces_numeric_price_to_float(mock_store, mock_fetch, raw_price, expected_price):
    """Numeric prices — including numeric strings — are returned as floats."""
    mock_fetch.return_value = {"available": True, "price": raw_price}
    mock_store.return_value = 1

    res = bandwidth.purchase_number("https://api", "X", MagicMock())

    assert isinstance(res["price"], float)
    assert res["price"] == expected_price


@patch("sh_util.tel.bandwidth.fetch_number_info")
@patch("sh_util.tel.bandwidth.store_purchase")
@pytest.mark.parametrize(
    "raw_price,expected_exc",
    [
        (None, TypeError),
        ("abc", ValueError),
        ("", ValueError),
    ],
)
def test_purchase_number_rejects_unparseable_price(mock_store, mock_fetch, raw_price, expected_exc):
    """An unparseable price propagates instead of falling back to 0.0.

    ``purchase_number`` calls ``float(info.get("price", 0.0))`` with no guard, so
    the 0.0 default only applies when the key is *absent* — a present-but-None or
    non-numeric price reaches ``float()`` and raises.
    """
    mock_fetch.return_value = {"available": True, "price": raw_price}
    mock_store.return_value = 1

    with pytest.raises(expected_exc):
        bandwidth.purchase_number("https://api", "X", MagicMock())

    # The coercion happens before the purchase, so nothing is written.
    mock_store.assert_not_called()


@patch("sh_util.tel.bandwidth.fetch_number_info")
@patch("sh_util.tel.bandwidth.store_purchase")
def test_purchase_number_defaults_price_when_key_is_absent(mock_store, mock_fetch):
    mock_fetch.return_value = {"available": True}
    mock_store.return_value = 1

    res = bandwidth.purchase_number("https://api", "X", MagicMock())

    assert res["price"] == 0.0


# ---------------------------
# Utilities & manual-run examples
# ---------------------------


# ---------------------------
# Tests for twilio_util.py gaps: twilio_phone_number_properties, sanitize,
# twilio_find_toll_free_number_in_area_code, twilio_buy_toll_free_phone_number
# ---------------------------


@pytest.fixture
def mock_twilio_client():
    client = MagicMock()
    client.username = "AC_test_account"
    return client


class TestTwilioPhoneNumberProperties:
    def test_maps_all_expected_fields(self):
        avail = MagicMock()
        avail.friendly_name = "(415) 555-1234"
        avail.phone_number = "+14155551234"
        avail.lata = "722"
        avail.locality = "San Francisco"
        avail.rate_center = "SNFC"
        avail.latitude = "37.77"
        avail.longitude = "-122.42"
        avail.region = "CA"
        avail.postal_code = "94105"
        avail.iso_country = "US"
        avail.address_requirements = "none"
        avail.beta = False
        avail.capabilities = {"voice": True}
        avail.account_sid = "AC123"
        avail.country_code = "US"
        # `avail` is a MagicMock, so `hasattr(avail, "_solution")` is True
        # (auto-vivified) - matching the real AvailablePhoneNumberInstance,
        # which always carries a `_solution` dict.

        result = twilio_phone_number_properties(avail)

        assert result["friendly_name"] == "(415) 555-1234"
        assert result["phone_number"] == "+14155551234"
        assert result["region"] == "CA"
        assert result["_solution"]["account_sid"] == "AC123"
        assert result["_solution"]["country_code"] == "US"

    def test_missing_attributes_default_to_none(self):
        avail = object()  # no attributes at all

        result = twilio_phone_number_properties(avail)

        assert result["friendly_name"] is None
        assert result["phone_number"] is None
        assert result["beta"] is False
        assert result["_solution"]["account_sid"] is None
        assert result["_solution"]["country_code"] is None


class TestSanitize:
    def test_decimal_rounded_to_float(self):
        assert sanitize(Decimal("1.23456789")) == round(1.23456789, 6)
        assert isinstance(sanitize(Decimal("1.5")), float)

    def test_dict_values_are_sanitized_recursively(self):
        result = sanitize({"price": Decimal("2.5"), "name": "widget"})
        assert result == {"price": 2.5, "name": "widget"}

    def test_list_values_are_sanitized_recursively(self):
        result = sanitize([Decimal("1.0"), Decimal("2.0"), "text"])
        assert result == [1.0, 2.0, "text"]

    def test_plain_value_passthrough(self):
        assert sanitize("hello") == "hello"
        assert sanitize(42) == 42
        assert sanitize(None) is None


class TestTwilioFindTollFreeNumberInAreaCode:
    def test_success_returns_result(self, mock_twilio_client):
        toll_free_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.toll_free.list
        toll_free_list.return_value = ["num1", "num2"]

        result = twilio_util.twilio_find_toll_free_number_in_area_code(mock_twilio_client, "800")

        assert result == ["num1", "num2"]
        toll_free_list.assert_called_once_with(contains="800", limit=6)

    def test_invalid_pattern_uses_none(self, mock_twilio_client):
        # Pattern must match ^8[0,3-7] - "999" does not, so `pattern` is
        # normalized to None before searching.
        toll_free_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.toll_free.list
        toll_free_list.return_value = []

        twilio_util.twilio_find_toll_free_number_in_area_code(mock_twilio_client, "999")

        toll_free_list.assert_called_once_with(contains=None, limit=6)

    def test_exception_raises_area_code_unavailable_error(self, mock_twilio_client):
        toll_free_list = mock_twilio_client.api.v2010.accounts.return_value.available_phone_numbers.return_value.toll_free.list
        toll_free_list.side_effect = Exception("carrier error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_util.twilio_find_toll_free_number_in_area_code(mock_twilio_client, "800")

        assert "having problems finding phone numbers" in str(exc_info.value)


class TestTwilioBuyTollFreePhoneNumber:
    @patch("sh_util.tel.twilio_util.twilio_find_toll_free_number_in_area_code")
    def test_buy_with_pattern_success(self, mock_find, mock_twilio_client):
        found_number = MagicMock()
        found_number.sid = "PN_toll_free"
        found_number.phone_number = "+18005551234"
        mock_find.return_value = [found_number]
        purchased = MagicMock()
        purchased.phone_number = "+18005551234"
        mock_twilio_client.incoming_phone_numbers.create.return_value = purchased

        result = twilio_buy_toll_free_phone_number(mock_twilio_client, "AP_app_sid", pattern="800")

        assert result == "+18005551234"
        mock_find.assert_called_once_with(mock_twilio_client, "800", country_code="US", max_limit=1)
        mock_twilio_client.incoming_phone_numbers.create.assert_called_once_with(
            phone_number=[found_number], sms_application_sid="AP_app_sid", voice_application_sid="AP_app_sid"
        )

    @patch("sh_util.tel.twilio_util.twilio_find_toll_free_number_in_area_code")
    def test_buy_with_pattern_exception_raises_area_code_unavailable_error(self, mock_find, mock_twilio_client):
        found_number = MagicMock()
        mock_find.return_value = [found_number]
        mock_twilio_client.incoming_phone_numbers.create.side_effect = Exception("carrier error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_toll_free_phone_number(mock_twilio_client, "AP_app_sid", pattern="800")

        assert "having problems buying phone numbers" in str(exc_info.value)

    def test_buy_specific_toll_free_number_success(self, mock_twilio_client):
        purchased = MagicMock()
        purchased.phone_number = "+18005551234"
        mock_twilio_client.incoming_phone_numbers.create.return_value = purchased

        result = twilio_buy_toll_free_phone_number(mock_twilio_client, "AP_app_sid", phone_number="+18005551234")

        assert result == "+18005551234"

    def test_buy_specific_toll_free_number_exception_raises_area_code_unavailable_error(self, mock_twilio_client):
        mock_twilio_client.incoming_phone_numbers.create.side_effect = Exception("carrier error")

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_toll_free_phone_number(mock_twilio_client, "AP_app_sid", phone_number="+18005551234")

        assert "having problems buying phone numbers" in str(exc_info.value)

    def test_buy_no_pattern_no_phone_number_raises_error(self, mock_twilio_client):
        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_toll_free_phone_number(mock_twilio_client, "AP_app_sid")

        assert "No available numbers left" in str(exc_info.value)

    @patch("sh_util.tel.twilio_util.twilio_find_toll_free_number_in_area_code")
    def test_buy_with_pattern_no_numbers_available_raises_error(self, mock_find, mock_twilio_client):
        mock_find.return_value = None

        with pytest.raises(AreaCodeUnavailableError) as exc_info:
            twilio_buy_toll_free_phone_number(mock_twilio_client, "AP_app_sid", pattern="800")

        assert "No available numbers left" in str(exc_info.value)


def test_fetch_nodeid_for_debugging(monkeypatch):
    """
    Example of using monkeypatch instead of patch to simulate requests.get returning strange payload.
    """

    class DummyResp:
        status_code = 200

        def json(self):
            return {"available": True, "price": 5.5}

    # ``fetch_number_info`` imports requests inside the function body, so the
    # module has no ``requests`` attribute to patch — the global one is what it
    # resolves ``get`` from at call time.
    monkeypatch.setattr(requests, "get", lambda *a, **k: DummyResp())
    r = bandwidth.fetch_number_info("https://x", "z")
    assert r["price"] == 5.5
