# communication/bandwidth.py
"""
Small example module to exercise testing patterns:
- pure function: calculate_bandwidth
- external HTTP call: fetch_number_info (uses requests)
- DB write: store_purchase (expects DB connection object)
- orchestration: purchase_number
"""

import sqlite3
from typing import Dict


class ExternalAPIError(RuntimeError):
    pass


def calculate_bandwidth(bits: int, seconds: int) -> float:
    """
    Returns bits per second as float.
    Raises ZeroDivisionError for seconds == 0.
    """
    return bits / seconds


def fetch_number_info(api_url: str, phone_number: str, timeout: float = 5.0) -> Dict:
    """
    Calls an external API to fetch information about a phone number.
    Expects JSON response with at least {'available': bool, 'price': float}.
    Raises ExternalAPIError on non-200 or invalid payload.
    """
    # Import moved here to avoid missing package dependency
    import requests

    try:
        resp = requests.get(f"{api_url}/numbers/{phone_number}", timeout=timeout)
    except requests.RequestException as exc:
        raise ExternalAPIError("network error") from exc

    if resp.status_code != 200:
        raise ExternalAPIError(f"bad status: {resp.status_code}")

    data = resp.json()
    if not isinstance(data, dict) or 'available' not in data:
        raise ExternalAPIError("invalid payload")
    return data


def store_purchase(db_conn, phone_number: str, metadata: Dict) -> int:
    """
    Stores a purchase into a SQL DB-like connection.
    The db_conn must implement `execute(sql, params)` and `commit()` or be a sqlite3.Connection.

    Returns the inserted row id (int).
    """
    if isinstance(db_conn, sqlite3.Connection):
        cur = db_conn.cursor()
        try:
            cur.execute(
                "INSERT INTO purchases (phone_number, metadata) VALUES (?, ?);",
                (phone_number, str(metadata)),
            )
            db_conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
    else:
        # duck-typed generic DB object, assume execute() returns a rowid attribute or similar
        res = db_conn.execute(
            "INSERT INTO purchases (phone_number, metadata) VALUES (%s, %s);",
            (phone_number, str(metadata)),
        )
        try:
            db_conn.commit()
        except Exception:
            # some mock DBs may not implement commit; ignore if not present
            pass
        # try common ways to get an id
        if hasattr(res, "lastrowid"):
            return res.lastrowid
        if hasattr(res, "rowid"):
            return res.rowid
        # fallback
        return 0


def purchase_number(api_url: str, phone_number: str, db_conn, prefer_available: bool = True) -> Dict:
    """
    Orchestrates checking number info via API and storing the purchase.

    Returns a dict with detail about what happened:
      {'phone_number':..., 'purchased': bool, 'price': float, 'rowid': int}

    Raises ExternalAPIError if API call fails.
    """
    info = fetch_number_info(api_url, phone_number)
    price = float(info.get('price', 0.0))
    available = bool(info.get('available', False))

    # If prefer_available is True we only purchase when available
    if prefer_available and not available:
        return {'phone_number': phone_number, 'purchased': False, 'price': price, 'rowid': None}

    # simulate a purchase action by storing to DB
    meta = {"from_api": info}
    rowid = store_purchase(db_conn, phone_number, meta)
    return {'phone_number': phone_number, 'purchased': True, 'price': price, 'rowid': rowid}
