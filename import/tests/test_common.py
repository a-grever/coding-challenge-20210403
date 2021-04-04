from datetime import date

import pytest

import common


def test_get_import_date(monkeypatch):
    monkeypatch.setenv("IMPORT_DATE", "2021-04-05")
    assert common.get_import_date() == date(2021, 4, 5)


def test_get_import_date_missing_date(monkeypatch):
    monkeypatch.delenv("IMPORT_DATE")
    with pytest.raises(RuntimeError):
        common.get_import_date()


def test_get_import_date_broken_date_str(monkeypatch):
    monkeypatch.setenv("IMPORT_DATE", "21-04-05")
    with pytest.raises(RuntimeError):
        common.get_import_date()
