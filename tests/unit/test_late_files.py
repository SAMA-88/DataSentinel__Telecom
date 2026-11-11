"""Unit tests for the late file detector logic in 11_dq_lib (used by notebook 33_late_file_detector), no Spark.

    python -m pytest tests/unit/test_late_files.py -v
"""
import copy
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from sentinel_dq.quality.contract_loader import load_contracts, load_dq_lib  # noqa: E402

lib = load_dq_lib()
CONTRACTS = load_contracts()
UTC = timezone.utc


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def file_contract(frequency="daily", **fresh):
    fr = {"frequency": frequency, "max_delay_days": 1 if frequency == "daily" else 10,
          "deadline": "06:00", "timezone": "Europe/Amsterdam",
          "expected_from": date(2026, 10, 1) if frequency == "daily" else date(2026, 7, 1), **fresh}
    pattern, fmt = (r"cdr_(\d{8})\.csv$", "%Y%m%d") if frequency == "daily" else (r"inv_(\d{6})\.csv$", "%Y%m")
    return {"dataset": "f", "version": "1", "owner": "tests",
            "source": {"kind": "csv", "table": "bronze.f", "file_source": "f",
                       "file_pattern": pattern, "file_date_format": fmt},
            "columns": [{"name": "id", "type": "string"}],
            "monitors": {"freshness": fr}}


# ---------------------------------------------------------------- deadlines (06:00 Amsterdam, summer and winter time)

@pytest.mark.parametrize("period,frequency,expected", [
    (date(2026, 10, 5), "daily", utc(2026, 10, 6, 4, 0)),      # CEST = UTC+2
    (date(2026, 11, 5), "daily", utc(2026, 11, 6, 5, 0)),      # CET  = UTC+1
    (date(2026, 10, 24), "daily", utc(2026, 10, 25, 5, 0)),    # clocks go back on 25 Oct at 03:00, so 06:00 is CET
    (date(2026, 7, 1), "monthly", utc(2026, 8, 10, 4, 0)),     # July file: 31 Jul + 10 days
    (date(2026, 12, 1), "monthly", utc(2027, 1, 10, 5, 0)),    # over the year end
])
def test_file_deadline(period, frequency, expected):
    fr = file_contract(frequency)["monitors"]["freshness"]
    assert lib.file_deadline(period, fr) == expected


def test_deadline_and_timezone_can_be_changed():
    fr = file_contract("daily", deadline="23:30", timezone="UTC", max_delay_days=0)["monitors"]["freshness"]
    assert lib.file_deadline(date(2026, 10, 5), fr) == utc(2026, 10, 5, 23, 30)


def test_defaults_when_deadline_and_timezone_are_left_out():
    fr = file_contract("daily")["monitors"]["freshness"]
    del fr["deadline"], fr["timezone"]
    assert lib.file_deadline(date(2026, 10, 5), fr) == utc(2026, 10, 6, 4, 0)


# ---------------------------------------------------------------- which periods are expected

def test_expected_periods_daily_up_to_today_in_local_time():
    fr = file_contract("daily")["monitors"]["freshness"]
    # 23:30 UTC on 4 Oct is already 01:30 on 5 Oct in Amsterdam
    assert lib.expected_periods(fr, utc(2026, 10, 4, 23, 30)) == [date(2026, 10, d) for d in range(1, 6)]


def test_expected_periods_monthly():
    fr = file_contract("monthly")["monitors"]["freshness"]
    assert lib.expected_periods(fr, utc(2026, 10, 15)) == [date(2026, m, 1) for m in (7, 8, 9, 10)]


def test_expected_periods_monthly_start_mid_month_counts_that_month():
    fr = file_contract("monthly", expected_from="2026-07-15")["monitors"]["freshness"]
    assert lib.expected_periods(fr, utc(2026, 8, 1, 12))[0] == date(2026, 7, 1)


def test_nothing_expected_before_expected_from():
    fr = file_contract("daily")["monitors"]["freshness"]
    assert lib.expected_periods(fr, utc(2026, 9, 1)) == []


# ---------------------------------------------------------------- reading the period from the file name

@pytest.mark.parametrize("name,frequency,expected", [
    ("cdr_20261002.csv", "daily", date(2026, 10, 2)),
    ("inv_202609.csv", "monthly", date(2026, 9, 1)),
    ("cdr_20261399.csv", "daily", None),           # matches the pattern but is not a real date
    ("readme.csv", "daily", None),
    ("cdr_20261002.csv.bak", "daily", None),
])
def test_file_period(name, frequency, expected):
    src = file_contract(frequency)["source"]
    assert lib.file_period(name, src, frequency) == expected


# ---------------------------------------------------------------- status

DUE = utc(2026, 10, 6, 4, 0)


@pytest.mark.parametrize("arrived,now,status", [
    (utc(2026, 10, 6, 3, 0), utc(2026, 10, 7), "ON_TIME"),
    (DUE, utc(2026, 10, 7), "ON_TIME"),                      # exactly at the deadline is still on time
    (utc(2026, 10, 6, 4, 1), utc(2026, 10, 7), "LATE"),
    (None, utc(2026, 10, 6, 4, 1), "MISSING"),
    (None, utc(2026, 10, 6, 3, 59), "PENDING"),
])
def test_arrival_status(arrived, now, status):
    assert lib.arrival_status(DUE, arrived, now) == status
    assert status in lib.FILE_STATUSES


# ---------------------------------------------------------------- expected vs arrived

def by_period(rows):
    return {r["period"]: r for r in rows}


def test_file_arrivals_finds_missing_late_on_time_and_pending():
    c = file_contract("daily")
    now = utc(2026, 10, 5, 12, 0)                              # 14:00 on 5 Oct in Amsterdam
    files = {
        "cdr_20261001.csv": utc(2026, 10, 1, 22, 0),           # on time
        # 2 Oct never arrives
        "cdr_20261003.csv": utc(2026, 10, 4, 9, 0),            # due 4 Oct 04:00 UTC: 5 hours late
        "cdr_20261004.csv": utc(2026, 10, 5, 1, 0),            # on time
        "notes.csv": utc(2026, 10, 2),                         # not a CDR file: ignored
    }
    rows = by_period(lib.file_arrivals(c, files, now))
    assert {p: r["status"] for p, r in rows.items()} == {
        date(2026, 10, 1): "ON_TIME", date(2026, 10, 2): "MISSING", date(2026, 10, 3): "LATE",
        date(2026, 10, 4): "ON_TIME", date(2026, 10, 5): "PENDING"}
    assert rows[date(2026, 10, 3)]["late_minutes"] == 300
    assert rows[date(2026, 10, 2)]["file_name"] is None and rows[date(2026, 10, 2)]["arrived_at"] is None
    assert rows[date(2026, 10, 1)]["late_minutes"] == 0


def test_first_seen_time_wins_over_a_later_upload():
    c = file_contract("daily")
    reuploaded = {"cdr_20261001.csv": utc(2026, 10, 20)}       # uploaded again much later
    first = {"cdr_20261001.csv": utc(2026, 10, 1, 22, 0)}      # what the detector recorded the first time
    row = by_period(lib.file_arrivals(c, reuploaded, utc(2026, 10, 21), first))[date(2026, 10, 1)]
    assert row["status"] == "ON_TIME" and row["arrived_at"] == utc(2026, 10, 1, 22, 0)


def test_a_file_moved_away_after_it_arrived_still_counts():
    c = file_contract("daily")
    first = {"cdr_20261001.csv": utc(2026, 10, 1, 22, 0)}
    row = by_period(lib.file_arrivals(c, {}, utc(2026, 10, 3), first))[date(2026, 10, 1)]
    assert row["status"] == "ON_TIME"


def test_two_files_for_one_period_the_earliest_counts():
    c = file_contract("monthly")
    files = {"inv_202607.csv": utc(2026, 8, 20), "x_inv_202607.csv": utc(2026, 8, 5)}
    row = by_period(lib.file_arrivals(c, files, utc(2026, 8, 25)))[date(2026, 7, 1)]
    assert row["file_name"] == "x_inv_202607.csv" and row["status"] == "ON_TIME"


def test_file_arrivals_does_not_change_its_inputs():
    c = file_contract("daily")
    files = {"cdr_20261001.csv": utc(2026, 10, 1, 22, 0)}
    first = {"cdr_20261002.csv": utc(2026, 10, 2, 22, 0)}
    before = copy.deepcopy((c, files, first))
    lib.file_arrivals(c, files, utc(2026, 10, 3), first)
    assert (c, files, first) == before


# ---------------------------------------------------------------- contract checks for the file schedule

def test_file_contract_is_valid():
    assert lib.validate_contract(file_contract("daily")) == []
    assert lib.validate_contract(file_contract("monthly")) == []


def with_fresh(**changes):
    c = file_contract("daily")
    c["monitors"]["freshness"].update(changes)
    return c


def with_source(**changes):
    c = file_contract("daily")
    c["source"].update(changes)
    return c


@pytest.mark.parametrize("contract,expected", [
    (with_fresh(expected_from="first of October"), "expected_from must be a date"),
    (with_fresh(expected_from=None), "expected_from must be a date"),
    (with_fresh(max_delay_days=-1), "max_delay_days must be a whole number"),
    (with_fresh(max_delay_days=1.5), "max_delay_days must be a whole number"),
    (with_fresh(deadline="6am"), "deadline must be HH:MM"),
    (with_fresh(deadline="24:00"), "deadline must be HH:MM"),
    (with_fresh(timezone="Mars/Olympus"), "not a known time zone"),
    (with_source(file_pattern=None), "file_pattern and source.file_date_format are required"),
    (with_source(file_pattern=r"cdr_\d{8}\.csv$"), "exactly one (group)"),
    (with_source(file_pattern="cdr_(["), "file_pattern: bad regex"),
])
def test_validate_contract_checks_the_file_schedule(contract, expected):
    errs = lib.validate_contract(contract)
    assert any(expected in e for e in errs), errs


def test_stream_freshness_needs_no_file_schedule():
    c = file_contract("daily")
    c["source"] = {"kind": "cdc", "table": "bronze.x", "topic": "t", "key": ["id"]}
    c["monitors"]["freshness"] = {"type": "stream", "max_delay_minutes": 60}
    assert lib.validate_contract(c) == []


# ---------------------------------------------------------------- the repo contracts

FILE_CONTRACTS = sorted(n for n, c in CONTRACTS.items() if c["source"]["kind"] == "csv")


def test_every_file_source_has_a_schedule():
    assert FILE_CONTRACTS == ["invoices_file", "network_cdr", "partner_usage", "payments_file"]
    for n in FILE_CONTRACTS:
        assert CONTRACTS[n]["monitors"]["freshness"].get("type") != "stream"


@pytest.mark.parametrize("name,file_name,period", [
    ("network_cdr", "cdr_20260928.csv", date(2026, 9, 28)),
    ("invoices_file", "invoices_202607.csv", date(2026, 7, 1)),
    ("payments_file", "payments_202609.csv", date(2026, 9, 1)),
    ("partner_usage", "partner_usage_202608.csv", date(2026, 8, 1)),
])
def test_repo_file_names_match_their_contract(name, file_name, period):
    c = CONTRACTS[name]
    assert lib.file_period(file_name, c["source"], c["monitors"]["freshness"]["frequency"]) == period


def test_sample_batch_data_shows_the_missing_cdr_file():
    """generators/batch/batch_data/network has no file for 2 Oct 2026: the detector must report it."""
    c = CONTRACTS["network_cdr"]
    folder = ROOT / "generators" / "batch" / "batch_data" / "network"
    files = {p.name: utc(2026, 10, 1) for p in folder.glob("cdr_*.csv")}   # pretend all arrived early
    rows = lib.file_arrivals(c, files, utc(2026, 10, 8, 12, 0))
    assert [r["period"] for r in rows if r["status"] == "MISSING"] == [date(2026, 10, 2)]
    assert [r["period"] for r in rows if r["status"] == "PENDING"] == [date(2026, 10, 8)]
