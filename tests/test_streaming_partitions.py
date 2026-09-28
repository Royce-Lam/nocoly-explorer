"""Tests for PartitionRouter."""

from __future__ import annotations

import datetime

import pytest

from nocoly_explorer.exceptions import CardinalityExceededError
from nocoly_explorer.streaming.options import PartitionSpec
from nocoly_explorer.streaming.partitions import PartitionRouter


def test_router_no_partition_returns_none():
    router = PartitionRouter(PartitionSpec(column="unused"), partition_column=None)
    assert router.key_for({"any": "row"}) is None


def test_router_simple_string_partition():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    assert router.key_for({"region": "HK"}) == "region=HK"


def test_router_string_with_unsafe_chars_replaced():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    # Spaces and slashes are both replaced with underscores.
    assert router.key_for({"region": "Hong Kong / Kowloon"}) == "region=Hong_Kong___Kowloon"


def test_router_date_day_granularity():
    spec = PartitionSpec(column="created_date", granularity="day")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.date(2026, 9, 28)}
    assert router.key_for(row) == "created_date=2026-09-28"


def test_router_datetime_day_granularity():
    spec = PartitionSpec(column="created_date", granularity="day")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.datetime(2026, 9, 28, 14, 30, 0)}
    assert router.key_for(row) == "created_date=2026-09-28"


def test_router_month_granularity():
    spec = PartitionSpec(column="created_date", granularity="month")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.date(2026, 9, 28)}
    assert router.key_for(row) == "created_date=2026-09"


def test_router_year_granularity():
    spec = PartitionSpec(column="created_date", granularity="year")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.date(2026, 9, 28)}
    assert router.key_for(row) == "created_date=2026"


def test_router_cardinality_guard():
    spec = PartitionSpec(column="user_id")
    router = PartitionRouter(spec, partition_column="user_id", max_cardinality=3)
    router.key_for({"user_id": "a"})
    router.key_for({"user_id": "b"})
    router.key_for({"user_id": "c"})
    with pytest.raises(CardinalityExceededError):
        router.key_for({"user_id": "d"})


def test_router_null_partition_value_becomes_null_partition():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    assert router.key_for({"region": None}) == "region=__HIVE_NULL__"


def test_router_missing_partition_column_treated_as_null():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    assert router.key_for({}) == "region=__HIVE_NULL__"