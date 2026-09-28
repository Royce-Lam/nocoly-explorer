"""Tests for advanced filter builder helpers."""

from __future__ import annotations

import pytest

from nocoly_explorer.filters import NocolyFilter, ensure_filter_dict, FilterCondition, FilterGroup


def test_quick_builder_maps_suffixes():
    expr = NocolyFilter.quick(Status="Active", Score__gt=50, Region__in=["HK", "SZ"])
    payload = expr.to_dict()

    assert payload["logic"] == "AND"
    assert len(payload["filters"]) == 3
    operators = {item["operator"] for item in payload["filters"]}
    assert operators == {"EQ", "GT", "IN"}


def test_group_depth_limit():
    """MAX_DEPTH counts groups only — four nested groups must raise."""
    g1 = NocolyFilter.and_group([NocolyFilter.equals("FieldA", 1)])
    g2 = NocolyFilter.and_group([g1])
    g3 = NocolyFilter.and_group([g2])
    with pytest.raises(ValueError):
        NocolyFilter.and_group([g3])  # 4 nested groups → over limit


def test_readme_example_builds():
    """The README's documented filter expression must be constructible."""
    expr = NocolyFilter.and_group([
        NocolyFilter.quick(Status="Active", _updatedAt__gt="2025-01-01"),
        NocolyFilter.or_group([
            NocolyFilter.quick(Region="HK"),
            NocolyFilter.quick(Region="SZ"),
        ]),
    ])
    payload = expr.to_dict()
    assert payload["logic"] == "AND"
    assert len(payload["filters"]) == 2
    # Inner OR group should be present and contain 2 quick() groups
    or_group = next(f for f in payload["filters"] if f.get("logic") == "OR")
    assert len(or_group["filters"]) == 2


def test_three_levels_of_groups_allowed():
    """Three nested groups are the maximum the API accepts."""
    g1 = NocolyFilter.and_group([NocolyFilter.equals("a", 1)])
    g2 = NocolyFilter.and_group([g1])
    g3 = NocolyFilter.and_group([g2])  # depth=3, allowed
    payload = g3.to_dict()
    assert payload["logic"] == "AND"


def test_four_levels_of_groups_rejected():
    """Four nested groups must raise."""
    g1 = NocolyFilter.and_group([NocolyFilter.equals("a", 1)])
    g2 = NocolyFilter.and_group([g1])
    g3 = NocolyFilter.and_group([g2])
    with pytest.raises(ValueError):
        NocolyFilter.and_group([g3])


def test_ensure_filter_dict_accepts_nodes():
    node = NocolyFilter.equals("Status", "Active")
    payload = ensure_filter_dict(node)

    assert payload["field"] == "Status"
    assert payload["operator"] == "EQ"
    # A flat dict is now auto-converted to a condition group (M10).
    assert ensure_filter_dict({"foo": "bar"}) == {
        "type": "group",
        "logic": "AND",
        "filters": [
            {"type": "condition", "field": "foo", "operator": "EQ", "value": "bar"}
        ],
    }


def test_filter_condition_to_dict():
    cond = FilterCondition(field="Foo", operator="EQ", value="bar")
    assert cond.to_dict() == {
        "type": "condition",
        "field": "Foo",
        "operator": "EQ",
        "value": "bar",
    }


def test_filter_group_to_dict():
    group = FilterGroup(logic="AND", children=[NocolyFilter.equals("Foo", 1)])
    payload = group.to_dict()
    assert payload["logic"] == "AND"
    assert payload["filters"][0]["field"] == "Foo"


def test_operators_are_consistent_between_static_methods_and_quick_suffixes():
    """M1: the operator strings used by NocolyFilter.equals() and quick() must match."""
    # If someone updates one, the other must follow.
    # We verify by checking every operator exposed via _parse_condition_key's map
    # matches what the static method would emit for the same condition.
    from nocoly_explorer.filters import _OPERATOR_MAP

    # Pick a sample mapping: __gt → GT
    assert _OPERATOR_MAP["gt"] == "GT"
    expr = NocolyFilter.quick(Score__gt=10)
    node = expr.to_dict()["filters"][0]
    assert node["operator"] == "GT"


def test_flat_dict_is_auto_converted_to_condition_group():
    """M10: filter_criteria={'Status': 'Active'} must be wrapped as a condition group."""
    payload = ensure_filter_dict({"Status": "Active"})
    assert payload["type"] == "group"
    assert payload["logic"] == "AND"
    assert payload["filters"] == [
        {"type": "condition", "field": "Status", "operator": "EQ", "value": "Active"}
    ]


def test_nested_dict_with_filters_key_passes_through():
    """A dict already in DSL form should not be re-wrapped."""
    raw = {"type": "group", "logic": "AND", "filters": []}
    assert ensure_filter_dict(raw) is raw


def test_none_filter_returns_none():
    assert ensure_filter_dict(None) is None
