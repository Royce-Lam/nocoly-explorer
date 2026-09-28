"""Composable helpers for building Nocoly v3 filter payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Protocol, Sequence, Union

# Max nesting levels of groups, counted from the leaf upward.
# The Nocoly API accepts at most one nested AND/OR group under the root, but
# `quick()` itself returns a single AND group, so a depth-3 tree
# (root AND → OR group → AND group from quick()) is the practical maximum
# for the README's documented usage.
MAX_DEPTH = 3


class FilterExpression(Protocol):
    """Protocol implemented by filter nodes."""

    def to_dict(self) -> Dict[str, Any]:
        ...


@dataclass(frozen=True)
class FilterCondition(FilterExpression):
    """Single field comparison."""

    field: str
    operator: str
    value: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "condition",
            "field": self.field,
            "operator": self.operator,
            "value": self.value,
        }


@dataclass(frozen=True)
class FilterGroup(FilterExpression):
    """Logical grouping (AND/OR) with depth validation."""

    logic: str
    children: Sequence[FilterExpression] = field(default_factory=list)

    def __post_init__(self):
        if self.logic not in {"AND", "OR"}:
            raise ValueError("logic must be 'AND' or 'OR'")
        depth = _calculate_depth(self)
        if depth > MAX_DEPTH:
            raise ValueError(f"Filter nesting exceeds max depth ({MAX_DEPTH})")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "group",
            "logic": self.logic,
            "filters": [child.to_dict() for child in self.children],
        }


def _calculate_depth(node: FilterExpression) -> int:
    """Count nested groups only — conditions are leaves and don't add a level.

    This matches the API constraint described in the README: max two levels of
    AND/OR groups. Conditions attached to a group are part of that level.
    """
    if isinstance(node, FilterGroup):
        if not node.children:
            return 1
        return 1 + max(_calculate_depth(child) for child in node.children)
    return 0


_OPERATOR_MAP = {
    "eq": "EQ",
    "neq": "NEQ",
    "gt": "GT",
    "gte": "GTE",
    "lt": "LT",
    "lte": "LTE",
    "contains": "CONTAINS",
    "not_contains": "NOT_CONTAINS",
    "in": "IN",
}


class NocolyFilter:
    """Factory methods for the worksheet API filter DSL."""

    @staticmethod
    def equals(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="EQ", value=value)

    @staticmethod
    def not_equals(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="NEQ", value=value)

    @staticmethod
    def greater_than(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="GT", value=value)

    @staticmethod
    def greater_than_or_equal(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="GTE", value=value)

    @staticmethod
    def less_than(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="LT", value=value)

    @staticmethod
    def less_than_or_equal(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="LTE", value=value)

    @staticmethod
    def contains(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="CONTAINS", value=value)

    @staticmethod
    def not_contains(field: str, value: Any) -> FilterExpression:
        return FilterCondition(field=field, operator="NOT_CONTAINS", value=value)

    @staticmethod
    def in_list(field: str, values: Iterable[Any]) -> FilterExpression:
        return FilterCondition(field=field, operator="IN", value=list(values))

    @staticmethod
    def not_empty(field: str) -> FilterExpression:
        return FilterCondition(field=field, operator="NOT_EMPTY", value=True)

    @staticmethod
    def empty(field: str) -> FilterExpression:
        return FilterCondition(field=field, operator="EMPTY", value=True)

    @staticmethod
    def and_group(filters: Sequence[FilterExpression]) -> FilterExpression:
        return FilterGroup(logic="AND", children=tuple(filters))

    @staticmethod
    def or_group(filters: Sequence[FilterExpression]) -> FilterExpression:
        return FilterGroup(logic="OR", children=tuple(filters))

    @staticmethod
    def quick(**conditions: Any) -> FilterExpression:
        """Convenience builder that infers operators from suffixes."""

        nodes: List[FilterExpression] = []
        for key, value in conditions.items():
            field, operator = _parse_condition_key(key)
            if operator == "NOT_EMPTY":
                nodes.append(FilterCondition(field=field, operator=operator, value=True))
            elif operator == "EMPTY":
                nodes.append(FilterCondition(field=field, operator=operator, value=True))
            elif operator == "IN":
                if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    raise ValueError(f"Value for '{key}' must be a non-string sequence")
                nodes.append(FilterCondition(field=field, operator=operator, value=list(value)))
            else:
                nodes.append(FilterCondition(field=field, operator=operator, value=value))
        return FilterGroup(logic="AND", children=tuple(nodes))


def _parse_condition_key(key: str) -> tuple[str, str]:
    if "__" not in key:
        return key, "EQ"
    field, suffix = key.split("__", 1)
    if suffix == "not_empty":
        return field, "NOT_EMPTY"
    if suffix == "empty":
        return field, "EMPTY"
    mapped = _OPERATOR_MAP.get(suffix)
    if not mapped:
        raise ValueError(f"Unsupported filter suffix: {suffix}")
    return field, mapped


FilterInput = Union[FilterExpression, Dict[str, Any], None]


def ensure_filter_dict(filter_obj: FilterInput) -> Optional[Dict[str, Any]]:
    """Convert filter expressions into raw dicts for the API client.

    A bare ``{field: value}`` mapping is treated as a flat EQ-AND group so
    callers can pass either a dict or a ``FilterExpression`` interchangeably.
    A dict that already looks like the DSL (has ``"type"`` or ``"filters"``)
    is passed through verbatim.
    """
    if filter_obj is None:
        return None
    if isinstance(filter_obj, dict):
        if "type" in filter_obj or "filters" in filter_obj or "logic" in filter_obj:
            return filter_obj
        # Treat as a flat EQ mapping.
        return {
            "type": "group",
            "logic": "AND",
            "filters": [
                {"type": "condition", "field": k, "operator": "EQ", "value": v}
                for k, v in filter_obj.items()
            ],
        }
    if hasattr(filter_obj, "to_dict"):
        return filter_obj.to_dict()  # type: ignore[return-value]
    raise TypeError("filter_criteria must be a dict or FilterExpression")
