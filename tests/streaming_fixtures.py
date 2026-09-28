"""Shared fixtures for streaming export tests."""

from __future__ import annotations

from typing import Any, Dict, Iterator, List


def make_pages(total_rows: int, page_size: int = 100) -> Iterator[List[Dict[str, Any]]]:
    """Yield pages of synthetic rows: each row has 'a', 'b', 'created_date', 'region'."""
    page: List[Dict[str, Any]] = []
    for i in range(total_rows):
        if len(page) >= page_size:
            yield page
            page = []
        page.append({
            "a": i,
            "b": f"row-{i}",
            "created_date": f"2026-09-{(i % 28) + 1:02d}",
            "region": "HK" if i % 2 == 0 else "SZ",
        })
    if page:
        yield page


class FakeClient:
    """A drop-in WorksheetClient replacement that emits pages from a generator."""

    def __init__(self, pages: Iterator[List[Dict[str, Any]]]):
        self._pages = list(pages)
        self.fetched = 0

    def fetch_rows(self, *, page_size: int = 200, max_pages: int = 1000, **_kwargs):
        self.fetched += 1
        return [row for page in self._pages for row in page]