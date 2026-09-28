"""Shared async test fixtures."""

from __future__ import annotations

from typing import Any, Dict, List

from aiohttp import web


def make_handler(
    pages: List[List[Dict[str, Any]]],
    *,
    fail_first_n: int = 0,
    fail_status: int = 429,
    retry_after: str = "1",
):
    """Build an aiohttp handler that serves pages sequentially.

    - `fail_first_n` requests return `fail_status` with a Retry-After header.
    - Once exhausted, returns the next page from `pages`.
    - Page `i` (1-indexed) is served for `page=i&page_size=...` request until
      pages run out, then returns an empty page with has_more=False.
    """
    fail_count = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal fail_count
        page = int(request.query.get("page", "1"))
        page_size = int(request.query.get("page_size", "200"))
        if fail_count < fail_first_n:
            fail_count += 1
            return web.Response(
                status=fail_status,
                headers={"Retry-After": retry_after},
                text="rate limited",
            )
        idx = page - 1
        if idx < 0 or idx >= len(pages):
            return web.json_response({"rows": [], "has_more": False})
        rows = pages[idx]
        has_more = idx < len(pages) - 1 or len(rows) == page_size
        return web.json_response({"rows": rows[:page_size], "has_more": has_more})

    return handler