# Nocoly Explorer

A modular Python client for downloading [Nocoly](https://www.nocoly.com) worksheet
data into pandas / PySpark / CSV / JSON / file artifacts. Designed for both
local scripts and Databricks runtimes, with pluggable credential resolution,
a validated filter DSL, and configurable retry / pagination behavior.

[![Tests](https://img.shields.io/badge/tests-56%20passed-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.10%E2%80%933.12-blue)]()

## Installation

```bash
pip install nocoly-explorer
```

For pandas output, install with the optional `dataframe` extra:

```bash
pip install "nocoly-explorer[dataframe]"
```

For PySpark output (typically inside Databricks):

```bash
pip install "nocoly-explorer[spark]"
```

## Quickstart

```python
from nocoly_explorer import WorksheetExporter, NocolyFilter
import logging

logger = logging.getLogger("nocoly_export")

# Build a filter expression with the helper DSL.
filter_expression = NocolyFilter.and_group([
    NocolyFilter.quick(Status="Active", _updatedAt__gt="2025-01-01"),
    NocolyFilter.or_group([
        NocolyFilter.quick(Region="HK"),
        NocolyFilter.quick(Region="SZ"),
    ]),
])

# Fetch into a pandas DataFrame.
df = WorksheetExporter().export(
    host="https://bpm-uat.chinachemgroup.com",
    worksheet_id="ws_123",
    filter_criteria=filter_expression,
    columns=["Name", "Region", "Status", "_updatedAt"],
    sorts=[{"field": "_updatedAt", "order": "DESC"}],
    page_size=500,
    max_pages=25,
    logger=logger,
)
```

## Architecture Overview

The package follows a ports-and-adapters separation of concerns:

- **Configuration & Environment Detection (`config.py`, `detector.py`)** — Reads user config files / env vars and determines whether the code runs on Databricks. This drives which adapters to instantiate.
- **Credential Providers (`auth.py`)** — Strategy classes that encapsulate how `app_key` and `app_sign` are resolved (Databricks secrets vs. local env/config). The exporter never needs to know *where* secrets came from.
- **API Client (`client.py`)** — Focused on calling the worksheet endpoints. Handles retries with capped exponential backoff, `Retry-After` honor, base URLs, and column/filter payload assembly.
- **Output Layer (`output.py`)** — Knows how to shape raw row data into the requested format (DataFrame, CSV, JSON string, file write, etc.). Detects whether pandas / PySpark is available.
- **Orchestrator (`exporter.py`)** — High-level façade (`WorksheetExporter`) that validates input, wires dependencies together, and returns the requested output type.

```
WorksheetExporter
 ├── EnvironmentDetector
 ├── CredentialProvider (strategy)
 ├── WorksheetFetcher (client)
 └── OutputFormatter (adapter)
```

## Output Options

| `output_type`     | Returns                          | Notes                                  |
|-------------------|----------------------------------|----------------------------------------|
| `dataframe` (default) | `pandas.DataFrame`           | Requires pandas.                       |
| `spark`           | `pyspark.sql.DataFrame`          | Requires PySpark / Databricks.         |
| `json`            | `list[dict]`                     | Raw passthrough; serialize as needed.  |
| `string`          | JSON-formatted `str`             | Handles `datetime`, `Decimal`, `UUID`, `bytes`. |
| `csv`             | CSV-formatted `str`              | Union of all row keys; `None` and missing keys both render as empty. |
| `file`            | writes to disk                   | Requires `file_path`. Supports `json`, `csv`, `parquet`. Atomic write (tmp + replace). |

`output_type="file"` writes atomically — a crash mid-export will not leave a
half-written file at the target path. If `file_format` is provided and disagrees
with the path extension, a warning is logged.

## Optional Parameters

`WorksheetExporter.export()` exposes several knobs beyond the required `host`
/ `worksheet_id` arguments:

| Parameter      | Default        | Description                                              |
|----------------|----------------|----------------------------------------------------------|
| `filter_criteria`  | `None`     | dict or `NocolyFilter` expression (auto-wrapped).        |
| `columns`          | `None`     | restricts the returned columns list.                     |
| `sorts`            | `None`     | list of `{field, order}` dicts.                          |
| `page_size`        | `200`      | Per-API-call page size; capped at 1000 (Nocoly v3 limit).|
| `max_pages`        | `1000`     | Safety cap; raises if 0 or negative.                     |
| `view_id`          | `""`       | Saved worksheet view identifier.                         |
| `output_type`      | `"dataframe"` | One of `dataframe` / `spark` / `json` / `string` / `csv` / `file`. |
| `file_path`        | `None`     | Required for `output_type="file"`.                        |
| `file_format`      | `None`     | `json` / `csv` / `parquet` (inferred from extension if absent). |
| `verify_ssl`       | `True`     | Set False to bypass TLS validation.                      |
| `logger`           | `None`     | `logging.Logger` to receive structured debug/warnings.   |
| `env_prefix`       | `None`     | Use `NOCOLY_<PREFIX>_APP_KEY` / `NOCOLY_<PREFIX>_APP_SIGN` for env-based credentials. |

## Filter Builder Helpers

- Import `NocolyFilter` from the top-level package to build validated payloads.
- Compose conditions via `equals`, `greater_than`, `contains`, etc., and combine with `and_group` / `or_group`.
- The depth check counts **group** levels only (max 3: root + 2 nested groups). Conditions attached to a group are part of that level.
- `quick()` maps suffixes to operators (`Score__gt=40`, `Region__in=["HK","SZ"]`, `Status="Active"`).
- A flat `{field: value}` dict is auto-wrapped as `{type: group, logic: AND, filters: [EQ condition]}`; pass either form to `filter_criteria=...`.

```python
from nocoly_explorer import WorksheetExporter, NocolyFilter

# Three-level group nesting — root AND, nested OR, two leaf quick() groups.
expr = NocolyFilter.and_group([
    NocolyFilter.quick(Status="Active", _updatedAt__gt="2025-01-01"),
    NocolyFilter.or_group([
        NocolyFilter.quick(Region="HK"),
        NocolyFilter.quick(Region="SZ"),
    ]),
])
df = WorksheetExporter().export(
    host="https://bpm-uat.chinachemgroup.com",
    worksheet_id="ws_123",
    filter_criteria=expr,
)
```

## Credential Resolution

Three sources, in order:

1. **Explicit kwargs** — pass `app_key=` and `app_sign=` directly.
2. **Static config** — set `static_credentials.app_key` / `app_sign` in your config file.
3. **Environment** — `NOCOLY_APP_KEY` / `NOCOLY_APP_SIGN`, or `NOCOLY_<PREFIX>_APP_KEY` / `NOCOLY_<PREFIX>_APP_SIGN` when `env_prefix="PROD"` is passed.

When `env_prefix` is explicitly set, **only** the matching prefixed env vars
are consulted — there is no silent fallback to unprefixed vars. This prevents
leaking credentials from a different environment.

For Databricks, set the secret scope/key names in config and the package
automatically switches to `DatabricksCredentialProvider` when
`DATABRICKS_RUNTIME_VERSION` is detected.

## Configuration File

Create `nocoly.toml` or `nocoly.json` in your working directory or
`~/.config/nocoly/`, or point at any path via the `NOCOLY_CONFIG` env var:

```json
{
  "env_mode": "databricks",
  "default_output_type": "spark",
  "request_timeout_seconds": 60.0,
  "max_retries": 5,
  "databricks": {
    "scope": "nocoly-prod",
    "app_key_secret": "app_key",
    "app_sign_secret": "app_sign"
  },
  "static_credentials": {
    "env_prefix": "PROD"
  }
}
```

Supported formats: `.json`, `.toml`. Other extensions raise a `ValueError`
listing the supported formats. Results are cached per `NOCOLY_CONFIG` value
within the same process.

## Error Handling

All public failures bubble up as subclasses of `NocolyError`:

| Exception                  | Cause                                                              |
|----------------------------|--------------------------------------------------------------------|
| `MissingCredentialsError`  | No source found for app_key/app_sign.                              |
| `OutputValidationError`    | Bad output params (missing `file_path`, unsupported format, etc). |
| `EnvironmentDetectionError`| `env_mode` is set to something other than `databricks` / `standard` / `None`. |
| `NocolyError` (base)       | HTTP errors, invalid JSON, transport failures after retries.       |

HTTP client retries transient errors up to `max_retries` times (default 3) with
exponential backoff capped at `max_wait_seconds` (default 30s). When the server
returns `429` or `503` with a `Retry-After` header, that header is honored.

## Testing & Development

```bash
git clone https://github.com/rollroyces/nocoly-explorer.git
cd nocoly-explorer
pip install -e ".[dataframe]"
pip install pytest pytest-cov
pytest
```

Tests stub out external services (`requests`/`DBUtils`) to keep runs fast and
hermetic. The test suite has 56 tests covering credential strategies, filter
builder depth, output formatter edge cases, paging validation, backoff and
`Retry-After` handling, environment detection, and config loading.

## Building & Publishing

```bash
# Build artifacts locally
python -m build
# → dist/nocoly_explorer-0.1.1-py3-none-any.whl
# → dist/nocoly_explorer-0.1.1.tar.gz

# Verify the wheel installs cleanly
pip install dist/nocoly_explorer-0.1.1-py3-none-any.whl

# Publish to an internal feed
twine upload --repository corporate dist/*
```

Downstream Databricks jobs and standard Python apps can then install with:

```bash
pip install --index-url <internal-feed> nocoly-explorer
```

## Continuous Integration

GitHub Actions at [.github/workflows/ci.yml](.github/workflows/ci.yml) runs on
pushes/PRs to `main`/`master` across Python 3.10–3.12. Steps: checkout →
set up Python (with pip cache) → install in editable mode with the `dataframe`
extra → run `pytest` with coverage to catch regressions in credential
providers, filters, client, output formatter, environment detection, and
config loading.

## Changelog

### 0.1.1

- **Fix:** README's documented filter expression (`and_group` of nested
  `or_group` of `quick()` calls) is now constructible. Depth check now counts
  groups only and the cap is 3.
- **Fix:** `env_prefix` no longer silently falls back to unprefixed
  credentials. Setting `env_prefix="PROD"` now strictly requires
  `NOCOLY_PROD_APP_KEY` / `NOCOLY_PROD_APP_SIGN`.
- **Fix:** Unknown `env_mode` values now raise `EnvironmentDetectionError`
  instead of silently downgrading to `standard`.
- **Fix:** CSV output handles rows with mismatched keys via union of all
  keys (first-row order preserved). `None` and missing keys both render
  consistently as empty fields.
- **Fix:** JSON / string output serializes `datetime`, `Decimal`, `UUID`,
  `bytes`, and `set` / `frozenset` values instead of crashing.
- **Fix:** HTTP client honors `Retry-After` (delta-seconds and HTTP-date
  formats) on 429/503 responses.
- **Fix:** Exponential backoff capped at `max_wait_seconds` (default 30s);
  no more unbounded retry waits.
- **Fix:** `max_pages=0` raises `ValueError`. `page_size=0` and
  `page_size > 1000` raise `ValueError` with actionable messages.
- **Fix:** File output is atomic — writes to `<path>.tmp` then `os.replace()`
  to avoid partial-write corruption. `parquet` export errors guide users to
  install `pyarrow` or `fastparquet`.
- **Fix:** Flat `{field: value}` dicts passed to `filter_criteria=` are
  auto-converted to a condition group; DSL-shaped dicts pass through.
- **Internal:** `load_package_config()` caches results per `NOCOLY_CONFIG`
  value. Config loading gives clearer errors for unsupported extensions and
  bad value types.
- **Tests:** 56 tests across 6 test modules; full suite passes in <5 s.

### 0.1.0

- Initial release.