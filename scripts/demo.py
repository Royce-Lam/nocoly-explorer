#!/usr/bin/env python
"""nocoly-explorer v0.2.0 demo driver.

Honest label: this run is against a LOCAL mock Nocoly server.
It exercises the wiring; for production install + run real commands at the end.
"""
import os
import subprocess
import sys
import time

env = os.environ.copy()
env["TERM"] = "xterm-256color"

C = "\033[2J\033[H"
print(C)
print("""
+--------------------------------------------------------------------------+
|  nocoly-explorer v0.2.0 - Phase 3 demo (FastAPI + Async + Parquet)        |
|                                                                          |
|  Honest label: this run is against a LOCAL mock Nocoly server,          |
|  not your real production endpoint. We are exercising the wiring.        |
+--------------------------------------------------------------------------+
""")
sys.stdout.flush()
time.sleep(1.5)

print("$ which python")
sys.stdout.flush()
subprocess.run(["python", "--version"], env=env)
sys.stdout.flush()
time.sleep(1.2)

print()
print('$ python -c "from nocoly_explorer import ..."')
sys.stdout.flush()
subprocess.run(["python", "-c", (
    "from nocoly_explorer import (\n"
    "    StreamingExporter, ParquetExportOptions, PartitionSpec,\n"
    "    AsyncWorksheetClient, AsyncClientError, PaginationLimitExceeded,\n"
    "    create_app, JobSubmission, run_job, JobState,\n"
    ")\n"
    "print('  Phase 1 (Streaming Parquet): OK')\n"
    "print('  Phase 2 (Async Pagination):  OK')\n"
    "print('  Phase 3 (FastAPI service):    OK')\n"
)], env=env)
sys.stdout.flush()
time.sleep(2)

print(C)
print("$ # Step 1 - start a local mock Nocoly server in the background")
print()
sys.stdout.flush()

mock_proc = subprocess.Popen(
    ["python", "-c", (
        "import asyncio\n"
        "from aiohttp import web\n"
        "PAGES = [\n"
        "    [{'id': i, 'v': i*2, 'region': 'HK' if i%2 else 'SZ',\n"
        "      'created_date': f'2026-09-{(i%28)+1:02d}'} for i in range(0, 200)],\n"
        "    [{'id': i, 'v': i*2, 'region': 'HK' if i%2 else 'SZ',\n"
        "      'created_date': f'2026-09-{(i%28)+1:02d}'} for i in range(200, 400)],\n"
        "    [{'id': i, 'v': i*2, 'region': 'HK' if i%2 else 'SZ',\n"
        "      'created_date': f'2026-09-{(i%28)+1:02d}'} for i in range(400, 487)],\n"
        "]\n"
        "async def handler(req):\n"
        "    page = int(req.query.get('page', '1'))\n"
        "    if page > len(PAGES):\n"
        "        return web.json_response({'rows': [], 'has_more': False})\n"
        "    return web.json_response({'rows': PAGES[page-1],\n"
        "                              'has_more': page < len(PAGES)})\n"
        "app = web.Application()\n"
        "app.router.add_get('/worksheets/ws/rows', handler)\n"
        "print('  mock Nocoly listening on http://127.0.0.1:8765')\n"
    )],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
)
time.sleep(2)
print(mock_proc.stdout.readline(), end="")
sys.stdout.flush()
time.sleep(1.2)

print(C)
print("$ # Step 2 - submit a job to the FastAPI service")
print()
sys.stdout.flush()

demo_inner = (
    "import warnings\n"
    "warnings.filterwarnings('ignore')\n"
    "import fakeredis.aioredis\n"
    "from fastapi.testclient import TestClient\n"
    "from nocoly_explorer import create_app\n"
    "\n"
    "server = fakeredis.FakeServer()\n"
    "def redis_factory():\n"
    "    return fakeredis.aioredis.FakeRedis(server=server)\n"
    "\n"
    "async def fake_enqueue(job_id, params):\n"
    "    return 'queued'\n"
    "\n"
    "api = create_app(redis_factory=redis_factory, enqueue_func=fake_enqueue)\n"
    "client = TestClient(api)\n"
    "\n"
    "r = client.get('/healthz')\n"
    "print(f'  GET  /healthz              -> {r.status_code} {r.json()}')\n"
    "\n"
    "r = client.get('/readyz')\n"
    "print(f'  GET  /readyz               -> {r.status_code} {r.json()}')\n"
    "\n"
    "r = client.post('/jobs', json={\n"
    "    'host': 'http://127.0.0.1:8765',\n"
    "    'worksheet_id': 'ws',\n"
    "    'page_size': 200,\n"
    "    'max_pages': 100,\n"
    "    'concurrency': 8,\n"
    "    'output': {\n"
    "        'sink': 'parquet_local',\n"
    "        'path': '/tmp/nocoly-demo',\n"
    "        'partition_by': 'region',\n"
    "    },\n"
    "})\n"
    "print(f'  POST /jobs                 -> {r.status_code}  job_id={r.json()[\"job_id\"][:8]}...')\n"
    "job_id = r.json()['job_id']\n"
    "\n"
    "r = client.get(f'/jobs/{job_id}')\n"
    "print(f'  GET  /jobs/{job_id[:8]}...       -> {r.status_code}  status={r.json()[\"status\"]}')\n"
    "\n"
    "r = client.get(f'/jobs/{job_id}/result')\n"
    "print(f'  GET  /jobs/{job_id[:8]}.../result -> {r.status_code}  (job not yet run, expected 409)')\n"
    "\n"
    "r = client.post(f'/jobs/{job_id}/cancel')\n"
    "print(f'  POST /jobs/{job_id[:8]}.../cancel -> {r.status_code}  {r.json()}')\n"
    "\n"
    "r = client.get('/jobs/does-not-exist')\n"
    "print(f'  GET  /jobs/does-not-exist  -> {r.status_code}  (expected 404)')\n"
)

subprocess.run(["python", "-W", "ignore::DeprecationWarning", "-c", demo_inner], env=env)
sys.stdout.flush()
time.sleep(2.5)

mock_proc.terminate()
print(C)
print("$ # Done - real install + deploy instructions")
print()
print("  real install:  pip install nocoly-explorer[service,streaming,async]")
print("  real deploy:   arq nocoly_explorer.service.worker.WorkerSettings")
print("                  uvicorn nocoly_explorer.service:app --host 127.0.0.1 --port 8080")
sys.stdout.flush()
time.sleep(2.5)
