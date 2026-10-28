# Load Balancer Simulation

A deterministic Python simulation service with a server management dashboard. The API and dashboard run in Docker Compose; no local Python or Node.js setup is required.

## How to run

Requirements: Docker Desktop or Docker Engine with the Compose plugin.

```sh
docker compose up --build
```

The equivalent standalone Compose command is `docker-compose up --build`.

Open the dashboard at <http://localhost:8080>. The API documentation is at <http://localhost:8000/docs>. Stop the services with `Ctrl+C`; run `docker compose down` to remove the containers.

The first startup seeds `state/servers.json` from the supplied `servers.json`. Dashboard edits are saved in `state/servers.json` and persist across restarts. The sample `requests.csv` is used when a simulation is started without uploading a file. An uploaded CSV must contain `t,request_id,work_units,mem_mb` columns.

To try different data, use **Import servers.json** in the dashboard (it replaces the server list for later runs) and upload a requests CSV in the run panel. **Export servers.json** downloads the current list in the same format.

Generated traces are written to `state/run.jsonl` and versioned by deterministic input/configuration fingerprint under `state/runs/<run-id>/`. Each version includes its `run.jsonl`, `metrics.json`, `servers.json`, `requests.csv`, and `summary.json`, so it can be inspected and validated after later configuration changes. The latest summary is in `state/latest.json`. The repository root's `run.jsonl` is the example trace the simulator generates for the bundled `servers.json` and `requests.csv`. It has the same 12 events as the example trace supplied with the assignment, in the simulator's deterministic order, and passes `validate_run.py`. To refresh it after a run, copy the latest trace:

```sh
cp state/run.jsonl run.jsonl
```

## Validate a run

The supplied `validate_run.py` is unchanged and is also copied into the API image. To check the latest run of the bundled data:

```sh
docker compose exec api python validate_run.py --servers servers.json --requests requests.csv --run /state/run.jsonl
```

A run that used edited servers or an uploaded CSV must be checked against its own saved snapshots. Replace `<run-id>` with the ID shown in the dashboard or in `state/latest.json`:

```sh
docker compose exec api python validate_run.py --servers /state/runs/<run-id>/servers.json --requests /state/runs/<run-id>/requests.csv --run /state/runs/<run-id>/run.jsonl
```

Shared-CPU and autoscaled runs are checked with the project's `validate_run_shared.py`, which reads the run's queue policy and autoscaler CPU changes from the saved run directory:

```sh
docker compose exec api python validate_run_shared.py --run-dir /state/runs/<run-id>
```

`validate_run.py` needs only Python 3.7 or newer, so `python3 validate_run.py --servers servers.json --requests requests.csv --run run.jsonl` also works on the host for the committed example.

## Run the tests

```sh
docker compose build tests && docker compose run --rm tests
```

This builds a test image from the current code and runs the pytest suite in `backend/tests`; `docker compose up` does not start it. It covers the bundled example trace, both validators, CPU sharing, memory and rate limits, tie-breaks, determinism under shuffled input, randomized workloads checked against an independent replay of the brief's rules, overload limits, starvation, autoscaler bounds, input validation, the HTTP API, and run time under sustained overload.

## Execution modes

By default each server runs **one request at a time**. That is the only behavior the supplied `validate_run.py` accepts:

- It rejects any two requests whose `[start, finish)` intervals overlap on the same server (the "single-run per server" check).
- It requires `finish = start + ceil(work_units / cpu_units_per_tick)`, which holds only when a request gets the server's full CPU every tick.
- The provided example `run.jsonl` behaves the same way: `r3` arrives at tick 1 but waits for `s1` to finish `r1` at tick 2, although `s1` had memory and start capacity for it.

The brief says a server *may* run several requests at once, so running one at a time is a valid special case of its execution model: with one active request, the even CPU split gives that request the whole CPU.

The dashboard's **Allow concurrent requests** option (`allow_concurrency`) runs the full shared-CPU model: each tick's CPU is split evenly across active requests, with exact fractional work, and memory reservations are summed. Those traces fail the supplied validator whenever requests overlap, so check them with `validate_run_shared.py` instead.

## Scheduling and overload

- Time is discrete, with one tick equal to one second. Requests are ordered by arrival tick, then `request_id`.
- Waiting requests are scanned in FIFO order. A request that cannot fit yet is skipped, so a later, smaller request may use free capacity.
- Three deterministic placement strategies are available: **Projected drain time** (default), **Least loaded server**, and **Round robin**. Ties break by fewer active requests, then the tighter memory fit, then server ID. See [`docs/design.md`](docs/design.md).
- A server starts at most `rate_limit_per_sec` requests per tick. A completing request releases memory at its finish tick, and the server can start another request at that boundary.
- A request is dropped on arrival if no server with a nonzero start limit has enough memory for it. Otherwise it waits.
- Waiting is unlimited by default. Under **Limit queueing under overload**, `max_queue_length` drops a tick's newest arrivals while more requests wait than the limit allows, and `max_wait_ticks` drops a request that has not started that many ticks after it arrived. Both drops appear as `REQUEST_DROPPED` events, and default-mode runs that use them still pass the supplied validator.
- In shared-CPU mode, a large request can starve while smaller requests keep taking freed memory. `starvation_guard_ticks` stops servers that could fit the oldest waiting request from starting younger ones once it has waited that long.
- **Enable CPU autoscaling** adds an in-run CPU controller that uses the previous tick's CPU utilization, queue length, and drop rate. It changes one server's CPU at a time, within the configured bounds and cooldown, and never edits the saved server list. The supplied validator cannot check an autoscaled run, because CPU changes are recorded in `metrics.json` rather than in the trace; `validate_run_shared.py --run-dir` replays them.
- The output is serialized in stable `(t, event, request_id, server_id)` order, with compact JSON and no wall-clock timestamps.

Under sustained overload, the simulator processes 5,000 requests in about 2 seconds and 50,000 in about 20 seconds. Ticks where nothing but remaining work changes are skipped in one step, so a single 200,000-tick request takes under 0.1 seconds. Metric series longer than 1,000 ticks are charted as bucket averages, which keeps `metrics.json` near 1 MB; summary values still use every tick.

## What the dashboard does

- Lists configured servers and supports add, edit, delete, and import/export of `servers.json`.
- Starts a simulation with the bundled requests or an uploaded requests CSV, with a selectable placement strategy, execution mode, autoscaler, and overload limits.
- Shows each request's queue wait and execution interval on a per-server timeline (the first 2,000 requests of very large runs).
- Shows per-server and cluster CPU, memory, queue, work, and drop-rate metrics, and downloads the generated `run.jsonl`.

Server changes apply to later runs. Each run uses an immutable server configuration snapshot. Deleting all servers is allowed; a simulation is disabled until at least one server is added again.

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/servers` | List server configuration |
| `PUT` | `/api/servers` | Replace all servers with a `servers.json` document |
| `POST` | `/api/servers` | Add a server |
| `PUT` | `/api/servers/{server_id}` | Edit server capacity |
| `DELETE` | `/api/servers/{server_id}` | Delete a server |
| `POST` | `/api/simulations` | Run bundled requests or upload `requests_file` (see the multipart options below) |
| `GET` | `/api/simulations/latest` | Read latest run summary |
| `GET` | `/api/simulations/latest/trace` | Download latest `run.jsonl` |
| `GET` | `/api/simulations/latest/metrics` | Read latest per-tick and aggregated performance metrics |
| `GET` | `/api/simulations/{run_id}/trace` | Download a versioned trace |
| `GET` | `/api/simulations/{run_id}/metrics` | Read metrics for a versioned run |
| `GET` | `/api/configuration` | Current servers and available strategies |

`POST /api/simulations` accepts these multipart fields:

- `allow_concurrency`
- `scheduling_strategy`
- `enable_autoscaling`, plus the autoscaler thresholds (`scale_up_cpu_percent`, `scale_down_cpu_percent`, `queue_threshold`, `error_rate_threshold_percent`, `cooldown_ticks`, `min_cpu_percent`, `max_cpu_percent`)
- `max_queue_length`, `max_wait_ticks`, `starvation_guard_ticks` (each optional)

Server IDs may contain letters, digits, `.`, `_` and `-`, start with a letter or digit, and be at most 80 characters, so every ID works in API paths.

## Project layout

```text
backend/app/            Simulation engine, API, and file-backed persistence
backend/tests/          Pytest suite (see "Run the tests")
frontend/               React dashboard served by Nginx
state/                  Persistent runtime configuration and generated output/metrics
servers.json            Initial server configuration
requests.csv            Sample workload
run.jsonl               Simulator-generated example trace
validate_run.py         Assignment-provided validator, unchanged
validate_run_shared.py  Checker for shared-CPU, autoscaled, and queue-limited runs
docker-compose.yml      API, dashboard, and test services
docs/                   Design and AI-use documentation
```

The application preserves the supplied data and trace schemas. Server state is stored as JSON with atomic replacement, so no database or Redis service is required.

## Submission contents

| Requirement | Where to find it |
|---|---|
| GitHub repository link | This repository |
| "How to Run" instructions | [How to run](#how-to-run), [Validate a run](#validate-a-run), [Run the tests](#run-the-tests) |
| Generated `run.jsonl` example | [`run.jsonl`](run.jsonl), regenerated byte for byte by `backend/tests/test_engine.py` |
| Scheduling approach | [`docs/design.md`](docs/design.md#time-and-scheduler), with [placement strategies](docs/design.md#placement-strategies) and [overload handling](docs/design.md#overload-handling) |
| Rate limiting strategy | [`docs/design.md`](docs/design.md#rate-limiting) |
| System architecture | [`docs/design.md`](docs/design.md#architecture) |
| Key assumptions and trade-offs | [`docs/design.md`](docs/design.md#key-assumptions-and-trade-offs) |
| How AI was used | [`docs/ai-usage.md`](docs/ai-usage.md): tools, prompts, and verification steps |
