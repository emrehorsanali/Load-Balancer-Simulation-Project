# Design notes

## Architecture

The backend is a FastAPI application. Its simulator (`backend/app/simulation.py`) is a pure Python module that parses the supplied server/request formats and returns a trace, summary, and metrics without depending on HTTP or persistent storage. API routes (`main.py`) handle server CRUD and import, run orchestration, and trace downloads; `storage.py` persists state as files.

The frontend is a React dashboard served by Nginx. Nginx sends `/api/` requests to the API service on the Compose network. Docker Compose builds and runs the web and API services, plus a `tests` service that only runs on demand. Runtime state is a host-mounted `state/` directory; JSON files are updated atomically and written world-readable, so the host can copy them even though the container writes as root. This avoids an external database service and preserves server edits and traces between container restarts.

## Data and lifecycle

The supplied `servers.json`, `requests.csv`, and `run.jsonl` schemas are unchanged. Server configuration is seeded into `state/servers.json` only on first startup. CRUD actions modify that runtime copy, and `PUT /api/servers` replaces it with an imported `servers.json` document using the same validation as the seed file. At the beginning of a run, the API reads the current server list once and passes that immutable snapshot to the simulator.

Server IDs are limited to letters, digits, `.`, `_` and `-`, starting with a letter or digit, up to 80 characters. They appear in API paths, so an ID such as `rack/1` could be created but never edited or deleted.

Each request follows one of these paths:

```text
REQUEST_ARRIVED -> REQUEST_STARTED -> REQUEST_FINISHED
                -> REQUEST_DROPPED
```

A request is dropped for one of three reasons, counted separately in the run summary's `drop_reasons`:

- `no_capacity`: no server with a nonzero start limit has enough memory. These requests are dropped at arrival, because server memory and start limits never change during a run.
- `queue_full`: an optional queue limit was exceeded.
- `wait_timeout`: an optional wait limit was exceeded.

Everything else waits in the queue. A zero start limit disables that server. A simulation requires at least one server, matching the supplied validator's requirement that `servers.json` not be empty.

## Time and scheduler

One tick equals one second. Each tick runs these phases in a fixed order:

1. Requests whose work reached zero in the preceding tick finish and release their memory.
2. Arrivals at this tick enter the queue, or are dropped if nothing could ever fit them.
3. The autoscaler, if enabled, may change one server's CPU.
4. Waiting requests are started.
5. Overload limits drop requests.
6. Each server's CPU is divided among its active requests.
7. The tick's metrics are recorded.

Every request started at tick `t` receives service during `[t, t+1)`. A request finishes at the next boundary after its remaining work reaches zero or below.

The run option `allow_concurrency` controls whether a server may have multiple active requests. It defaults to `false` so the default trace passes the supplied validator (see [Validator compatibility](#validator-compatibility)). With concurrency off, each server runs one request at a time and that request receives the server's full CPU. With concurrency on, the server divides its CPU evenly among active requests each tick. CPU shares and remaining work use exact rational arithmetic (`Fraction`), so a 10-unit CPU split three ways completes 10 work units in exactly 3 ticks.

Memory is reserved for each request's full interval. In concurrent mode, the scheduler admits a request only when the sum of active reservations plus its `mem_mb` fits.

Queue order is `(arrival tick, request_id)`. The scheduler starts, at each step, the oldest waiting request that fits the largest free memory among servers that can still start a request this tick, then lets the placement strategy pick among the servers that fit it. That is the request a front-to-back scan of the queue would pick, so a smaller later request may use capacity an earlier one cannot. The queue keeps a min-tree of memory demand by queue position, so finding that request takes O(log² n) instead of rescanning thousands of waiting requests every tick. A server may start no more than `rate_limit_per_sec` requests per tick in either mode.

When a tick has no arrival, start, or drop, nothing can start until a request finishes. The following ticks then only consume work, so the simulator applies them in one step, up to the earliest of these events:

- the next completion
- the next arrival
- the next wait timeout
- the next tick at which the autoscaler could act

The skipped ticks are recorded as identical samples. The trace and metrics are the same as stepping one tick at a time, which was checked against the previous tick-by-tick engine on randomized workloads in every mode. Run time grows with the number of events, not the number of ticks.

## Placement strategies

Strategies live in a registry (`STRATEGIES` in `simulation.py`). Each is a small `PlacementStrategy` subclass whose `choose(request, candidates)` picks one server from those that can start the request now. Candidates expose remaining work, active requests, free memory, and current CPU. A new instance is created for each run, so a strategy may keep state, as round robin does. Adding a strategy means writing one class and one registry entry; the API and dashboard list strategies from `GET /api/configuration`.

- **`projected_drain`** (default) estimates completion ticks as the ceiling of `(active remaining work + new request work) / current CPU capacity`, then selects the eligible server with the lowest estimate. Ties prefer fewer active requests after placement, less free memory after placement (packing into the tighter fit), then lexicographically smaller server ID.
- **`least_loaded`** selects the eligible server with the lowest active remaining work divided by its current CPU capacity. Ties prefer fewer active requests, less free memory after placement, then lexicographically smaller server ID.
- **`round_robin`** selects the next eligible server in sorted server-ID order, rotating the cursor after each placement. Ineligible servers are skipped without changing the stable ordering.

All strategies keep FIFO request scanning, memory checks, per-tick rate limits, and deterministic tie-breaking.

## Overload handling

By default the queue is unbounded: every request that can ever run eventually runs, and overload shows up as waiting time and queue length. Three opt-in controls (`QueuePolicy`, set per run from the dashboard or API) bound that behavior:

- **`max_queue_length`**: after a tick's starts, if more requests wait than the limit allows, that tick's newest arrivals are dropped (largest request ID first) until the queue fits. A request that can start on arrival is never dropped by this limit, and 0 turns the queue off entirely.
- **`max_wait_ticks`**: a request that has not started by tick `arrival + max_wait_ticks` is dropped at that tick, after the tick's starts. A request may therefore wait at most that many ticks.
- **`starvation_guard_ticks`** (shared-CPU mode): skip-ahead scheduling lets a stream of small requests keep taking memory that a large request needs. In a test workload, a 1,000 MB request arriving at tick 1 behind 400 MB requests did not start until tick 180. Once the oldest waiting request has waited this long, servers that could ever fit it stop starting younger requests, so their memory drains. With a 5-tick guard the same request started at tick 9. With one request per server, the oldest request is always offered every free server first and cannot starve, so the guard has no effect there and is left out of the run ID.

Queue-limit and timeout drops are ordinary `REQUEST_DROPPED` events, so exclusive-mode runs that use them still pass the supplied validator. `validate_run_shared.py` checks that each such drop is explained by the run's limits, and that the queue never exceeds `max_queue_length`.

The tick's drop rate (the autoscaler's error-rate signal) is dropped requests per arrival in that tick, capped at 100 percent. A tick that drops timed-out requests without new arrivals counts as 100 percent.

The simulator does not preempt work. In concurrent mode, each active request receives its equal share at the start of a tick; if that share exceeds its remaining work, the excess is discarded when it completes at the next boundary.

## Metrics and visualization

The simulator records a sample for every simulated busy tick and every tick containing an arrival, finish, or drop. Long intervals where the cluster is fully idle and no autoscaler action is due are skipped. Each sample includes:

- cluster and per-server CPU capacity, completed work, and CPU utilization
- memory reserved and memory utilization
- active requests and queue depth
- starts, finishes, and drops

CPU utilization is completed work divided by capacity for that tick; memory utilization is reserved memory divided by configured memory.

Per-run aggregates are computed from every sample:

- average and peak utilization
- peak queue length and peak active requests
- work completed
- per-server request starts

Series up to 1,000 samples are stored per tick. Longer series are stored as up to 1,000 points, each averaging gauges and summing counters over an equal bucket of ticks (`series_ticks_per_point`, with `tick`/`tick_end` per point). This keeps `metrics.json` near 1 MB even for runs with hundreds of thousands of ticks.

The dashboard uses the lifecycle trace to render each request's queue wait and execution interval on its assigned server lane, up to the first 2,000 started requests. It charts cluster and server CPU utilization and cluster queue length, and displays per-server and cluster-wide aggregates. Metrics are stored as `metrics.json` next to each saved run and are served through `/api/simulations/{run_id}/metrics`; they are not added as unsupported event types to `run.jsonl`.

## Autoscaling

Autoscaling is opt-in and disabled by default. It vertically changes simulated CPU capacity per server, while leaving memory and start-rate limits unchanged. At each tick boundary it checks the preceding sample:

- Scale up one eligible server if CPU utilization reaches the upper threshold, queue length reaches its threshold, or drop rate reaches its threshold.
- Scale down one eligible server if CPU utilization is at or below the lower threshold and the queue and drop rate are both zero.
- Respect the configured per-server cooldown and minimum/maximum CPU percentages relative to the initial server configuration. Each change moves capacity by one quarter of baseline CPU (rounded up), with a minimum step of one unit.
- Choose a scale-up target by highest prior CPU utilization, then most active requests, then server ID. Choose a scale-down target by highest capacity relative to baseline, then lowest prior utilization, then server ID.

Each decision and its input signals are saved in the run's metrics. Capacity changes apply only within that run and do not edit `state/servers.json`.

The supplied validator accepts only static server capacities. It cannot reconstruct an autoscaled run from `run.jsonl`, because that event schema has no capacity-change event. `validate_run_shared.py --run-dir` replays the recorded CPU changes and checks every completion tick. Keep autoscaling off when producing a trace for the supplied validator; the default remains compatible.

A drop signal may cause a CPU increase even when the actual cause is a memory or rate limit, which CPU scaling cannot resolve. This trade-off is visible in the recorded signal and decision history.

## Validator compatibility

The supplied `validate_run.py` treats a server as occupied by one request at a time:

- Its pass over started requests computes `finish = start + ceil(work_units / cpu_units_per_tick)` and reports any other finish tick as an error. That formula is only true when a request receives the server's whole CPU every tick.
- Its overlap check, commented "Single-run per server constraint", rejects any two `[start, finish)` intervals on the same server.

The brief points the same way in two places. Its data section says a request "occupies" a server for `work_units/cpu_units_per_tick` ticks. Its example `run.jsonl` holds `r3` from tick 1 to tick 2 while `s1` had the memory and start budget to run it next to `r1`.

The execution model section says a server *may* execute multiple requests concurrently. So the scheduler's default choice of never co-locating requests is allowed: with one active request, the even CPU split gives that request the whole CPU, and the brief's rules and the validator agree.

Concurrent mode follows the shared-CPU and aggregate-memory rules, but a trace with overlap cannot pass the supplied validator. The provided `validate_run.py` is left unchanged. The separate `validate_run_shared.py` checks traces from every mode, including the shared-CPU replay; it is not a substitute for the supplied validator.

## Determinism and output

Simultaneous arrivals are ordered by request ID. Server placement uses the stable tuples above. At equal ticks, trace lines are ordered by `(t, event, request_id, server_id)`, placing finishes before starts by event name. JSON is compact and contains no wall-clock values. Input is normalized by sorting servers by ID and requests by `(t, request_id)` before a run fingerprint is calculated.

The SHA-256 fingerprint is the versioned run ID. It covers the normalized servers and requests, the execution mode, the strategy, and only the settings that can change the outcome. The autoscaler thresholds count only when autoscaling is on, and queue limits count only when set. Equal runs therefore always share an ID. Each run directory stores:

- `run.jsonl`
- normalized `servers.json` and `requests.csv` snapshots
- `metrics.json`
- a summary including the execution mode and queue policy

The stable latest copy is `state/run.jsonl`. Atomic replacement prevents readers from seeing a partially written artifact. Repeating a run with the same normalized configuration, requests, and options reuses the same run ID and produces the same trace bytes. Saved snapshots let a previous run be validated after dashboard configuration changes.

## Testing

`docker compose build tests && docker compose run --rm tests` runs the pytest suite in `backend/tests`. Alongside example-based tests of each rule, it runs randomized workloads in both modes and all strategies through both validators, plus an independent per-tick replay of the brief's rules. That replay checks memory, exclusivity, the even CPU split, exact completion, and that no request waits while some server could start it. It also checks determinism under shuffled input, the API against a temporary state directory, and run time under sustained overload.

## Limitations

The API runs simulations synchronously, one at a time. With the event-skipping engine, 5,000 overloaded requests take about 2 seconds and 50,000 about 20 seconds, which fits a request/response flow; Nginx allows 300 seconds per API call and 50 MB uploads. A production workload would need an asynchronous job model and persistent run status. Autoscaling adjusts only CPU; it does not add server instances or change memory/rate capacity.
