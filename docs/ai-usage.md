# AI use and verification record

## AI assistance used

AI assistance was used to analyze the assignment, compare the supplied validator to the written execution model, propose the architecture and deterministic scheduling policy, and draft the backend, dashboard, Docker configuration, and documentation.

Tool: Codex desktop, GPT-6.

Some prompts from this work:

1. “read [the assignment brief] carefully, think deeply, examine other files in folder, and create a strong detailed plan”
2. “does this plan fits everything” followed by the complete assignment requirements.
3. “ok if you think plan is solid and fits to document, start implementing it”
4. “isnt this clear?” followed by the explicit concurrent execution-model requirements.
5. “plan and change acording to that”
6. “can we make concurreny allowence as a optional parameter?”
7. “are belows hard to implement:” followed by the four optional bonus features.
8. “plan and implement all those”

The initial implementation used serial execution per server to satisfy the supplied validator. After review against the assignment's explicit execution model, concurrent execution was added as an option: the simulator can run several requests per server with even per-tick CPU sharing and aggregate memory reservations. Serial execution remains the default because the supplied validator rejects overlapping requests on a server (see [`design.md`](design.md#validator-compatibility)). The input and trace schemas remain unchanged.

## Verification performed

Docker Compose builds and starts successfully. The API container becomes healthy; `/health`, `/docs`, and the dashboard at port 8080 return HTTP 200. Startup verification found and fixed two issues: the original short bind-mount syntax could not parse this workspace's colon-containing directory name, and FastAPI rejected the delete route's inferred response for HTTP 204. The Compose mount now uses long syntax and the delete endpoint returns an explicit empty `Response`.

Dashboard server add, edit, and delete were exercised in the browser. The dashboard's Run simulation button also completed the bundled workload and showed the summary and trace download. API checks confirmed that server configuration survives an API container restart. The temporary verification server was deleted afterward; the configured sample servers `s1` and `s2` remain.

The original serial-policy bundled trace was generated with run ID `3d7e4e1cad3b4541856e087ca1332ca98dfb24f98bebce6fadaeca2053f01e93`. It was validated with the original validator before the policy change. That trace and its verification describe the superseded implementation.

The original uploaded scenario passed the original validator with exit code 0: five requests, four started and finished, one dropped because its memory requirement exceeded every server, and a queued request that exercised same-tick finish/start behavior. Uploaded run ID: `b7ad4f1154614bc1f403b864f2f4a1d5ecc7c3a99c533bb9fc61b9d0aebd6616`. This result also describes the superseded serial policy.

## Follow-up: shared-CPU execution model

The user highlighted that the brief's execution model may expect multiple active requests per server with equal CPU sharing, aggregate memory limits, and per-tick start-rate limits. The simulator now has an `allow_concurrency` option. It defaults to `false` for compatibility with the supplied validator; enabling it uses exact fractional CPU sharing and aggregate memory checks. The dashboard exposes this option as a checkbox. The deterministic scheduler and tie-breakers are described in [`design.md`](design.md).

The supplied `validate_run.py` contains single-active checks: it rejects overlapping requests on a server and expects each request to receive the server's full CPU. The supplied script is left unchanged. Default runs pass it; concurrent runs use the separate `validate_run_shared.py` for local diagnosis and may fail the supplied validator if they overlap.

On 2026-10-27, `docker compose up --build -d` completed with the API healthy and the dashboard running. The default bundled run (concurrency off) passed the unchanged supplied `validate_run.py` with exit code 0: 4 requests, 4 started, 4 finished, 0 dropped; average wait 0.25 ticks, p50 0, p95 1, maximum 1. Its run ID is `3d7e4e1cad3b4541856e087ca1332ca98dfb24f98bebce6fadaeca2053f01e93`; repeated runs produced the same ID and trace SHA-256 `6c187ab920846353bc5fb66799eddde0fe4918729add40bce8bf061bdfb91421`.

The concurrent bundled run passed `validate_run_shared.py` with exit code 0: 4 requests, 4 started, 4 finished, 0 dropped; all wait metrics were 0 ticks. Its run ID is `6e5cadd957614d7f641f038991c55d1b44f60d9fafde898528704be24d28bdc7`, and repeated runs produced the same trace SHA-256 `28dcd32bbefdfb3dd6e167a135dcfbe6a3316d91ecf6698f8370e35a066a4f45`. That concurrent trace fails the supplied validator because it overlaps on `s1` and finishes according to shared CPU.

The uploaded scenario was submitted through the simulation API. Dashboard server management was exercised directly in the browser; the dashboard's file chooser itself was not exercised in this pass. GitHub publication remains the submitter's step.

## Follow-up: optional bonus features

The bonus-feature request was: “are belows hard to implement:” followed by the assignment's four optional features, then “plan and implement all those.” This directed implementation of simulation visualization, interchangeable scheduling strategies, performance metrics, and autoscaling.

The simulator now supports `projected_drain` (default), `least_loaded`, and `round_robin` placement strategies. It records deterministic per-tick CPU, memory, queue, active-request, work, start/finish, and drop metrics. The React dashboard renders request execution lanes, CPU and queue charts, and cluster/per-server aggregates. An opt-in CPU autoscaler can scale capacity up or down using the prior tick's utilization, queued requests, and dropped-request rate, subject to configured thresholds, bounds, and cooldown. Its decisions are saved in the companion `metrics.json`; the request trace schema and provided validator remain unchanged.

Autoscaling and concurrent CPU-sharing modes are opt-in. The supplied validator is designed for fixed capacity and exclusive per-server execution, so the default fixed-capacity, exclusive mode remains the validator-compatible run. Autoscaled runs are explainable from their metrics companion but are not replayable by the supplied static-capacity validator.

Verification after implementing the bonus features (2026-10-27):

- `docker compose build` succeeded. The API and web services started healthy; the dashboard and API docs returned HTTP 200.
- The default simulation passed the unchanged supplied `validate_run.py` (4 started, 4 finished, 0 dropped). Repeating it produced the same run ID (`4648617268c6bee7800f860c762ca426de1cb712de776c1916893853e0736c83`) and trace SHA-256 (`6c187ab920846353bc5fb66799eddde0fe4918729add40bce8bf061bdfb91421`). The default run was restored as the latest trace after exercising other modes.
- `least_loaded` and `round_robin` fixed-capacity runs each passed the supplied validator. A concurrency-enabled run passed `validate_run_shared.py`, which checks the shared-CPU execution model.
- An autoscaling run exercised both scale-up and scale-down decisions. Repeating it produced the same run ID and identical metrics SHA-256 (`a6713a5c41b8d4bd3ba5d98c2761246f6048f58de169ffd1272443bdddc7c98f`). The supplied validator does not model changing CPU capacity, so autoscaling runs are not validated by that script; autoscaling remains off by default.

These checks verify the Docker build, default validator-compatible path, alternate fixed-capacity strategies, concurrent-mode checker, and deterministic autoscaler decisions. Browser interaction for the new charts was not separately automated in this verification pass.

## Follow-up: review and hardening with Claude Code

Tool: Claude Code (Claude Opus 5.5), 2026-10-27 and 2026-10-28.

Prompts:

1. “examine project and codes deeply, does this project fits to:” followed by the complete assignment brief.
2. “fix all, btw default is one request per server bc other ai said validator (dont change) expects it to work like that is it true?”

### Review

The review read every source file and document, then tested the project in Docker:

- 1,800 randomized runs, each checked by both validators and by an independent checker written from the brief.
- Determinism checks with shuffled input.
- API and dashboard checks on an isolated copy of the stack.
- Load tests.

It confirmed that the mandatory requirements were met, apart from publishing the repository, and found these gaps:

- Overload cost grew quadratically: 5,000 requests took about 100 seconds, and a 4,500-request upload through the dashboard returned HTTP 504 when Nginx's 60-second timeout expired.
- The engine stepped through every tick and stored every sample: one 200,000-tick request took 12 seconds and produced a 173 MB `metrics.json`.
- There were no automated tests.
- A server ID containing `/` could be created but not edited or deleted.
- The queue was unbounded with no overload controls, and shared-CPU mode could starve large requests: one request arriving at tick 1 started at tick 180.
- State files were written owner-only (0600) as root.
- Run IDs changed with autoscaler thresholds even when autoscaling was off.
- There was no way to import a new `servers.json`.
- The dashboard reloaded the whole latest run after every server edit.
- `.gitignore` would have committed the brief PDF, the original data zip and IDE files, and there was no npm lockfile.
- The README did not show how to run the supplied validator.
- `simulate()` was a single 470-line function with strategies in an if/elif chain.
- Autoscaled runs could not be validated.

The answer to the second prompt's question was yes: the supplied validator requires one request per server. It computes `finish = start + ceil(work / cpu)` and rejects overlapping intervals on a server, and the provided example trace follows the same rule. The evidence is recorded in [`design.md`](design.md#validator-compatibility). The default mode was kept, and `validate_run.py` was not modified.

### Changes

- The engine was restructured into tick phases on a `_Simulation` class, with a `PlacementStrategy` registry.
- An indexed waiting queue finds the next request to start without rescanning the queue.
- Ticks that only consume work are applied in one step, and long metric series are bucketed to at most 1,000 points.
- `QueuePolicy` adds `max_queue_length`, `max_wait_ticks` and `starvation_guard_ticks`, and the run summary counts drops by reason.
- Server IDs are restricted to URL-safe characters.
- `PUT /api/servers` imports a `servers.json`, and the dashboard can import and export it.
- State files are written 0644.
- Run IDs leave out settings that cannot affect the result.
- The dashboard has controls for the overload limits, reloads only the server list after edits, and caps the timeline at 2,000 requests.
- `validate_run_shared.py` accepts `--run-dir`, explains queue-limit and timeout drops, and replays autoscaler CPU changes.
- Nginx allows 300-second API calls and 50 MB uploads.
- A pytest suite runs through a `tests` Compose service.
- The frontend has a lockfile and uses `npm ci`.
- The README, design notes and `.gitignore` were updated.

### Verification (2026-10-28)

- The rewritten engine was compared with the previous engine on 1,800 randomized runs: 150 workloads, each run in both execution modes, all three strategies, and autoscaling off and on with random settings. Workloads included idle gaps, infeasible requests, zero-rate servers and requests of up to 5,000 work units. Traces, summaries, autoscaler decisions, sample counts and per-tick series were identical, and aggregate metrics matched within float rounding.
- The default bundled trace is byte-identical to the committed `run.jsonl` (SHA-256 `6c187ab920846353bc5fb66799eddde0fe4918729add40bce8bf061bdfb91421`) and passes the unchanged supplied validator. The shared-CPU bundled trace is also unchanged (`28dcd32bbefdfb3dd6e167a135dcfbe6a3316d91ecf6698f8370e35a066a4f45`). Their run IDs changed because disabled autoscaler settings no longer count: default `c9f820f85024401f4f7e068012cd356c944aabe338667d55b2d36cf37d4fd1f0`, shared CPU `551daa7030cd8da451c4404ea2e0a8426211aa7fdc17490240705db9e8bd47ab`.
- `docker-compose run --rm tests`: 122 passed.
- Isolated API and dashboard containers were built from this code:
  - The 4,500-request upload through Nginx returned HTTP 200 in 1.7 seconds.
  - Queue-limited, shared-CPU, starvation-guarded and autoscaled runs passed `validate_run_shared.py --run-dir`; the exclusive-mode ones also passed the supplied validator.
  - Server CRUD, import, ID validation and option validation returned the expected status codes.
  - The dashboard rendered the new controls, the downsampled charts and the capped timeline, imported a `servers.json` and rejected invalid JSON, with no console errors.
- Timings in the API image:
  - 5,000 overloaded requests: 1.5 to 2.0 seconds.
  - 50,000 overloaded requests: 17 to 19 seconds.
  - One 200,000-tick request: 0.07 seconds, with a 0.7 MB `metrics.json`.
