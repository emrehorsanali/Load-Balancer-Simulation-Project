#!/usr/bin/env python3
"""Optional validator for traces using the concurrent shared-CPU model.

Usage:
  python validate_run_shared.py --servers servers.json --requests requests.csv --run run.jsonl
  python validate_run_shared.py --run-dir state/runs/<run-id>

``--run-dir`` reads a saved run's snapshots, its queue policy from
``summary.json`` and its autoscaler CPU changes from ``metrics.json``. With
separate files, pass ``--max-queue-length``, ``--max-wait-ticks`` and
``--metrics`` to describe the same settings.

The assignment-provided ``validate_run.py`` is kept unchanged. See README.md
for the incompatibility between its single-request assumption and the written
shared-CPU execution model.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ServerSpec:
    id: str
    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int


@dataclass(frozen=True)
class RequestSpec:
    id: str
    arrival_t: int
    work_units: int
    mem_mb: int


@dataclass(frozen=True)
class ValidationIssue:
    level: str
    msg: str


def ceil_fraction(value: Fraction) -> int:
    return (value.numerator + value.denominator - 1) // value.denominator


def load_servers(path: Path) -> tuple[int, dict[str, ServerSpec]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    tick_seconds = int(data.get("tick_seconds", 1))
    if tick_seconds != 1:
        raise ValueError("This validator requires tick_seconds = 1")
    servers: dict[str, ServerSpec] = {}
    for raw in data.get("servers", []):
        sid = raw["id"]
        if sid in servers:
            raise ValueError(f"Duplicate server id in servers.json: {sid}")
        server = ServerSpec(
            sid,
            int(raw["cpu_units_per_tick"]),
            int(raw["mem_mb"]),
            int(raw["rate_limit_per_sec"]),
        )
        if server.cpu_units_per_tick <= 0 or server.mem_mb <= 0 or server.rate_limit_per_sec < 0:
            raise ValueError(f"Invalid capacity values for server '{sid}'")
        servers[sid] = server
    if not servers:
        raise ValueError("servers.json contains no servers")
    return tick_seconds, servers


def load_requests(path: Path) -> dict[str, RequestSpec]:
    requests: dict[str, RequestSpec] = {}
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"t", "request_id", "work_units", "mem_mb"}
        if not required.issubset(reader.fieldnames or set()):
            raise ValueError(f"requests.csv must contain columns: {sorted(required)}")
        for row in reader:
            rid = row["request_id"].strip()
            if not rid:
                raise ValueError("request_id cannot be empty")
            if rid in requests:
                raise ValueError(f"Duplicate request_id in requests.csv: {rid}")
            request = RequestSpec(
                rid, int(row["t"]), int(row["work_units"]), int(row["mem_mb"])
            )
            if request.arrival_t < 0 or request.work_units <= 0 or request.mem_mb <= 0:
                raise ValueError(f"Invalid request values for '{rid}'")
            requests[rid] = request
    if not requests:
        raise ValueError("requests.csv contains no requests")
    return requests


def load_events(path: Path) -> list[dict[str, Any]]:
    events = []
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc}") from exc
            if not isinstance(event, dict):
                raise ValueError(f"Event on line {line_number} is not an object")
            events.append(event)
    if not events:
        raise ValueError("run.jsonl is empty")
    return events


def load_capacity_changes(path: Path) -> dict[str, dict[int, int]]:
    """Read autoscaler decisions from a run's metrics.json as {server: {tick: cpu}}."""
    metrics = json.loads(path.read_text(encoding="utf-8"))
    changes: dict[str, dict[int, int]] = {}
    for decision in metrics.get("autoscaling", {}).get("decisions", []):
        changes.setdefault(decision["server_id"], {})[int(decision["tick"])] = int(
            decision["cpu_after"]
        )
    return changes


def validate(
    servers: dict[str, ServerSpec],
    requests: dict[str, RequestSpec],
    events: list[dict[str, Any]],
    max_queue_length: int | None = None,
    max_wait_ticks: int | None = None,
    capacity_changes: dict[str, dict[int, int]] | None = None,
) -> tuple[bool, list[ValidationIssue], dict[str, Any]]:
    issues: list[ValidationIssue] = []
    capacity_changes = capacity_changes or {}

    def error(message: str) -> None:
        issues.append(ValidationIssue("ERROR", message))

    event_types = {
        "REQUEST_ARRIVED", "REQUEST_STARTED", "REQUEST_FINISHED", "REQUEST_DROPPED"
    }
    sort_keys = []
    arrivals: dict[str, int] = {}
    started: dict[str, tuple[int, str]] = {}
    finished: dict[str, tuple[int, str]] = {}
    dropped: dict[str, int] = {}
    starts_by_server_tick: dict[tuple[str, int], int] = {}

    for event in events:
        t, kind, rid = event.get("t"), event.get("event"), event.get("request_id")
        if isinstance(t, bool) or not isinstance(t, int) or t < 0:
            error(f"Event has invalid tick: {event}")
            continue
        if not isinstance(kind, str) or kind not in event_types:
            error(f"Unknown event type '{kind}'")
            continue
        if not isinstance(rid, str) or not rid or rid not in requests:
            error(f"{kind} references an invalid request_id: {event}")
            continue
        raw_server_id = event.get("server_id", "")
        server_sort_key = raw_server_id if isinstance(raw_server_id, str) else ""
        sort_keys.append((t, kind, rid, server_sort_key))

        if kind == "REQUEST_ARRIVED":
            if rid in arrivals:
                error(f"Duplicate REQUEST_ARRIVED for '{rid}'")
            else:
                arrivals[rid] = t
                if t != requests[rid].arrival_t:
                    error(
                        f"REQUEST_ARRIVED.t mismatch for '{rid}': got {t}, "
                        f"expected {requests[rid].arrival_t}"
                    )
        elif kind == "REQUEST_DROPPED":
            if rid in dropped:
                error(f"Duplicate REQUEST_DROPPED for '{rid}'")
            else:
                dropped[rid] = t
        elif kind in {"REQUEST_STARTED", "REQUEST_FINISHED"}:
            sid = event.get("server_id")
            if not isinstance(sid, str) or sid not in servers:
                error(f"{kind} has invalid server_id '{sid}' for '{rid}'")
                continue
            if kind == "REQUEST_STARTED":
                if rid in started:
                    error(f"Duplicate REQUEST_STARTED for '{rid}'")
                else:
                    started[rid] = (t, sid)
                    key = (sid, t)
                    starts_by_server_tick[key] = starts_by_server_tick.get(key, 0) + 1
            else:
                if rid in finished:
                    error(f"Duplicate REQUEST_FINISHED for '{rid}'")
                else:
                    finished[rid] = (t, sid)

    if sort_keys != sorted(sort_keys):
        error("Events are not in deterministic (t, event, request_id, server_id) order")

    # Queue length after each tick's starts and drops, for the max_queue_length check.
    queue_after: dict[int, int] = {}
    if max_queue_length is not None:
        changes: dict[int, int] = {}
        for t in arrivals.values():
            changes[t] = changes.get(t, 0) + 1
        for t, _ in started.values():
            changes[t] = changes.get(t, 0) - 1
        for t in dropped.values():
            changes[t] = changes.get(t, 0) - 1
        waiting = 0
        for tick in sorted(changes):
            waiting += changes[tick]
            queue_after[tick] = waiting
            if waiting > max_queue_length:
                error(
                    f"{waiting} requests wait after tick {tick}, "
                    f"above max_queue_length {max_queue_length}"
                )

    for rid, request in requests.items():
        if rid not in arrivals:
            error(f"Missing REQUEST_ARRIVED for '{rid}'")
        has_start, has_drop = rid in started, rid in dropped
        if has_start == has_drop:
            error(f"'{rid}' must have exactly one of REQUEST_STARTED or REQUEST_DROPPED")
        if has_start and rid not in finished:
            error(f"'{rid}' started but never finished")
        if has_drop and rid in finished:
            error(f"'{rid}' was dropped and finished")
        if has_drop and rid in started:
            error(f"'{rid}' was dropped and started")
        if has_start:
            start_t, sid = started[rid]
            if rid in arrivals and start_t < arrivals[rid]:
                error(f"'{rid}' started before its arrival")
            if max_wait_ticks is not None and start_t - request.arrival_t > max_wait_ticks:
                error(f"'{rid}' waited {start_t - request.arrival_t} ticks, above max_wait_ticks")
            finish = finished.get(rid)
            if finish:
                finish_t, finish_sid = finish
                if finish_sid != sid:
                    error(f"'{rid}' finished on '{finish_sid}' after starting on '{sid}'")
                if finish_t <= start_t:
                    error(f"'{rid}' has a non-positive run interval")
            server = servers[sid]
            if request.mem_mb > server.mem_mb:
                error(
                    f"'{rid}' requires {request.mem_mb} MB, exceeding server '{sid}' "
                    f"capacity {server.mem_mb} MB"
                )
        if has_drop:
            drop_t = dropped[rid]
            if rid in arrivals and drop_t < arrivals[rid]:
                error(f"'{rid}' was dropped before its arrival")
            feasible = any(
                server.rate_limit_per_sec > 0 and server.mem_mb >= request.mem_mb
                for server in servers.values()
            )
            if not feasible:
                if drop_t != request.arrival_t:
                    error(f"'{rid}' fits no server and should be dropped on arrival")
            elif not (
                (max_wait_ticks is not None and drop_t == request.arrival_t + max_wait_ticks)
                or (
                    max_queue_length is not None
                    and drop_t == request.arrival_t
                    and queue_after.get(drop_t) == max_queue_length
                )
            ):
                error(f"'{rid}' was dropped although a server can eventually accept it")

    for (sid, tick), count in starts_by_server_tick.items():
        limit = servers[sid].rate_limit_per_sec
        if count > limit:
            error(f"Rate limit exceeded on '{sid}' at tick {tick}: {count} starts > {limit}")

    # Replay each server independently. Starts, finishes and CPU changes define
    # boundaries; between them CPU is split evenly across active requests.
    for sid, server in servers.items():
        starts_at: dict[int, list[str]] = {}
        finishes_at: dict[int, list[str]] = {}
        for rid, (start_t, start_sid) in started.items():
            if start_sid == sid:
                starts_at.setdefault(start_t, []).append(rid)
        for rid, (finish_t, finish_sid) in finished.items():
            if finish_sid == sid:
                finishes_at.setdefault(finish_t, []).append(rid)
        cpu_changes = capacity_changes.get(sid, {})
        boundaries = sorted(set(starts_at) | set(finishes_at) | set(cpu_changes))
        cpu = server.cpu_units_per_tick
        active: dict[str, Fraction] = {}
        for index, tick in enumerate(boundaries):
            expected_finished = {rid for rid, remaining in active.items() if remaining <= 0}
            actual_finished = set(finishes_at.get(tick, []))
            for rid in sorted(actual_finished - expected_finished):
                error(f"'{rid}' finishes early on '{sid}' at tick {tick}")
            for rid in sorted(expected_finished - actual_finished):
                error(f"'{rid}' should finish on '{sid}' at tick {tick}")
            for rid in actual_finished:
                active.pop(rid, None)

            cpu = cpu_changes.get(tick, cpu)
            for rid in sorted(starts_at.get(tick, [])):
                if rid in active:
                    error(f"'{rid}' starts twice while active on '{sid}'")
                active[rid] = Fraction(requests[rid].work_units)

            used_mem = sum(requests[rid].mem_mb for rid in active)
            if used_mem > server.mem_mb:
                error(
                    f"Memory exceeded on '{sid}' at tick {tick}: "
                    f"{used_mem} MB > {server.mem_mb} MB"
                )

            if index + 1 >= len(boundaries):
                continue
            next_tick = boundaries[index + 1]
            delta = next_tick - tick
            if delta <= 0 or not active:
                continue
            share = Fraction(cpu, len(active))
            earliest_finish = min(ceil_fraction(remaining / share) for remaining in active.values())
            if earliest_finish < delta:
                error(
                    f"A request on '{sid}' should finish before tick {next_tick} "
                    f"(expected completion at tick {tick + earliest_finish})"
                )
            for rid in active:
                active[rid] -= share * delta

        if active:
            for rid in sorted(active):
                error(f"'{rid}' remains active after the final trace boundary on '{sid}'")

    waits = sorted(
        started[rid][0] - requests[rid].arrival_t for rid in started
    )

    def percentile(values: list[int], proportion: float) -> int | None:
        if not values:
            return None
        index = max(0, min(math.ceil(proportion * len(values)) - 1, len(values) - 1))
        return values[index]

    summary = {
        "total_requests": len(requests),
        "started": len(started),
        "finished": len(finished),
        "dropped": len(dropped),
        "avg_wait_ticks": (sum(waits) / len(waits)) if waits else None,
        "p50_wait_ticks": percentile(waits, 0.50),
        "p95_wait_ticks": percentile(waits, 0.95),
        "max_wait_ticks": max(waits) if waits else None,
    }
    return not any(issue.level == "ERROR" for issue in issues), issues, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", help="Saved run directory, e.g. state/runs/<run-id>")
    parser.add_argument("--servers")
    parser.add_argument("--requests")
    parser.add_argument("--run")
    parser.add_argument("--metrics", help="metrics.json whose autoscaler decisions to replay")
    parser.add_argument("--max-queue-length", type=int)
    parser.add_argument("--max-wait-ticks", type=int)
    args = parser.parse_args()

    max_queue_length, max_wait_ticks = args.max_queue_length, args.max_wait_ticks
    metrics_path = Path(args.metrics) if args.metrics else None
    if args.run_dir:
        run_dir = Path(args.run_dir)
        servers_path = run_dir / "servers.json"
        requests_path = run_dir / "requests.csv"
        run_path = run_dir / "run.jsonl"
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        policy = summary.get("summary", {}).get("queue_policy", {})
        max_queue_length = policy.get("max_queue_length", max_queue_length)
        max_wait_ticks = policy.get("max_wait_ticks", max_wait_ticks)
        if metrics_path is None and (run_dir / "metrics.json").is_file():
            metrics_path = run_dir / "metrics.json"
    elif args.servers and args.requests and args.run:
        servers_path, requests_path, run_path = Path(args.servers), Path(args.requests), Path(args.run)
    else:
        parser.error("pass --run-dir, or all of --servers, --requests and --run")

    _, servers = load_servers(servers_path)
    requests = load_requests(requests_path)
    events = load_events(run_path)
    capacity_changes = load_capacity_changes(metrics_path) if metrics_path else None
    valid, issues, summary = validate(
        servers, requests, events, max_queue_length, max_wait_ticks, capacity_changes
    )
    for issue in issues:
        print(f"{issue.level}: {issue.msg}")
    print("\nSUMMARY:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    if valid:
        print("\nRESULT: VALID (concurrent shared-CPU model)")
        return 0
    print("\nRESULT: INVALID")
    return 2


if __name__ == "__main__":
    sys.exit(main())
