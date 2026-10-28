"""Helpers shared by the test modules."""

from __future__ import annotations

import json
import random
from fractions import Fraction
from pathlib import Path
from typing import Any

import validate_run
import validate_run_shared
from app.simulation import Request, Server, parse_requests_csv, parse_server_document

DATA_DIR = next(
    directory for directory in (Path(__file__).resolve().parents[1], Path(__file__).resolve().parents[2])
    if (directory / "validate_run.py").is_file()
)


def bundled_inputs() -> tuple[list[Server], list[Request]]:
    servers = parse_server_document(json.loads((DATA_DIR / "servers.json").read_text()))
    requests = parse_requests_csv((DATA_DIR / "requests.csv").read_text())
    return servers, requests


def supplied_validate(servers, requests, events):
    """Run the unchanged validator supplied with the assignment."""
    specs = {
        s.id: validate_run.ServerSpec(s.id, s.cpu_units_per_tick, s.mem_mb, s.rate_limit_per_sec)
        for s in servers
    }
    reqs = {
        r.request_id: validate_run.RequestSpec(r.request_id, r.t, r.work_units, r.mem_mb)
        for r in requests
    }
    return validate_run.validate(specs, reqs, events)


def shared_validate(servers, requests, events, **options):
    """Run the project's shared-CPU validator."""
    specs = {
        s.id: validate_run_shared.ServerSpec(s.id, s.cpu_units_per_tick, s.mem_mb, s.rate_limit_per_sec)
        for s in servers
    }
    reqs = {
        r.request_id: validate_run_shared.RequestSpec(r.request_id, r.t, r.work_units, r.mem_mb)
        for r in requests
    }
    return validate_run_shared.validate(specs, reqs, events, **options)


def capacity_changes(metrics: dict[str, Any]) -> dict[str, dict[int, int]]:
    changes: dict[str, dict[int, int]] = {}
    for decision in metrics["autoscaling"]["decisions"]:
        changes.setdefault(decision["server_id"], {})[decision["tick"]] = decision["cpu_after"]
    return changes


def lifecycle(result) -> dict[str, tuple[int, int, str]]:
    """Map each started request to (start tick, finish tick, server id)."""
    starts = {e["request_id"]: e for e in result.events if e["event"] == "REQUEST_STARTED"}
    finishes = {e["request_id"]: e["t"] for e in result.events if e["event"] == "REQUEST_FINISHED"}
    return {rid: (e["t"], finishes[rid], e["server_id"]) for rid, e in starts.items()}


def start_ticks(result) -> dict[str, int]:
    return {e["request_id"]: e["t"] for e in result.events if e["event"] == "REQUEST_STARTED"}


def drops(result) -> dict[str, int]:
    return {e["request_id"]: e["t"] for e in result.events if e["event"] == "REQUEST_DROPPED"}


def random_scenario(rng: random.Random, max_requests: int = 60) -> tuple[list[Server], list[Request]]:
    servers = [
        Server(f"s{i}", rng.randint(1, 20), rng.choice([64, 256, 512, 1024, 2048]), rng.randint(0, 3))
        for i in range(rng.randint(1, 4))
    ]
    if all(server.rate_limit_per_sec == 0 for server in servers):
        first = servers[0]
        servers[0] = Server(first.id, first.cpu_units_per_tick, first.mem_mb, 1)
    requests = [
        Request(
            rng.randint(0, 30) if rng.random() < 0.9 else rng.randint(30, 300),
            f"r{i}",
            rng.randint(1, 80) if rng.random() < 0.8 else rng.randint(80, 600),
            rng.randint(1, 2200),
        )
        for i in range(rng.randint(1, max_requests))
    ]
    return servers, requests


def schedule_violations(servers, requests, events, allow_concurrency: bool) -> list[str]:
    """Check a trace against the brief directly, independent of both validators.

    Replays every tick: memory and exclusivity limits hold, CPU is split evenly,
    requests finish at the first boundary where their work is done, and the
    scheduler is work-conserving (nothing waits while a server could start it).
    """
    errors: list[str] = []
    request_by_id = {r.request_id: r for r in requests}
    started = {e["request_id"]: (e["t"], e["server_id"]) for e in events if e["event"] == "REQUEST_STARTED"}
    finished = {e["request_id"]: e["t"] for e in events if e["event"] == "REQUEST_FINISHED"}
    if not started:
        return errors
    starts_per_tick: dict[tuple[str, int], int] = {}
    for start_t, sid in started.values():
        starts_per_tick[(sid, start_t)] = starts_per_tick.get((sid, start_t), 0) + 1

    runs_by_server: dict[str, list[tuple[str, int, int]]] = {server.id: [] for server in servers}
    for rid, (start_t, sid) in started.items():
        runs_by_server[sid].append((rid, start_t, finished[rid]))

    def active_on(sid: str, tick: int) -> list[str]:
        return [rid for rid, start_t, finish_t in runs_by_server[sid] if start_t <= tick < finish_t]

    remaining = {rid: Fraction(request_by_id[rid].work_units) for rid in started}
    first_tick = min(s for s, _ in started.values())
    last_tick = max(finished.values())
    for server in servers:
        for tick in range(first_tick, last_tick + 1):
            for rid, start_t, _ in runs_by_server[server.id]:
                if finished[rid] == tick and remaining[rid] > 0:
                    errors.append(f"{rid} finished at {tick} with work left")
                if start_t < tick < finished[rid] and remaining[rid] <= 0:
                    errors.append(f"{rid} should have finished by {tick}")
            active = active_on(server.id, tick)
            if not allow_concurrency and len(active) > 1:
                errors.append(f"{server.id} runs {len(active)} requests at {tick}")
            if sum(request_by_id[rid].mem_mb for rid in active) > server.mem_mb:
                errors.append(f"{server.id} exceeds memory at {tick}")
            for rid in active:
                remaining[rid] -= Fraction(server.cpu_units_per_tick, len(active))

    for tick in sorted({e["t"] for e in events}):
        waiting = [rid for rid, (s, _) in started.items() if request_by_id[rid].t <= tick < s]
        for rid in waiting:
            for server in servers:
                active = active_on(server.id, tick)
                if (
                    sum(request_by_id[x].mem_mb for x in active) + request_by_id[rid].mem_mb <= server.mem_mb
                    and starts_per_tick.get((server.id, tick), 0) < server.rate_limit_per_sec
                    and (allow_concurrency or not active)
                ):
                    errors.append(f"{rid} waited at {tick} although {server.id} could start it")
                    break
    return errors
