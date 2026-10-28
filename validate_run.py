#!/usr/bin/env python3
"""
validate_run.py

Usage:
  python validate_run.py --servers servers.json --requests requests.csv --run run.jsonl

Exit codes:
  0 = valid
  2 = invalid
"""

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class ServerSpec:
    id: str
    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int  # treated as max starts per tick (tick_seconds=1)


@dataclass(frozen=True)
class RequestSpec:
    id: str
    arrival_t: int
    work_units: int
    mem_mb: int


@dataclass
class ValidationIssue:
    level: str  # "ERROR" or "WARN"
    msg: str


def ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b


def load_servers(path: Path) -> Tuple[int, Dict[str, ServerSpec]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    tick_seconds = int(data.get("tick_seconds", 1))
    servers_raw = data.get("servers", [])
    servers: Dict[str, ServerSpec] = {}

    for s in servers_raw:
        sid = s["id"]
        if sid in servers:
            raise ValueError(f"Duplicate server id in servers.json: {sid}")
        servers[sid] = ServerSpec(
            id=sid,
            cpu_units_per_tick=int(s["cpu_units_per_tick"]),
            mem_mb=int(s["mem_mb"]),
            rate_limit_per_sec=int(s["rate_limit_per_sec"]),
        )

    if not servers:
        raise ValueError("servers.json contains no servers")
    if tick_seconds != 1:
        # You *can* support different tick sizes, but then rate limiting semantics get ambiguous.
        # Keep it strict unless you really need it.
        raise ValueError("This validator currently requires tick_seconds = 1")

    return tick_seconds, servers


def load_requests(path: Path) -> Dict[str, RequestSpec]:
    reqs: Dict[str, RequestSpec] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_cols = {"t", "request_id", "work_units", "mem_mb"}
        if not required_cols.issubset(reader.fieldnames or set()):
            raise ValueError(f"requests.csv must contain columns: {sorted(required_cols)}")

        for row in reader:
            rid = row["request_id"].strip()
            if rid in reqs:
                raise ValueError(f"Duplicate request_id in requests.csv: {rid}")
            reqs[rid] = RequestSpec(
                id=rid,
                arrival_t=int(row["t"]),
                work_units=int(row["work_units"]),
                mem_mb=int(row["mem_mb"]),
            )

    if not reqs:
        raise ValueError("requests.csv contains no requests")
    return reqs


def parse_run_jsonl(path: Path) -> List[dict]:
    events: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {i}: {e}") from e
            if not isinstance(ev, dict):
                raise ValueError(f"Event on line {i} is not a JSON object")
            events.append(ev)
    if not events:
        raise ValueError("run.jsonl is empty")
    return events


def validate(servers: Dict[str, ServerSpec], requests: Dict[str, RequestSpec], events: List[dict]) -> Tuple[bool, List[ValidationIssue], dict]:
    issues: List[ValidationIssue] = []

    # Basic schema checks + sorting
    for ev in events:
        if "t" not in ev or "event" not in ev:
            issues.append(ValidationIssue("ERROR", f"Missing required fields in event: {ev}"))
            continue
        if not isinstance(ev["t"], int):
            issues.append(ValidationIssue("ERROR", f"Event t must be int: {ev}"))
        if not isinstance(ev["event"], str):
            issues.append(ValidationIssue("ERROR", f"Event event must be str: {ev}"))

    events_sorted = sorted(events, key=lambda e: (e.get("t", 0), e.get("event", "")))

    # Track per-request lifecycle
    seen_arrival: Dict[str, int] = {}
    started: Dict[str, Tuple[int, str]] = {}   # request_id -> (t_start, server_id)
    finished: Dict[str, Tuple[int, str]] = {}  # request_id -> (t_finish, server_id)
    dropped: Dict[str, int] = {}

    # Rate limiting: starts per server per tick
    starts_per_server_tick: Dict[Tuple[str, int], int] = {}

    # Single-run per server constraint: ensure no overlap in [start, finish)
    server_busy_intervals: Dict[str, List[Tuple[int, int, str]]] = {sid: [] for sid in servers.keys()}

    def add_error(msg: str) -> None:
        issues.append(ValidationIssue("ERROR", msg))

    def add_warn(msg: str) -> None:
        issues.append(ValidationIssue("WARN", msg))

    # Pass 1: read events and record lifecycle markers
    for ev in events_sorted:
        t = ev.get("t")
        et = ev.get("event")
        rid = ev.get("request_id")

        if et in ("REQUEST_ARRIVED", "REQUEST_STARTED", "REQUEST_FINISHED", "REQUEST_DROPPED"):
            if not isinstance(rid, str) or not rid:
                add_error(f"{et} missing/invalid request_id: {ev}")
                continue
            if rid not in requests:
                add_error(f"{et} references unknown request_id '{rid}' not in requests.csv")
                continue

        if et == "REQUEST_ARRIVED":
            if rid in seen_arrival:
                add_error(f"Duplicate REQUEST_ARRIVED for request '{rid}'")
                continue
            expected_t = requests[rid].arrival_t
            if t != expected_t:
                add_error(f"REQUEST_ARRIVED.t mismatch for '{rid}': got {t}, expected {expected_t}")
            seen_arrival[rid] = t

        elif et == "REQUEST_STARTED":
            sid = ev.get("server_id")
            if not isinstance(sid, str) or sid not in servers:
                add_error(f"REQUEST_STARTED invalid server_id '{sid}' for request '{rid}'")
                continue
            if rid not in seen_arrival:
                add_error(f"REQUEST_STARTED before REQUEST_ARRIVED for '{rid}'")
                continue
            if rid in started:
                add_error(f"Duplicate REQUEST_STARTED for '{rid}'")
                continue
            if rid in dropped:
                add_error(f"REQUEST_STARTED after REQUEST_DROPPED for '{rid}'")
                continue

            # Rate limit per server per tick
            key = (sid, t)
            starts_per_server_tick[key] = starts_per_server_tick.get(key, 0) + 1

            # Memory feasibility (simple model: request mem must fit server mem)
            req = requests[rid]
            srv = servers[sid]
            if req.mem_mb > srv.mem_mb:
                add_error(f"Request '{rid}' mem_mb={req.mem_mb} exceeds server '{sid}' mem_mb={srv.mem_mb}")

            started[rid] = (t, sid)

        elif et == "REQUEST_FINISHED":
            sid = ev.get("server_id")
            if not isinstance(sid, str) or sid not in servers:
                add_error(f"REQUEST_FINISHED invalid server_id '{sid}' for request '{rid}'")
                continue
            if rid not in started:
                add_error(f"REQUEST_FINISHED before REQUEST_STARTED for '{rid}'")
                continue
            if rid in finished:
                add_error(f"Duplicate REQUEST_FINISHED for '{rid}'")
                continue
            t_start, sid_start = started[rid]
            if sid != sid_start:
                add_error(f"REQUEST_FINISHED server mismatch for '{rid}': started on {sid_start}, finished on {sid}")
            finished[rid] = (t, sid)

        elif et == "REQUEST_DROPPED":
            if rid in dropped:
                add_error(f"Duplicate REQUEST_DROPPED for '{rid}'")
                continue
            if rid in started:
                add_error(f"REQUEST_DROPPED after REQUEST_STARTED for '{rid}'")
                continue
            dropped[rid] = t

        else:
            # Unknown events are allowed but ignored (forward-compatible)
            pass

    # Ensure all requests arrived exactly once
    for rid in requests.keys():
        if rid not in seen_arrival:
            add_error(f"Missing REQUEST_ARRIVED for request '{rid}'")

    # Ensure every started request finished unless explicitly dropped (drop after start is invalid already)
    for rid, (t_start, sid) in started.items():
        if rid not in finished:
            add_error(f"Request '{rid}' started but never finished")

    # Rate limit enforcement
    for (sid, t), count in starts_per_server_tick.items():
        limit = servers[sid].rate_limit_per_sec
        if count > limit:
            add_error(f"Rate limit exceeded on server '{sid}' at tick {t}: starts={count} > limit={limit}")

    for rid, (t_start, sid) in started.items():
        req = requests[rid]
        srv = servers[sid]
        runtime = ceil_div(req.work_units, srv.cpu_units_per_tick)
        expected_finish = t_start + runtime
        t_finish, _ = finished[rid]

        if t_finish != expected_finish:
            add_error(
                f"Finish time mismatch for '{rid}' on '{sid}': "
                f"got t_finish={t_finish}, expected {expected_finish} "
                f"(work_units={req.work_units}, cpu_units_per_tick={srv.cpu_units_per_tick})"
            )

        server_busy_intervals[sid].append((t_start, t_finish, rid))

    # Overlap check per server
    for sid, intervals in server_busy_intervals.items():
        intervals.sort(key=lambda x: x[0])
        for i in range(1, len(intervals)):
            prev_start, prev_end, prev_rid = intervals[i - 1]
            cur_start, cur_end, cur_rid = intervals[i]
            # No overlap allowed in [start, end)
            if cur_start < prev_end:
                add_error(
                    f"Server '{sid}' has overlapping runs: "
                    f"'{prev_rid}' [{prev_start},{prev_end}) and '{cur_rid}' [{cur_start},{cur_end})"
                )

    total = len(requests)
    started_n = len(started)
    finished_n = len(finished)
    dropped_n = len(dropped)

    waits: List[int] = []
    for rid, (t_start, _sid) in started.items():
        waits.append(t_start - requests[rid].arrival_t)
    waits.sort()

    def pctl(vals: List[int], p: float) -> Optional[int]:
        if not vals:
            return None
        idx = int(math.ceil(p * len(vals))) - 1
        idx = max(0, min(idx, len(vals) - 1))
        return vals[idx]

    summary = {
        "total_requests": total,
        "started": started_n,
        "finished": finished_n,
        "dropped": dropped_n,
        "avg_wait_ticks": (sum(waits) / len(waits)) if waits else None,
        "p50_wait_ticks": pctl(waits, 0.50),
        "p95_wait_ticks": pctl(waits, 0.95),
        "max_wait_ticks": max(waits) if waits else None,
    }

    ok = not any(i.level == "ERROR" for i in issues)
    return ok, issues, summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--servers", required=True, help="Path to servers.json")
    ap.add_argument("--requests", required=True, help="Path to requests.csv")
    ap.add_argument("--run", required=True, help="Path to run.jsonl")
    args = ap.parse_args()

    tick_seconds, servers = load_servers(Path(args.servers))
    requests = load_requests(Path(args.requests))
    events = parse_run_jsonl(Path(args.run))

    ok, issues, summary = validate(servers, requests, events)

    # Print issues
    for it in issues:
        print(f"{it.level}: {it.msg}")

    # Print summary
    print("\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    if ok:
        print("\nRESULT: VALID ✅")
        return 0
    else:
        print("\nRESULT: INVALID ❌")
        return 2


if __name__ == "__main__":
    sys.exit(main())