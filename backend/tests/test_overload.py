"""Overload controls, starvation and run time under heavy load."""

from __future__ import annotations

import random
import time

from app.simulation import MAX_SERIES_POINTS, QueuePolicy, Request, Server, simulate
from support import drops, shared_validate, start_ticks, supplied_validate

SERVERS = [Server("s1", 10, 1000, 1)]
# Five 3-tick requests arrive together on a server that runs one at a time.
BURST = [Request(0, f"r{i}", 30, 100) for i in range(5)]


def test_max_wait_drops_requests_that_cannot_start_in_time():
    result = simulate(SERVERS, BURST, queue_policy=QueuePolicy(max_wait_ticks=4))
    assert start_ticks(result) == {"r0": 0, "r1": 3}
    assert drops(result) == {"r2": 4, "r3": 4, "r4": 4}
    assert result.summary["drop_reasons"]["wait_timeout"] == 3
    valid, issues, _ = supplied_validate(SERVERS, BURST, result.events)
    assert valid, [issue.msg for issue in issues]
    valid, _, _ = shared_validate(SERVERS, BURST, result.events, max_wait_ticks=4)
    assert valid
    # Without the policy, these drops are unexplained.
    valid, _, _ = shared_validate(SERVERS, BURST, result.events)
    assert not valid


def test_queue_limit_drops_the_newest_arrivals():
    result = simulate(SERVERS, BURST, queue_policy=QueuePolicy(max_queue_length=2))
    assert start_ticks(result) == {"r0": 0, "r1": 3, "r2": 6}
    assert drops(result) == {"r3": 0, "r4": 0}
    assert result.summary["drop_reasons"]["queue_full"] == 2
    assert result.metrics["cluster"]["peak_queue_length"] == 2
    valid, issues, _ = supplied_validate(SERVERS, BURST, result.events)
    assert valid, [issue.msg for issue in issues]
    valid, _, _ = shared_validate(SERVERS, BURST, result.events, max_queue_length=2)
    assert valid


def test_drop_rate_counts_timeouts_without_arrivals():
    result = simulate(SERVERS, BURST, queue_policy=QueuePolicy(max_wait_ticks=4))
    sample = next(point for point in result.metrics["cluster_series"] if point["tick"] == 4)
    assert sample["requests_dropped"] == 3
    assert sample["error_rate_percent"] == 100.0


def starvation_workload() -> list[Request]:
    # A 1000 MB request arrives behind a steady stream of 400 MB requests.
    smalls = [Request(t, f"small-{t:03d}", 30, 400) for t in range(60)]
    return smalls + [Request(1, "big", 10, 1000)]


def test_shared_mode_can_starve_large_requests_without_the_guard():
    servers = [Server("s1", 10, 1000, 3)]
    result = simulate(servers, starvation_workload(), allow_concurrency=True)
    assert start_ticks(result)["big"] >= 150


def test_starvation_guard_bounds_the_wait_of_large_requests():
    servers = [Server("s1", 10, 1000, 3)]
    workload = starvation_workload()
    result = simulate(
        servers, workload, allow_concurrency=True,
        queue_policy=QueuePolicy(starvation_guard_ticks=5),
    )
    # The guard engages at t=6; the two running 400 MB requests drain within 6 ticks.
    assert start_ticks(result)["big"] <= 12
    valid, issues, _ = shared_validate(servers, workload, result.events)
    assert valid, [issue.msg for issue in issues]


def test_starvation_guard_is_ignored_with_one_request_per_server():
    servers = [Server("s1", 10, 1000, 3), Server("s2", 5, 500, 1)]
    workload = starvation_workload()
    plain = simulate(servers, workload)
    guarded = simulate(servers, workload, queue_policy=QueuePolicy(starvation_guard_ticks=0))
    assert plain.events == guarded.events
    assert plain.run_id == guarded.run_id


def test_sustained_overload_runs_quickly_with_bounded_metrics():
    rng = random.Random(5)
    servers = [Server("s1", 10, 1024, 2), Server("s2", 5, 512, 1)]
    requests = [
        Request(rng.randint(0, 2000), f"q{i:05d}", rng.randint(5, 60), rng.randint(32, 512))
        for i in range(5000)
    ]
    started = time.perf_counter()
    result = simulate(servers, requests)
    # The previous full-queue rescan took about 100 seconds here.
    assert time.perf_counter() - started < 20
    assert result.summary["finished"] == 5000
    assert len(result.metrics["cluster_series"]) <= MAX_SERIES_POINTS
    assert sum(point["requests_started"] for point in result.metrics["cluster_series"]) == 5000
    valid, issues, _ = supplied_validate(servers, requests, result.events)
    assert valid, [issue.msg for issue in issues][:3]


def test_long_requests_skip_ticks_without_changing_the_result():
    started = time.perf_counter()
    result = simulate([Server("s1", 1, 1024, 1)], [Request(0, "long", 200_000, 10)])
    assert time.perf_counter() - started < 5
    finish = next(e["t"] for e in result.events if e["event"] == "REQUEST_FINISHED")
    assert finish == 200_000
    assert result.metrics["sample_count"] == 200_001
    assert len(result.metrics["cluster_series"]) <= MAX_SERIES_POINTS
    assert result.metrics["per_server"]["s1"]["avg_cpu_utilization_percent"] > 99.99
