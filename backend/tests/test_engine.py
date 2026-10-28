"""Scheduling, resource limits, determinism and validator compatibility."""

from __future__ import annotations

import random

import pytest

from app.simulation import STRATEGIES, AutoscalingConfig, Request, Server, serialize_trace, simulate
from support import (
    DATA_DIR,
    bundled_inputs,
    drops,
    lifecycle,
    random_scenario,
    schedule_violations,
    shared_validate,
    start_ticks,
    supplied_validate,
)


def test_default_run_reproduces_the_committed_example_trace():
    servers, requests = bundled_inputs()
    result = simulate(servers, requests)
    assert serialize_trace(result.events) == (DATA_DIR / "run.jsonl").read_bytes()


@pytest.mark.parametrize("strategy", sorted(STRATEGIES))
def test_one_request_per_server_passes_the_supplied_validator(strategy):
    servers, requests = bundled_inputs()
    result = simulate(servers, requests, scheduling_strategy=strategy)
    valid, issues, summary = supplied_validate(servers, requests, result.events)
    assert valid, [issue.msg for issue in issues]
    assert summary["finished"] == len(requests)


def test_shared_cpu_mode_is_valid_but_rejected_by_the_supplied_validator():
    servers, requests = bundled_inputs()
    result = simulate(servers, requests, allow_concurrency=True)
    valid, issues, _ = shared_validate(servers, requests, result.events)
    assert valid, [issue.msg for issue in issues]
    # The supplied validator forbids overlapping requests on one server.
    valid, issues, _ = supplied_validate(servers, requests, result.events)
    assert not valid
    assert any("overlapping" in issue.msg for issue in issues)


def test_cpu_is_split_evenly_between_running_requests():
    # Two requests share 10 CPU: r1 (10 work) gets 5 per tick and finishes at
    # t=2, then r2 gets the full 10 for its last 20 units and finishes at t=4.
    servers = [Server("s1", 10, 1000, 2)]
    requests = [Request(0, "r1", 10, 100), Request(0, "r2", 30, 100)]
    result = simulate(servers, requests, allow_concurrency=True)
    assert lifecycle(result) == {"r1": (0, 2, "s1"), "r2": (0, 4, "s1")}


def test_fractional_shares_complete_exactly():
    # 10 CPU split three ways is 10/3 per tick; 10 work units take exactly 3 ticks.
    servers = [Server("s1", 10, 1000, 3)]
    requests = [Request(0, f"r{i}", 10, 10) for i in range(3)]
    result = simulate(servers, requests, allow_concurrency=True)
    assert {finish for _, finish, _ in lifecycle(result).values()} == {3}


def test_rate_limit_caps_starts_per_server_per_tick():
    servers = [Server("s1", 10, 1000, 2)]
    requests = [Request(0, f"r{i}", 10, 10) for i in range(5)]
    result = simulate(servers, requests, allow_concurrency=True)
    assert start_ticks(result) == {"r0": 0, "r1": 0, "r2": 1, "r3": 1, "r4": 2}


def test_shared_mode_sums_reserved_memory_and_backfills_smaller_requests():
    servers = [Server("s1", 10, 1000, 5)]
    requests = [Request(0, "a", 50, 600), Request(0, "b", 10, 500), Request(0, "c", 10, 400)]
    result = simulate(servers, requests, allow_concurrency=True)
    # b (500 MB) does not fit beside a (600 MB), so the later c (400 MB) starts first.
    assert start_ticks(result) == {"a": 0, "c": 0, "b": 6}


def test_exclusive_mode_runs_one_request_per_server():
    servers = [Server("s1", 10, 1000, 5)]
    requests = [Request(0, "a", 20, 10), Request(0, "b", 20, 10)]
    result = simulate(servers, requests)
    assert lifecycle(result) == {"a": (0, 2, "s1"), "b": (2, 4, "s1")}


def test_requests_no_server_can_fit_are_dropped_on_arrival():
    # "big" has the memory but a zero start limit, so it never runs anything.
    servers = [Server("big", 10, 4096, 0), Server("small", 5, 512, 1)]
    requests = [Request(3, "too-big", 10, 1024), Request(3, "fits", 10, 100)]
    result = simulate(servers, requests)
    assert drops(result) == {"too-big": 3}
    assert lifecycle(result) == {"fits": (3, 5, "small")}
    assert result.summary["drop_reasons"]["no_capacity"] == 1
    valid, issues, _ = supplied_validate(servers, requests, result.events)
    assert valid, [issue.msg for issue in issues]


def test_ties_between_identical_servers_break_by_server_id():
    servers = [Server("b", 10, 1000, 5), Server("a", 10, 1000, 5), Server("c", 10, 1000, 5)]
    requests = [Request(0, f"r{i}", 10, 100) for i in range(3)]
    result = simulate(servers, requests)
    assert {rid: sid for rid, (_, _, sid) in lifecycle(result).items()} == {
        "r0": "a", "r1": "b", "r2": "c",
    }


def test_round_robin_rotates_in_server_id_order():
    servers = [Server("b", 10, 1000, 5), Server("a", 10, 1000, 5), Server("c", 10, 1000, 5)]
    requests = [Request(0, f"r{i}", 10, 100) for i in range(4)]
    result = simulate(servers, requests, allow_concurrency=True, scheduling_strategy="round_robin")
    assert {rid: sid for rid, (_, _, sid) in lifecycle(result).items()} == {
        "r0": "a", "r1": "b", "r2": "c", "r3": "a",
    }


@pytest.mark.parametrize("allow_concurrency", [False, True])
@pytest.mark.parametrize("strategy", sorted(STRATEGIES))
def test_input_order_does_not_change_the_output(allow_concurrency, strategy):
    servers, requests = random_scenario(random.Random(7), max_requests=120)
    autoscaling = AutoscalingConfig(enabled=True)
    first = simulate(servers, requests, allow_concurrency, strategy, autoscaling)
    shuffled_servers, shuffled_requests = servers[:], requests[:]
    random.Random(1).shuffle(shuffled_servers)
    random.Random(2).shuffle(shuffled_requests)
    second = simulate(shuffled_servers, shuffled_requests, allow_concurrency, strategy, autoscaling)
    assert serialize_trace(first.events) == serialize_trace(second.events)
    assert first.run_id == second.run_id
    assert first.metrics == second.metrics


@pytest.mark.parametrize("seed", range(30))
def test_random_workloads_respect_every_constraint(seed):
    servers, requests = random_scenario(random.Random(seed))
    for allow_concurrency in (False, True):
        for strategy in STRATEGIES:
            result = simulate(servers, requests, allow_concurrency, strategy)
            valid, issues, _ = shared_validate(servers, requests, result.events)
            assert valid, (allow_concurrency, strategy, [issue.msg for issue in issues][:3])
            if not allow_concurrency:
                valid, issues, _ = supplied_validate(servers, requests, result.events)
                assert valid, (strategy, [issue.msg for issue in issues][:3])
            problems = schedule_violations(servers, requests, result.events, allow_concurrency)
            assert not problems, (allow_concurrency, strategy, problems[:3])
            assert result.summary["finished"] + result.summary["dropped"] == len(requests)
