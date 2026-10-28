"""The opt-in CPU autoscaler."""

from __future__ import annotations

import random

import pytest

from app.simulation import STRATEGIES, AutoscalingConfig, Request, Server, simulate
from support import capacity_changes, random_scenario, shared_validate


@pytest.mark.parametrize("seed", range(25))
def test_autoscaler_respects_bounds_cooldown_and_one_change_per_tick(seed):
    rng = random.Random(seed)
    servers, requests = random_scenario(rng)
    config = AutoscalingConfig(
        enabled=True,
        cooldown_ticks=rng.randint(1, 4),
        min_cpu_percent=rng.randint(1, 100),
        max_cpu_percent=rng.randint(100, 400),
    )
    for allow_concurrency in (False, True):
        result = simulate(servers, requests, allow_concurrency, rng.choice(sorted(STRATEGIES)), config)
        base = {server.id: server.cpu_units_per_tick for server in servers}
        last_change: dict[str, int] = {}
        ticks_seen: set[int] = set()
        for decision in result.metrics["autoscaling"]["decisions"]:
            sid, tick = decision["server_id"], decision["tick"]
            low = min(base[sid], max(1, (base[sid] * config.min_cpu_percent + 99) // 100))
            high = max(base[sid], (base[sid] * config.max_cpu_percent + 99) // 100)
            assert low <= decision["cpu_after"] <= high
            assert tick - last_change.get(sid, -config.cooldown_ticks) >= config.cooldown_ticks
            assert tick not in ticks_seen
            ticks_seen.add(tick)
            last_change[sid] = tick
        assert result.summary["finished"] + result.summary["dropped"] == len(requests)
        # Replaying the recorded CPU changes reproduces every completion tick.
        valid, issues, _ = shared_validate(
            servers, requests, result.events, capacity_changes=capacity_changes(result.metrics)
        )
        assert valid, [issue.msg for issue in issues][:3]


def test_autoscaler_scales_up_under_queue_pressure_then_down_when_idle():
    servers = [Server("s1", 4, 1000, 1)]
    requests = [Request(0, f"r{i}", 40, 10) for i in range(4)] + [Request(200, "late", 4, 10)]
    result = simulate(servers, requests, autoscaling=AutoscalingConfig(enabled=True))
    directions = [decision["direction"] for decision in result.metrics["autoscaling"]["decisions"]]
    assert directions[0] == "up"
    assert "down" in directions
    cpu_after = [decision["cpu_after"] for decision in result.metrics["autoscaling"]["decisions"]]
    assert max(cpu_after) == 8  # 200 percent of the 4-unit baseline
