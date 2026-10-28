"""Deterministic, discrete-tick load balancer simulation with optional CPU sharing.

Every tick runs the same phases in order: finish completed requests, admit
arrivals, autoscale, start waiting requests, apply overload limits, allocate
CPU, and record metrics. When a tick changes nothing but remaining work, the
following ticks would repeat it exactly, so they are applied in one step. Run
time therefore grows with the number of events rather than the number of ticks.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from dataclasses import asdict, dataclass, field
from fractions import Fraction
from typing import Any, Iterable


class SimulationInputError(ValueError):
    """Raised when an input file or server configuration is invalid."""


# Server ids are used in API paths, so they are limited to URL-safe characters.
SERVER_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}")
# Metric series longer than this are averaged into equal buckets of ticks.
MAX_SERIES_POINTS = 1000
DROP_REASONS = ("no_capacity", "queue_full", "wait_timeout")


@dataclass(frozen=True, slots=True)
class Server:
    id: str
    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int


@dataclass(frozen=True, slots=True)
class Request:
    t: int
    request_id: str
    work_units: int
    mem_mb: int


@dataclass(slots=True)
class RunningRequest:
    request: Request
    remaining_work: Fraction


@dataclass(frozen=True, slots=True)
class SimulationResult:
    events: list[dict[str, Any]]
    summary: dict[str, Any]
    metrics: dict[str, Any]
    run_id: str


@dataclass(frozen=True, slots=True)
class AutoscalingConfig:
    """Opt-in CPU capacity controller evaluated once at each tick boundary."""

    enabled: bool = False
    scale_up_cpu_percent: int = 80
    scale_down_cpu_percent: int = 30
    queue_threshold: int = 1
    error_rate_threshold_percent: int = 1
    cooldown_ticks: int = 2
    min_cpu_percent: int = 50
    max_cpu_percent: int = 200

    def validate(self) -> None:
        if not self.enabled:
            return
        for key, value, minimum, maximum in (
            ("scale_up_cpu_percent", self.scale_up_cpu_percent, 1, 100),
            ("scale_down_cpu_percent", self.scale_down_cpu_percent, 0, 99),
            ("queue_threshold", self.queue_threshold, 1, 1_000_000),
            ("error_rate_threshold_percent", self.error_rate_threshold_percent, 1, 100),
            ("cooldown_ticks", self.cooldown_ticks, 1, 1_000_000),
            ("min_cpu_percent", self.min_cpu_percent, 1, 100),
            ("max_cpu_percent", self.max_cpu_percent, 100, 1_000),
        ):
            _integer(value, key, minimum)
            if value > maximum:
                raise SimulationInputError(f"{key} must be at most {maximum}")
        if self.scale_down_cpu_percent >= self.scale_up_cpu_percent:
            raise SimulationInputError(
                "scale_down_cpu_percent must be lower than scale_up_cpu_percent"
            )
        if self.min_cpu_percent > self.max_cpu_percent:
            raise SimulationInputError("min_cpu_percent cannot exceed max_cpu_percent")


@dataclass(frozen=True, slots=True)
class QueuePolicy:
    """Opt-in overload controls. ``None`` leaves a control off.

    ``max_queue_length`` caps how many requests may wait; after a tick's starts,
    the newest arrivals beyond the cap are dropped. ``max_wait_ticks`` drops a
    request that has not started that many ticks after it arrived.
    ``starvation_guard_ticks`` applies to shared-CPU mode only: once the oldest
    waiting request has waited that long, servers that could fit it stop
    starting younger requests until it starts.
    """

    max_queue_length: int | None = None
    max_wait_ticks: int | None = None
    starvation_guard_ticks: int | None = None

    def validate(self) -> None:
        for key, value in asdict(self).items():
            if value is not None:
                _integer(value, key, 0)

    def effective(self, allow_concurrency: bool) -> dict[str, int]:
        """Return the controls that can change a run's outcome."""
        values = {key: value for key, value in asdict(self).items() if value is not None}
        if not allow_concurrency:
            # With one request per server the oldest request is always offered
            # every free server first, so the guard can never hold one back.
            values.pop("starvation_guard_ticks", None)
        return values


def _integer(value: Any, field: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SimulationInputError(f"{field} must be an integer")
    if value < minimum:
        raise SimulationInputError(f"{field} must be at least {minimum}")
    return value


def parse_server(value: dict[str, Any]) -> Server:
    if not isinstance(value, dict):
        raise SimulationInputError("Each server must be a JSON object")
    sid = value.get("id")
    if not isinstance(sid, str) or not sid.strip():
        raise SimulationInputError("Server id must be a non-empty string")
    sid = sid.strip()
    if not SERVER_ID_PATTERN.fullmatch(sid):
        raise SimulationInputError(
            f"Server id '{sid}' must be 1-80 letters, digits, '.', '_' or '-', "
            "starting with a letter or digit"
        )
    return Server(
        id=sid,
        cpu_units_per_tick=_integer(value.get("cpu_units_per_tick"), "cpu_units_per_tick", 1),
        mem_mb=_integer(value.get("mem_mb"), "mem_mb", 1),
        rate_limit_per_sec=_integer(value.get("rate_limit_per_sec"), "rate_limit_per_sec", 0),
    )


def parse_server_document(document: Any) -> list[Server]:
    if not isinstance(document, dict):
        raise SimulationInputError("servers.json must contain a JSON object")
    tick_seconds = _integer(document.get("tick_seconds", 1), "tick_seconds", 1)
    if tick_seconds != 1:
        raise SimulationInputError("Only tick_seconds = 1 is supported")
    raw_servers = document.get("servers")
    if not isinstance(raw_servers, list):
        raise SimulationInputError("servers.json must contain a servers array")
    servers = [parse_server(item) for item in raw_servers]
    ids = [server.id for server in servers]
    if len(ids) != len(set(ids)):
        raise SimulationInputError("Server ids must be unique")
    return sorted(servers, key=lambda server: server.id)


def parse_requests_csv(source: str) -> list[Request]:
    try:
        reader = csv.DictReader(io.StringIO(source, newline=""))
        columns = set(reader.fieldnames or [])
        required = {"t", "request_id", "work_units", "mem_mb"}
        if not required.issubset(columns):
            raise SimulationInputError(
                "requests.csv must contain columns: " + ", ".join(sorted(required))
            )
        requests: list[Request] = []
        seen: set[str] = set()
        for line_number, row in enumerate(reader, start=2):
            request_id = (row.get("request_id") or "").strip()
            if not request_id:
                raise SimulationInputError(f"request_id is empty on CSV line {line_number}")
            if request_id in seen:
                raise SimulationInputError(f"Duplicate request_id in requests.csv: {request_id}")
            seen.add(request_id)
            try:
                arrival_t = int((row.get("t") or "").strip())
                work_units = int((row.get("work_units") or "").strip())
                mem_mb = int((row.get("mem_mb") or "").strip())
            except ValueError as exc:
                raise SimulationInputError(
                    f"t, work_units and mem_mb must be integers on CSV line {line_number}"
                ) from exc
            _integer(arrival_t, f"t on CSV line {line_number}", 0)
            _integer(work_units, f"work_units on CSV line {line_number}", 1)
            _integer(mem_mb, f"mem_mb on CSV line {line_number}", 1)
            requests.append(Request(arrival_t, request_id, work_units, mem_mb))
    except csv.Error as exc:
        raise SimulationInputError(f"Invalid CSV: {exc}") from exc

    if not requests:
        raise SimulationInputError("requests.csv contains no requests")
    return sorted(requests, key=lambda request: (request.t, request.request_id))


def ceil_fraction(value: Fraction) -> int:
    return (value.numerator + value.denominator - 1) // value.denominator


def _event(t: int, name: str, request_id: str, server_id: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"t": t, "event": name, "request_id": request_id}
    if server_id is not None:
        result["server_id"] = server_id
    return result


def _percentile(values: list[int], numerator: int, denominator: int) -> int | None:
    if not values:
        return None
    rank = (numerator * len(values) + denominator - 1) // denominator
    index = max(0, min(rank - 1, len(values) - 1))
    return values[index]


def _error_rate(arrived: int, dropped: int) -> float:
    """Dropped requests per arrival in a tick, as a percentage capped at 100.

    Drops in a tick without arrivals (wait timeouts) count as 100 percent.
    """
    if not dropped:
        return 0.0
    if not arrived:
        return 100.0
    return round(min(dropped / arrived * 100, 100.0), 4)


def canonical_run_id(
    servers: Iterable[Server],
    requests: Iterable[Request],
    allow_concurrency: bool = False,
    scheduling_strategy: str = "projected_drain",
    autoscaling: AutoscalingConfig | None = None,
    queue_policy: QueuePolicy | None = None,
) -> str:
    autoscaling = autoscaling or AutoscalingConfig()
    queue_policy = queue_policy or QueuePolicy()
    payload: dict[str, Any] = {
        "tick_seconds": 1,
        "scheduler_policy": (
            "shared-cpu-fifo-v2" if allow_concurrency else "exclusive-fifo-v1"
        ),
        "scheduling_strategy": scheduling_strategy,
        "servers": [asdict(server) for server in sorted(servers, key=lambda item: item.id)],
        "requests": [
            asdict(request)
            for request in sorted(requests, key=lambda item: (item.t, item.request_id))
        ],
    }
    # Settings that cannot change the outcome are left out, so equal runs share an id.
    if autoscaling.enabled:
        payload["autoscaling"] = asdict(autoscaling)
    effective_queue_policy = queue_policy.effective(allow_concurrency)
    if effective_queue_policy:
        payload["queue_policy"] = effective_queue_policy
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(slots=True, eq=False)
class ServerState:
    """One server's live state during a run. Placement strategies only read it."""

    spec: Server
    index: int
    cpu: int
    active: dict[str, RunningRequest] = field(default_factory=dict)
    reserved_mem: int = 0
    remaining_work: Fraction = field(default_factory=Fraction)
    starts_this_tick: int = 0

    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def free_mem(self) -> int:
        return self.spec.mem_mb - self.reserved_mem


class PlacementStrategy:
    """Chooses a server for a request among the servers that can start it now.

    ``candidates`` is never empty and is in server-id order. A new instance is
    created for every run, so a strategy may keep state between calls. To add
    a strategy, subclass this and register it in ``STRATEGIES``.
    """

    def __init__(self, servers: list[ServerState]) -> None:
        self.servers = servers

    def choose(self, request: Request, candidates: list[ServerState]) -> ServerState:
        raise NotImplementedError


class ProjectedDrain(PlacementStrategy):
    """Fewest ticks to drain the server's remaining work plus this request.

    Ties prefer fewer active requests, then the tighter memory fit, then the
    smaller server id.
    """

    def choose(self, request: Request, candidates: list[ServerState]) -> ServerState:
        return min(candidates, key=lambda server: (
            ceil_fraction((server.remaining_work + request.work_units) / server.cpu),
            len(server.active) + 1,
            server.free_mem - request.mem_mb,
            server.id,
        ))


class LeastLoaded(PlacementStrategy):
    """Least remaining work per CPU unit, with the same tie-breaks as projected drain."""

    def choose(self, request: Request, candidates: list[ServerState]) -> ServerState:
        return min(candidates, key=lambda server: (
            server.remaining_work / server.cpu,
            len(server.active),
            server.free_mem - request.mem_mb,
            server.id,
        ))


class RoundRobin(PlacementStrategy):
    """Next server in id order after the previous placement, skipping ineligible ones."""

    def __init__(self, servers: list[ServerState]) -> None:
        super().__init__(servers)
        self.cursor = 0

    def choose(self, request: Request, candidates: list[ServerState]) -> ServerState:
        count = len(self.servers)
        server = min(candidates, key=lambda item: ((item.index - self.cursor) % count, item.id))
        self.cursor = (server.index + 1) % count
        return server


STRATEGIES: dict[str, type[PlacementStrategy]] = {
    "projected_drain": ProjectedDrain,
    "least_loaded": LeastLoaded,
    "round_robin": RoundRobin,
}


class _WaitingQueue:
    """Waiting requests in FIFO order, searchable by memory demand.

    A position indexes the run's sorted request list, so position order is
    queue order. A min-tree of memory demand over positions finds the oldest
    waiting request that fits a memory budget without rescanning the queue.
    """

    def __init__(self, requests: list[Request]) -> None:
        size = 1
        while size < len(requests):
            size *= 2
        self._size = size
        self._tree: list[float] = [math.inf] * (2 * size)
        self._requests = requests
        self._waiting = [False] * len(requests)
        self._head = 0
        self._end = 0
        self.count = 0

    def add(self, position: int) -> None:
        self._waiting[position] = True
        self._end = position + 1
        self._set(position, self._requests[position].mem_mb)
        self.count += 1

    def remove(self, position: int) -> None:
        self._waiting[position] = False
        self._set(position, math.inf)
        self.count -= 1

    def is_waiting(self, position: int) -> bool:
        return self._waiting[position]

    def oldest(self) -> int | None:
        # Positions are added in increasing order, so the head only moves forward.
        while self._head < self._end and not self._waiting[self._head]:
            self._head += 1
        return self._head if self._head < self._end else None

    def first_fit(self, start: int, budget: int) -> int | None:
        """Return the oldest waiting position at or after ``start`` that fits ``budget``."""
        return self._descend(1, 0, self._size, start, budget)

    def _descend(self, node: int, low: int, high: int, start: int, budget: int) -> int | None:
        if high <= start or self._tree[node] > budget:
            return None
        if high - low == 1:
            return low
        middle = (low + high) // 2
        found = self._descend(2 * node, low, middle, start, budget)
        if found is None:
            found = self._descend(2 * node + 1, middle, high, start, budget)
        return found

    def _set(self, position: int, value: float) -> None:
        node = position + self._size
        self._tree[node] = value
        node //= 2
        while node:
            left, right = self._tree[2 * node], self._tree[2 * node + 1]
            self._tree[node] = left if left < right else right
            node //= 2


_CLUSTER_FIELDS = (
    "cpu_capacity", "cpu_used_units", "cpu_utilization_percent", "work_completed_units",
    "active_requests", "queued_requests", "memory_capacity_mb", "memory_reserved_mb",
    "memory_utilization_percent", "requests_arrived", "requests_started",
    "requests_finished", "requests_dropped", "error_rate_percent",
)
_SERVER_FIELDS = (
    "cpu_capacity", "cpu_used_units", "cpu_utilization_percent", "work_completed_units",
    "active_requests", "memory_capacity_mb", "memory_reserved_mb",
    "memory_utilization_percent", "requests_started", "requests_finished",
)
_COUNTER_FIELDS = frozenset(
    {"requests_arrived", "requests_started", "requests_finished", "requests_dropped"}
)


class _Series:
    """Samples of consecutive processed ticks, stored as runs of equal values."""

    def __init__(self, fields: tuple[str, ...]) -> None:
        self.fields = fields
        self.runs: list[list[Any]] = []  # [first tick, tick count, values]
        self.count = 0

    def add(self, tick: int, sample: dict[str, Any], repeat: int = 1) -> None:
        values = tuple(sample[name] for name in self.fields)
        last = self.runs[-1] if self.runs else None
        if last is not None and last[2] == values and last[0] + last[1] == tick:
            last[1] += repeat
        else:
            self.runs.append([tick, repeat, values])
        self.count += repeat

    def mean(self, name: str) -> float:
        index = self.fields.index(name)
        return round(math.fsum(values[index] * count for _, count, values in self.runs) / self.count, 4)

    def peak(self, name: str) -> Any:
        index = self.fields.index(name)
        return max(values[index] for _, _, values in self.runs)

    def total(self, name: str) -> Any:
        index = self.fields.index(name)
        return sum(values[index] * count for _, count, values in self.runs)

    def points(self, bucket: int) -> list[dict[str, Any]]:
        """Return one point per tick, or one per ``bucket`` ticks when downsampling.

        A bucket point averages gauges and sums counters over its ticks;
        ``tick`` and ``tick_end`` give the first and last tick it covers.
        """
        if bucket == 1:
            return [
                {"tick": first + offset, **dict(zip(self.fields, values))}
                for first, count, values in self.runs
                for offset in range(count)
            ]
        points: list[dict[str, Any]] = []
        first_tick = last_tick = 0
        filled = 0
        totals = [0.0] * len(self.fields)
        for first, count, values in self.runs:
            offset = 0
            while offset < count:
                if not filled:
                    first_tick = first + offset
                take = min(count - offset, bucket - filled)
                for index, value in enumerate(values):
                    totals[index] += value * take
                filled += take
                offset += take
                last_tick = first + offset - 1
                if filled == bucket:
                    points.append(self._bucket_point(first_tick, last_tick, filled, totals))
                    filled, totals = 0, [0.0] * len(self.fields)
        if filled:
            points.append(self._bucket_point(first_tick, last_tick, filled, totals))
        return points

    def _bucket_point(self, first: int, last: int, ticks: int, totals: list[float]) -> dict[str, Any]:
        point: dict[str, Any] = {"tick": first, "tick_end": last, "ticks": ticks}
        for name, total in zip(self.fields, totals):
            point[name] = int(total) if name in _COUNTER_FIELDS else round(total / ticks, 4)
        if "error_rate_percent" in point:
            point["error_rate_percent"] = _error_rate(
                point["requests_arrived"], point["requests_dropped"]
            )
        return point


class _Simulation:
    """State and tick phases of one run. Inputs arrive validated and sorted."""

    def __init__(
        self,
        servers: list[Server],
        requests: list[Request],
        allow_concurrency: bool,
        scheduling_strategy: str,
        autoscaling: AutoscalingConfig,
        queue_policy: QueuePolicy,
    ) -> None:
        self.requests = requests
        self.allow_concurrency = allow_concurrency
        self.scheduling_strategy = scheduling_strategy
        self.autoscaling = autoscaling
        self.queue_policy = queue_policy
        self.servers = [
            ServerState(server, index, server.cpu_units_per_tick)
            for index, server in enumerate(servers)
        ]
        self.strategy = STRATEGIES[scheduling_strategy](self.servers)
        self.queue = _WaitingQueue(requests)
        self.next_arrival = 0
        self.base_cpu = {server.id: server.cpu_units_per_tick for server in servers}
        self.last_scale_tick = {server.id: -autoscaling.cooldown_ticks for server in servers}
        self.memory_capacity = sum(server.mem_mb for server in servers)
        self.max_admissible_mem = max(
            (server.mem_mb for server in servers if server.rate_limit_per_sec > 0), default=0
        )
        self.events: list[dict[str, Any]] = []
        self.starts: dict[str, int] = {}
        self.finishes: dict[str, int] = {}
        self.drops: dict[str, str] = {}
        self.scale_decisions: list[dict[str, Any]] = []
        self.cluster_series = _Series(_CLUSTER_FIELDS)
        self.server_series = {server.id: _Series(_SERVER_FIELDS) for server in servers}
        self.last_cluster_sample: dict[str, Any] | None = None
        self.last_server_samples: dict[str, dict[str, Any]] = {}

    def run(self) -> None:
        now = 0
        while self.next_arrival < len(self.requests) or self.queue.count or self._busy():
            if not self.queue.count and not self._busy():
                now = self._next_tick_after_idle(now)
            if self._process_tick(now):
                now = self._fast_forward(now)
            now += 1

    def _busy(self) -> bool:
        return any(server.active for server in self.servers)

    def _process_tick(self, now: int) -> bool:
        """Run every phase of tick ``now``; return True if nothing arrived, started or dropped."""
        finished = self._finish_completed(now)
        arrived, dropped = self._admit_arrivals(now)
        self._autoscale(now)
        started = self._start_waiting(now)
        if self.queue.count and not self._busy():
            raise SimulationInputError("Scheduler could not make progress with waiting requests")
        dropped += self._expire_waiting(now)
        dropped += self._enforce_queue_limit(now, arrived)
        work = self._allocate_cpu()
        self._record_sample(now, arrived, started, finished, dropped, work)
        return not arrived and not started and not dropped

    def _next_tick_after_idle(self, now: int) -> int:
        """Jump an idle gap to the next arrival, stopping where the autoscaler may scale down."""
        next_arrival_t = self.requests[self.next_arrival].t
        if not (self.autoscaling.enabled and self.last_cluster_sample and next_arrival_t > now):
            return max(now, next_arrival_t)
        shrinkable = [server for server in self.servers if server.cpu > self._min_cpu(server)]
        if not shrinkable:
            return next_arrival_t
        next_control_tick = min(
            self.last_scale_tick[server.id] + self.autoscaling.cooldown_ticks
            for server in shrinkable
        )
        # If the controller is already due, process this tick before jumping.
        return min(next_control_tick, next_arrival_t) if next_control_tick > now else now

    def _finish_completed(self, now: int) -> dict[str, int]:
        """Finish requests whose work reached zero in the preceding tick, releasing memory."""
        finished: dict[str, int] = {}
        for server in self.servers:
            done = sorted(
                request_id for request_id, running in server.active.items()
                if running.remaining_work <= 0
            )
            for request_id in done:
                running = server.active.pop(request_id)
                server.reserved_mem -= running.request.mem_mb
                server.remaining_work -= running.remaining_work
                self.finishes[request_id] = now
                self.events.append(_event(now, "REQUEST_FINISHED", request_id, server.id))
            finished[server.id] = len(done)
        return finished

    def _admit_arrivals(self, now: int) -> tuple[int, int]:
        """Queue this tick's arrivals; drop those no positive-rate server could ever fit."""
        arrived = dropped = 0
        while self.next_arrival < len(self.requests) and self.requests[self.next_arrival].t == now:
            position = self.next_arrival
            request = self.requests[position]
            self.next_arrival += 1
            arrived += 1
            self.events.append(_event(now, "REQUEST_ARRIVED", request.request_id))
            if request.mem_mb <= self.max_admissible_mem:
                self.queue.add(position)
            else:
                self._drop(request, now, "no_capacity")
                dropped += 1
        return arrived, dropped

    def _autoscale(self, now: int) -> None:
        """Change at most one server's CPU, judged by the previous processed tick."""
        previous = self.last_cluster_sample
        if not self.autoscaling.enabled or previous is None:
            return
        ready = [
            server for server in self.servers
            if now - self.last_scale_tick[server.id] >= self.autoscaling.cooldown_ticks
        ]
        if self._under_pressure(previous):
            scalable = [server for server in ready if server.cpu < self._max_cpu(server)]
            if scalable:
                target = min(scalable, key=lambda server: (
                    -self.last_server_samples[server.id]["cpu_utilization_percent"],
                    -self.last_server_samples[server.id]["active_requests"],
                    server.id,
                ))
                after = min(self._max_cpu(target), target.cpu + self._scale_step(target))
                self._scale(target, now, "up", after, previous)
        elif self._under_low_load(previous):
            scalable = [server for server in ready if server.cpu > self._min_cpu(server)]
            if scalable:
                target = min(scalable, key=lambda server: (
                    -server.cpu / self.base_cpu[server.id],
                    self.last_server_samples[server.id]["cpu_utilization_percent"],
                    server.id,
                ))
                after = max(self._min_cpu(target), target.cpu - self._scale_step(target))
                self._scale(target, now, "down", after, previous)

    def _under_pressure(self, sample: dict[str, Any]) -> bool:
        return (
            sample["cpu_utilization_percent"] >= self.autoscaling.scale_up_cpu_percent
            or sample["queued_requests"] >= self.autoscaling.queue_threshold
            or sample["error_rate_percent"] >= self.autoscaling.error_rate_threshold_percent
        )

    def _under_low_load(self, sample: dict[str, Any]) -> bool:
        return (
            sample["cpu_utilization_percent"] <= self.autoscaling.scale_down_cpu_percent
            and sample["queued_requests"] == 0
            and sample["error_rate_percent"] == 0
        )

    def _min_cpu(self, server: ServerState) -> int:
        base = self.base_cpu[server.id]
        return min(base, max(1, (base * self.autoscaling.min_cpu_percent + 99) // 100))

    def _max_cpu(self, server: ServerState) -> int:
        base = self.base_cpu[server.id]
        return max(base, (base * self.autoscaling.max_cpu_percent + 99) // 100)

    def _scale_step(self, server: ServerState) -> int:
        return max(1, (self.base_cpu[server.id] + 3) // 4)

    def _scale(
        self, server: ServerState, now: int, direction: str, after: int, signals: dict[str, Any]
    ) -> None:
        before = server.cpu
        if after == before:
            return
        server.cpu = after
        self.last_scale_tick[server.id] = now
        self.scale_decisions.append({
            "tick": now,
            "server_id": server.id,
            "direction": direction,
            "cpu_before": before,
            "cpu_after": after,
            "signals": {
                "cpu_utilization_percent": signals["cpu_utilization_percent"],
                "queued_requests": signals["queued_requests"],
                "error_rate_percent": signals["error_rate_percent"],
            },
        })

    def _start_waiting(self, now: int) -> int:
        """Start waiting requests in FIFO order, letting later ones use capacity earlier ones cannot.

        Each step starts the oldest waiting request that fits the largest free
        memory among servers that can still start a request this tick, which
        is the request a front-to-back scan of the queue would pick next.
        """
        for server in self.servers:
            server.starts_this_tick = 0
        candidates = [server for server in self.servers if self._accepts_start(server)]
        guarded = self._starving_request(now)
        position = 0
        started = 0
        while candidates and self.queue.count:
            found = self.queue.first_fit(position, max(server.free_mem for server in candidates))
            if found is None:
                break
            if guarded is not None:
                if found != guarded:
                    # The starving request cannot start yet. Hold back every server
                    # that could fit it, so their memory drains until it can.
                    need = self.requests[guarded].mem_mb
                    candidates = [server for server in candidates if server.spec.mem_mb < need]
                    position = guarded + 1
                    guarded = None
                    continue
                guarded = None
            request = self.requests[found]
            eligible = [server for server in candidates if server.free_mem >= request.mem_mb]
            server = self.strategy.choose(request, eligible)
            self._start(request, found, server, now)
            started += 1
            if not self._accepts_start(server):
                candidates.remove(server)
            position = found + 1
        return started

    def _accepts_start(self, server: ServerState) -> bool:
        return server.starts_this_tick < server.spec.rate_limit_per_sec and (
            self.allow_concurrency or not server.active
        )

    def _starving_request(self, now: int) -> int | None:
        guard = self.queue_policy.starvation_guard_ticks
        if guard is None or not self.allow_concurrency:
            return None
        oldest = self.queue.oldest()
        if oldest is None or now - self.requests[oldest].t < guard:
            return None
        return oldest

    def _start(self, request: Request, position: int, server: ServerState, now: int) -> None:
        self.queue.remove(position)
        server.active[request.request_id] = RunningRequest(request, Fraction(request.work_units))
        server.reserved_mem += request.mem_mb
        server.remaining_work += request.work_units
        server.starts_this_tick += 1
        self.starts[request.request_id] = now
        self.events.append(_event(now, "REQUEST_STARTED", request.request_id, server.id))

    def _expire_waiting(self, now: int) -> int:
        """Drop requests that have waited ``max_wait_ticks`` without starting."""
        limit = self.queue_policy.max_wait_ticks
        if limit is None:
            return 0
        dropped = 0
        while (oldest := self.queue.oldest()) is not None and now - self.requests[oldest].t >= limit:
            self.queue.remove(oldest)
            self._drop(self.requests[oldest], now, "wait_timeout")
            dropped += 1
        return dropped

    def _enforce_queue_limit(self, now: int, arrived: int) -> int:
        """Drop this tick's newest arrivals while more than ``max_queue_length`` requests wait."""
        limit = self.queue_policy.max_queue_length
        if limit is None:
            return 0
        dropped = 0
        position = self.next_arrival - 1
        first_arrival = self.next_arrival - arrived
        while self.queue.count > limit and position >= first_arrival:
            if self.queue.is_waiting(position):
                self.queue.remove(position)
                self._drop(self.requests[position], now, "queue_full")
                dropped += 1
            position -= 1
        return dropped

    def _drop(self, request: Request, now: int, reason: str) -> None:
        self.drops[request.request_id] = reason
        self.events.append(_event(now, "REQUEST_DROPPED", request.request_id))

    def _allocate_cpu(self) -> dict[str, Fraction]:
        """Split each server's CPU evenly across its active requests for this tick."""
        work: dict[str, Fraction] = {}
        for server in self.servers:
            completed = Fraction(0)
            if server.active:
                share = Fraction(server.cpu, len(server.active))
                for running in server.active.values():
                    completed += min(running.remaining_work, share)
                    running.remaining_work -= share
                server.remaining_work -= server.cpu
            work[server.id] = completed
        return work

    def _record_sample(
        self,
        now: int,
        arrived: int,
        started: int,
        finished: dict[str, int],
        dropped: int,
        work: dict[str, Fraction],
    ) -> None:
        total_used = Fraction(0)
        reserved_total = 0
        for server in self.servers:
            used = work[server.id]
            total_used += used
            reserved_total += server.reserved_mem
            sample = {
                "cpu_capacity": server.cpu,
                "cpu_used_units": float(used),
                "cpu_utilization_percent": round(float(used / server.cpu * 100), 4),
                "work_completed_units": float(used),
                "active_requests": len(server.active),
                "memory_capacity_mb": server.spec.mem_mb,
                "memory_reserved_mb": server.reserved_mem,
                "memory_utilization_percent": round(
                    server.reserved_mem / server.spec.mem_mb * 100, 4
                ),
                "requests_started": server.starts_this_tick,
                "requests_finished": finished[server.id],
            }
            self.server_series[server.id].add(now, sample)
            self.last_server_samples[server.id] = sample
        capacity = sum(server.cpu for server in self.servers)
        cluster = {
            "cpu_capacity": capacity,
            "cpu_used_units": float(total_used),
            "cpu_utilization_percent": round(float(total_used / capacity * 100), 4),
            "work_completed_units": float(total_used),
            "active_requests": sum(len(server.active) for server in self.servers),
            "queued_requests": self.queue.count,
            "memory_capacity_mb": self.memory_capacity,
            "memory_reserved_mb": reserved_total,
            "memory_utilization_percent": round(reserved_total / self.memory_capacity * 100, 4),
            "requests_arrived": arrived,
            "requests_started": started,
            "requests_finished": sum(finished.values()),
            "requests_dropped": dropped,
            "error_rate_percent": _error_rate(arrived, dropped),
        }
        self.cluster_series.add(now, cluster)
        self.last_cluster_sample = cluster

    def _fast_forward(self, now: int) -> int:
        """Apply the ticks after ``now`` that would repeat it exactly; return the last one.

        After a tick with no arrival, start or drop, nothing can start until a
        request finishes, so later ticks only consume work until the next
        completion, arrival, wait timeout or possible autoscaler action.
        """
        horizon: float = math.inf
        for server in self.servers:
            if server.active:
                share = Fraction(server.cpu, len(server.active))
                for running in server.active.values():
                    if running.remaining_work <= 0:
                        return now
                    # Ticks in which this request still uses its full share.
                    horizon = min(horizon, ceil_fraction(running.remaining_work / share) - 1)
        if horizon == math.inf:
            return now
        if self.next_arrival < len(self.requests):
            horizon = min(horizon, self.requests[self.next_arrival].t - now - 1)
        oldest = self.queue.oldest()
        if self.queue_policy.max_wait_ticks is not None and oldest is not None:
            deadline = self.requests[oldest].t + self.queue_policy.max_wait_ticks
            horizon = min(horizon, deadline - now - 1)
        horizon = min(horizon, self._next_scaling_tick(now) - now - 1)
        if horizon < 1:
            return now

        skip = int(horizon)
        for server in self.servers:
            if server.active:
                share = Fraction(server.cpu, len(server.active))
                for running in server.active.values():
                    running.remaining_work -= share * skip
                server.remaining_work -= server.cpu * skip
        # Skipped ticks have the same gauges as tick ``now`` and no lifecycle events.
        assert self.last_cluster_sample is not None
        self.last_cluster_sample = {**self.last_cluster_sample, "requests_finished": 0}
        self.cluster_series.add(now + 1, self.last_cluster_sample, repeat=skip)
        for server in self.servers:
            sample = {**self.last_server_samples[server.id], "requests_finished": 0}
            self.server_series[server.id].add(now + 1, sample, repeat=skip)
            self.last_server_samples[server.id] = sample
        return now + skip

    def _next_scaling_tick(self, now: int) -> float:
        """First tick after ``now`` at which the autoscaler could act on unchanged metrics."""
        if not self.autoscaling.enabled or self.last_cluster_sample is None:
            return math.inf
        if self._under_pressure(self.last_cluster_sample):
            scalable = [server for server in self.servers if server.cpu < self._max_cpu(server)]
        elif self._under_low_load(self.last_cluster_sample):
            scalable = [server for server in self.servers if server.cpu > self._min_cpu(server)]
        else:
            return math.inf
        if not scalable:
            return math.inf
        return max(now + 1, min(
            self.last_scale_tick[server.id] + self.autoscaling.cooldown_ticks
            for server in scalable
        ))

    def summary(self) -> dict[str, Any]:
        wait_values = sorted(
            self.starts[request.request_id] - request.t
            for request in self.requests
            if request.request_id in self.starts
        )
        drop_reasons = dict.fromkeys(DROP_REASONS, 0)
        for reason in self.drops.values():
            drop_reasons[reason] += 1
        return {
            "total_requests": len(self.requests),
            "started": len(self.starts),
            "finished": len(self.finishes),
            "dropped": len(self.drops),
            "drop_reasons": drop_reasons,
            "execution_mode": (
                "shared_cpu" if self.allow_concurrency else "single_request_per_server"
            ),
            "scheduling_strategy": self.scheduling_strategy,
            "autoscaling_enabled": self.autoscaling.enabled,
            "queue_policy": self.queue_policy.effective(self.allow_concurrency),
            "avg_wait_ticks": (sum(wait_values) / len(wait_values)) if wait_values else None,
            "p50_wait_ticks": _percentile(wait_values, 50, 100),
            "p95_wait_ticks": _percentile(wait_values, 95, 100),
            "max_wait_ticks": max(wait_values) if wait_values else None,
        }

    def metrics(self) -> dict[str, Any]:
        cluster = self.cluster_series
        bucket = max(1, math.ceil(cluster.count / MAX_SERIES_POINTS))
        per_server = {
            server_id: {
                "ticks_sampled": series.count,
                "avg_cpu_utilization_percent": series.mean("cpu_utilization_percent"),
                "peak_cpu_utilization_percent": series.peak("cpu_utilization_percent"),
                "avg_memory_utilization_percent": series.mean("memory_utilization_percent"),
                "peak_memory_utilization_percent": series.peak("memory_utilization_percent"),
                "peak_active_requests": series.peak("active_requests"),
                "requests_started": series.total("requests_started"),
            }
            for server_id, series in self.server_series.items()
        }
        work_completed = sum(
            request.work_units for request in self.requests if request.request_id in self.starts
        )
        return {
            "sample_count": cluster.count,
            "series_ticks_per_point": bucket,
            "cluster": {
                "avg_cpu_utilization_percent": cluster.mean("cpu_utilization_percent"),
                "peak_cpu_utilization_percent": cluster.peak("cpu_utilization_percent"),
                "avg_memory_utilization_percent": cluster.mean("memory_utilization_percent"),
                "peak_memory_utilization_percent": cluster.peak("memory_utilization_percent"),
                "peak_queue_length": cluster.peak("queued_requests"),
                "avg_queue_length": cluster.mean("queued_requests"),
                "peak_active_requests": cluster.peak("active_requests"),
                "avg_error_rate_percent": cluster.mean("error_rate_percent"),
                "peak_error_rate_percent": cluster.peak("error_rate_percent"),
                "total_work_completed_units": float(work_completed),
            },
            "per_server": per_server,
            "cluster_series": cluster.points(bucket),
            "server_series": {
                server_id: series.points(bucket)
                for server_id, series in self.server_series.items()
            },
            "autoscaling": {
                "enabled": self.autoscaling.enabled,
                "config": asdict(self.autoscaling),
                "decisions": self.scale_decisions,
                "final_cpu_capacity": {server.id: server.cpu for server in self.servers},
            },
        }


def simulate(
    servers: list[Server],
    requests: list[Request],
    allow_concurrency: bool = False,
    scheduling_strategy: str = "projected_drain",
    autoscaling: AutoscalingConfig | None = None,
    queue_policy: QueuePolicy | None = None,
) -> SimulationResult:
    """Run a deterministic simulation and return its trace, summary, and metrics.

    By default each server runs one request at a time, the model the supplied
    validator checks. ``allow_concurrency`` lets requests share a server's CPU
    evenly. Autoscaling and the queue policy are opt-in. Fraction arithmetic
    keeps CPU shares and completion ticks exact.
    """
    if scheduling_strategy not in STRATEGIES:
        raise SimulationInputError(
            "scheduling_strategy must be one of: " + ", ".join(sorted(STRATEGIES))
        )
    autoscaling = autoscaling or AutoscalingConfig()
    autoscaling.validate()
    queue_policy = queue_policy or QueuePolicy()
    queue_policy.validate()
    ordered_servers = sorted(servers, key=lambda server: server.id)
    ordered_requests = sorted(requests, key=lambda request: (request.t, request.request_id))
    if not ordered_servers:
        raise SimulationInputError("At least one server is required to run a simulation")
    if not ordered_requests:
        raise SimulationInputError("At least one request is required to run a simulation")

    server_ids = [server.id for server in ordered_servers]
    if len(server_ids) != len(set(server_ids)):
        raise SimulationInputError("Server ids must be unique")
    request_ids = [request.request_id for request in ordered_requests]
    if len(request_ids) != len(set(request_ids)):
        raise SimulationInputError("Request ids must be unique")

    simulation = _Simulation(
        ordered_servers, ordered_requests, allow_concurrency,
        scheduling_strategy, autoscaling, queue_policy,
    )
    simulation.run()
    # Match the validator's primary ordering (tick, event type), then stabilize ties.
    simulation.events.sort(
        key=lambda item: (
            item["t"],
            item["event"],
            item["request_id"],
            item.get("server_id", ""),
        )
    )
    return SimulationResult(
        simulation.events,
        simulation.summary(),
        simulation.metrics(),
        canonical_run_id(
            ordered_servers, ordered_requests, allow_concurrency,
            scheduling_strategy, autoscaling, queue_policy,
        ),
    )


def serialize_trace(events: Iterable[dict[str, Any]]) -> bytes:
    lines = [json.dumps(event, separators=(",", ":"), ensure_ascii=False) for event in events]
    return ("\n".join(lines) + "\n").encode("utf-8")
