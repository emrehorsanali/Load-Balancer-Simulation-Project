"""HTTP API for server management and simulation runs."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from .simulation import (
    SERVER_ID_PATTERN,
    STRATEGIES,
    AutoscalingConfig,
    QueuePolicy,
    SimulationInputError,
    parse_requests_csv,
    serialize_trace,
    simulate,
)
from .storage import RunStore, ServerStore


STATE_DIR = Path(os.environ.get("STATE_DIR", "/state"))
SEED_SERVERS_FILE = Path(os.environ.get("INITIAL_SERVERS_FILE", "/app/servers.json"))
DEFAULT_REQUESTS_FILE = Path(os.environ.get("DEFAULT_REQUESTS_FILE", "/app/requests.csv"))

server_store = ServerStore(STATE_DIR, SEED_SERVERS_FILE)
run_store = RunStore(STATE_DIR)
simulation_lock = threading.Lock()

app = FastAPI(
    title="Load Balancer Simulation API",
    version="1.0.0",
    description="Manage server capacity and create deterministic simulation traces.",
)


class ServerPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    id: str = Field(min_length=1, max_length=80, pattern=f"^{SERVER_ID_PATTERN.pattern}$")
    cpu_units_per_tick: int = Field(gt=0, strict=True)
    mem_mb: int = Field(gt=0, strict=True)
    rate_limit_per_sec: int = Field(ge=0, strict=True)


def _server_json(server: Any) -> dict[str, Any]:
    return {
        "id": server.id,
        "cpu_units_per_tick": server.cpu_units_per_tick,
        "mem_mb": server.mem_mb,
        "rate_limit_per_sec": server.rate_limit_per_sec,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/servers")
def list_servers() -> dict[str, list[dict[str, Any]]]:
    return {"servers": [_server_json(server) for server in server_store.list_servers()]}


@app.put("/api/servers")
def replace_servers(document: dict[str, Any] = Body(...)) -> dict[str, list[dict[str, Any]]]:
    """Import a servers.json document, replacing the whole server list."""
    try:
        servers = server_store.replace_servers(document)
    except SimulationInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"servers": [_server_json(server) for server in servers]}


@app.post("/api/servers", status_code=status.HTTP_201_CREATED)
def create_server(payload: ServerPayload) -> dict[str, Any]:
    try:
        return _server_json(server_store.create_server(payload.model_dump()))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.put("/api/servers/{server_id}")
def update_server(server_id: str, payload: ServerPayload) -> dict[str, Any]:
    try:
        return _server_json(server_store.update_server(server_id, payload.model_dump()))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Server '{server_id}' was not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.delete(
    "/api/servers/{server_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_server(server_id: str) -> Response:
    try:
        server_store.delete_server(server_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Server '{server_id}' was not found") from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/api/simulations/latest")
def latest_simulation() -> dict[str, Any]:
    latest = run_store.latest()
    if latest is None:
        raise HTTPException(status_code=404, detail="No simulation has been run yet")
    return latest


@app.get("/api/simulations/latest/trace")
def latest_trace() -> FileResponse:
    path = run_store.latest_trace()
    if path is None:
        raise HTTPException(status_code=404, detail="No simulation trace is available yet")
    return FileResponse(path, media_type="application/x-ndjson", filename="run.jsonl")


@app.get("/api/simulations/latest/metrics")
def latest_metrics() -> dict[str, Any]:
    latest = run_store.latest()
    if latest is None:
        raise HTTPException(status_code=404, detail="No simulation has been run yet")
    metrics = run_store.metrics_for(latest["run_id"])
    if metrics is None:
        raise HTTPException(status_code=404, detail="Metrics are not available for this run")
    return metrics


@app.get("/api/simulations/{run_id}/trace")
def simulation_trace(run_id: str) -> FileResponse:
    path = run_store.trace_for(run_id)
    if path is None:
        raise HTTPException(status_code=404, detail="Simulation trace was not found")
    return FileResponse(path, media_type="application/x-ndjson", filename="run.jsonl")


@app.get("/api/simulations/{run_id}/metrics")
def simulation_metrics(run_id: str) -> dict[str, Any]:
    metrics = run_store.metrics_for(run_id)
    if metrics is None:
        raise HTTPException(status_code=404, detail="Metrics for this run were not found")
    return metrics


@app.post("/api/simulations")
def create_simulation(
    requests_file: UploadFile | None = File(default=None),
    allow_concurrency: bool = Form(default=False),
    scheduling_strategy: str = Form(default="projected_drain"),
    enable_autoscaling: bool = Form(default=False),
    scale_up_cpu_percent: int = Form(default=80),
    scale_down_cpu_percent: int = Form(default=30),
    queue_threshold: int = Form(default=1),
    error_rate_threshold_percent: int = Form(default=1),
    cooldown_ticks: int = Form(default=2),
    min_cpu_percent: int = Form(default=50),
    max_cpu_percent: int = Form(default=200),
    max_queue_length: int | None = Form(default=None),
    max_wait_ticks: int | None = Form(default=None),
    starvation_guard_ticks: int | None = Form(default=None),
) -> dict[str, Any]:
    servers = server_store.list_servers()
    if not servers:
        raise HTTPException(
            status_code=409,
            detail="Add at least one server before running a simulation",
        )

    try:
        if requests_file is None:
            csv_text = DEFAULT_REQUESTS_FILE.read_text(encoding="utf-8-sig")
        else:
            csv_text = requests_file.file.read().decode("utf-8-sig")
        requests = parse_requests_csv(csv_text)
    except (OSError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=422, detail=f"Could not read requests CSV: {exc}") from exc
    except SimulationInputError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Serialize execution and latest-pointer updates. The server snapshot above remains
    # immutable for this run even if the dashboard changes configuration meanwhile.
    with simulation_lock:
        try:
            autoscaling = AutoscalingConfig(
                enabled=enable_autoscaling,
                scale_up_cpu_percent=scale_up_cpu_percent,
                scale_down_cpu_percent=scale_down_cpu_percent,
                queue_threshold=queue_threshold,
                error_rate_threshold_percent=error_rate_threshold_percent,
                cooldown_ticks=cooldown_ticks,
                min_cpu_percent=min_cpu_percent,
                max_cpu_percent=max_cpu_percent,
            )
            result = simulate(
                servers,
                requests,
                allow_concurrency=allow_concurrency,
                scheduling_strategy=scheduling_strategy,
                autoscaling=autoscaling,
                queue_policy=QueuePolicy(
                    max_queue_length=max_queue_length,
                    max_wait_ticks=max_wait_ticks,
                    starvation_guard_ticks=starvation_guard_ticks,
                ),
            )
        except SimulationInputError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        metadata = run_store.save(
            result.run_id,
            serialize_trace(result.events),
            result.summary,
            result.metrics,
            servers,
            requests,
        )
    return metadata


@app.get("/api/configuration")
def current_configuration() -> dict[str, Any]:
    return {
        "tick_seconds": 1,
        "servers": [_server_json(server) for server in server_store.list_servers()],
        "scheduling_strategies": list(STRATEGIES),
    }
