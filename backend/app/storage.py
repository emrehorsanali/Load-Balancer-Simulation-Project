"""Small, file-backed stores for editable server configuration and run artifacts."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import tempfile
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .simulation import Request, Server, SimulationInputError, parse_server, parse_server_document


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        # mkstemp creates owner-only files; state files must stay readable from the
        # host even though the container writes them as root.
        os.fchmod(file_descriptor, 0o644)
        with os.fdopen(file_descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _json_bytes(value: Any, compact: bool = False) -> bytes:
    if compact:
        return (json.dumps(value, separators=(",", ":")) + "\n").encode("utf-8")
    return (json.dumps(value, indent=2, sort_keys=False) + "\n").encode("utf-8")


class ServerStore:
    """Persist the editable server list in the mounted state directory."""

    def __init__(self, state_dir: Path, seed_file: Path):
        self.path = state_dir / "servers.json"
        self.seed_file = seed_file
        self._lock = threading.RLock()
        with self._lock:
            if not self.path.exists():
                if not seed_file.is_file():
                    raise RuntimeError(f"Initial server file not found: {seed_file}")
                document = json.loads(seed_file.read_text(encoding="utf-8"))
                parse_server_document(document)
                _atomic_write(self.path, _json_bytes(document))
            self._read_servers_unlocked()

    def _read_servers_unlocked(self) -> list[Server]:
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
            return parse_server_document(document)
        except (OSError, json.JSONDecodeError, SimulationInputError) as exc:
            raise RuntimeError(f"Could not load server configuration from {self.path}: {exc}") from exc

    def _save_unlocked(self, servers: list[Server]) -> None:
        document = {
            "tick_seconds": 1,
            "servers": [asdict(server) for server in sorted(servers, key=lambda item: item.id)],
        }
        parse_server_document(document)
        _atomic_write(self.path, _json_bytes(document))

    def list_servers(self) -> list[Server]:
        with self._lock:
            return self._read_servers_unlocked()

    def create_server(self, payload: dict[str, Any]) -> Server:
        server = parse_server(payload)
        with self._lock:
            servers = self._read_servers_unlocked()
            if any(item.id == server.id for item in servers):
                raise ValueError(f"Server '{server.id}' already exists")
            servers.append(server)
            self._save_unlocked(servers)
        return server

    def update_server(self, server_id: str, payload: dict[str, Any]) -> Server:
        server = parse_server(payload)
        if server.id != server_id:
            raise ValueError("Server id cannot be changed")
        with self._lock:
            servers = self._read_servers_unlocked()
            if not any(item.id == server_id for item in servers):
                raise KeyError(server_id)
            self._save_unlocked([server if item.id == server_id else item for item in servers])
        return server

    def delete_server(self, server_id: str) -> None:
        with self._lock:
            servers = self._read_servers_unlocked()
            if not any(item.id == server_id for item in servers):
                raise KeyError(server_id)
            self._save_unlocked([item for item in servers if item.id != server_id])

    def replace_servers(self, document: Any) -> list[Server]:
        """Replace the whole list with a validated servers.json document."""
        servers = parse_server_document(document)
        with self._lock:
            self._save_unlocked(servers)
        return servers


class RunStore:
    """Atomically save each trace, a stable latest trace, and summary metadata."""

    _RUN_ID = re.compile(r"^[0-9a-f]{64}$")

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.runs_dir = state_dir / "runs"
        self.latest_path = state_dir / "latest.json"
        self.trace_path = state_dir / "run.jsonl"
        self._lock = threading.RLock()

    def save(
        self,
        run_id: str,
        trace: bytes,
        summary: dict[str, Any],
        metrics: dict[str, Any],
        servers: list[Server],
        requests: list[Request],
    ) -> dict[str, Any]:
        if not self._RUN_ID.fullmatch(run_id):
            raise ValueError("Invalid run id")
        metadata = {
            "run_id": run_id,
            "summary": summary,
            "trace_url": f"/api/simulations/{run_id}/trace",
            "metrics_url": f"/api/simulations/{run_id}/metrics",
        }
        run_dir = self.runs_dir / run_id
        server_snapshot = {
            "tick_seconds": 1,
            "servers": [
                asdict(server) for server in sorted(servers, key=lambda item: item.id)
            ],
        }
        request_buffer = io.StringIO(newline="")
        request_writer = csv.writer(request_buffer, lineterminator="\n")
        request_writer.writerow(["t", "request_id", "work_units", "mem_mb"])
        for request in sorted(requests, key=lambda item: (item.t, item.request_id)):
            request_writer.writerow(
                [request.t, request.request_id, request.work_units, request.mem_mb]
            )
        with self._lock:
            _atomic_write(run_dir / "run.jsonl", trace)
            _atomic_write(run_dir / "summary.json", _json_bytes(metadata))
            _atomic_write(run_dir / "metrics.json", _json_bytes(metrics, compact=True))
            _atomic_write(run_dir / "servers.json", _json_bytes(server_snapshot))
            _atomic_write(run_dir / "requests.csv", request_buffer.getvalue().encode("utf-8"))
            _atomic_write(self.trace_path, trace)
            _atomic_write(self.latest_path, _json_bytes(metadata))
        return metadata

    def latest(self) -> dict[str, Any] | None:
        with self._lock:
            try:
                return json.loads(self.latest_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                return None
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Latest run metadata is invalid: {exc}") from exc

    def trace_for(self, run_id: str) -> Path | None:
        if not self._RUN_ID.fullmatch(run_id):
            return None
        candidate = self.runs_dir / run_id / "run.jsonl"
        return candidate if candidate.is_file() else None

    def metrics_for(self, run_id: str) -> dict[str, Any] | None:
        if not self._RUN_ID.fullmatch(run_id):
            return None
        candidate = self.runs_dir / run_id / "metrics.json"
        with self._lock:
            try:
                return json.loads(candidate.read_text(encoding="utf-8"))
            except FileNotFoundError:
                return None
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Metrics for run {run_id} are invalid: {exc}") from exc

    def latest_trace(self) -> Path | None:
        return self.trace_path if self.trace_path.is_file() else None
