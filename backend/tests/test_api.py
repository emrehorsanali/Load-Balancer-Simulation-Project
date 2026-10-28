"""HTTP API behaviour against a temporary state directory."""

from __future__ import annotations

import json
import stat

from support import DATA_DIR, bundled_inputs, supplied_validate

NEW_SERVER = {"id": "s3", "cpu_units_per_tick": 8, "mem_mb": 2048, "rate_limit_per_sec": 2}


def run_simulation(api, **fields):
    response = api.post("/api/simulations", data={key: str(value) for key, value in fields.items()})
    assert response.status_code == 200, response.text
    return response.json()


def test_server_crud_round_trip(api):
    assert [server["id"] for server in api.get("/api/servers").json()["servers"]] == ["s1", "s2"]
    assert api.post("/api/servers", json=NEW_SERVER).status_code == 201
    assert api.post("/api/servers", json=NEW_SERVER).status_code == 409
    updated = {**NEW_SERVER, "cpu_units_per_tick": 12}
    assert api.put("/api/servers/s3", json=updated).json()["cpu_units_per_tick"] == 12
    assert api.put("/api/servers/s3", json={**updated, "id": "other"}).status_code == 422
    assert api.delete("/api/servers/s3").status_code == 204
    assert api.delete("/api/servers/s3").status_code == 404


def test_server_ids_must_be_url_safe(api):
    response = api.post("/api/servers", json={**NEW_SERVER, "id": "rack/1"})
    assert response.status_code == 422
    assert api.post("/api/servers", json={**NEW_SERVER, "id": "  rack-1  "}).json()["id"] == "rack-1"
    assert api.delete("/api/servers/rack-1").status_code == 204


def test_importing_servers_json_replaces_the_list(api):
    document = {"tick_seconds": 1, "servers": [NEW_SERVER]}
    response = api.put("/api/servers", json=document)
    assert response.status_code == 200
    assert [server["id"] for server in api.get("/api/servers").json()["servers"]] == ["s3"]
    invalid = {"tick_seconds": 1, "servers": [{**NEW_SERVER, "mem_mb": 0}]}
    assert api.put("/api/servers", json=invalid).status_code == 422
    assert [server["id"] for server in api.get("/api/servers").json()["servers"]] == ["s3"]


def test_server_changes_apply_to_the_next_run(api):
    api.put("/api/servers", json={"tick_seconds": 1, "servers": [NEW_SERVER]})
    metadata = run_simulation(api)
    trace = api.get(metadata["trace_url"]).text.splitlines()
    servers_used = {json.loads(line).get("server_id") for line in trace} - {None}
    assert servers_used == {"s3"}


def test_default_run_is_versioned_valid_and_repeatable(api):
    first = run_simulation(api)
    second = run_simulation(api)
    assert first["run_id"] == second["run_id"]
    trace = api.get("/api/simulations/latest/trace").content
    assert trace == (DATA_DIR / "run.jsonl").read_bytes()
    servers, requests = bundled_inputs()
    events = [json.loads(line) for line in trace.decode().splitlines()]
    valid, issues, _ = supplied_validate(servers, requests, events)
    assert valid, [issue.msg for issue in issues]
    run_dir = api.state_dir / "runs" / first["run_id"]
    assert {path.name for path in run_dir.iterdir()} == {
        "run.jsonl", "summary.json", "metrics.json", "servers.json", "requests.csv",
    }


def test_disabled_autoscaler_settings_do_not_change_the_run_id(api):
    plain = run_simulation(api)
    with_ignored_settings = run_simulation(api, enable_autoscaling="false", cooldown_ticks=7)
    assert plain["run_id"] == with_ignored_settings["run_id"]
    autoscaled = run_simulation(api, enable_autoscaling="true")
    assert autoscaled["run_id"] != plain["run_id"]


def test_queue_policy_options_are_applied_and_validated(api):
    metadata = run_simulation(api, max_queue_length=0, max_wait_ticks=0)
    assert metadata["summary"]["queue_policy"] == {"max_queue_length": 0, "max_wait_ticks": 0}
    assert metadata["summary"]["dropped"] == 1  # r3 cannot start on arrival
    response = api.post("/api/simulations", data={"max_wait_ticks": "-1"})
    assert response.status_code == 422


def test_invalid_uploads_and_options_are_rejected(api):
    response = api.post(
        "/api/simulations",
        files={"requests_file": ("requests.csv", b"t,request_id\n0,r1\n", "text/csv")},
    )
    assert response.status_code == 422
    assert api.post("/api/simulations", data={"scheduling_strategy": "random"}).status_code == 422


def test_state_files_are_readable_outside_the_container(api):
    run_simulation(api)
    for path in [api.state_dir / "run.jsonl", api.state_dir / "latest.json", api.state_dir / "servers.json"]:
        assert stat.S_IMODE(path.stat().st_mode) == 0o644


def test_configuration_lists_the_available_strategies(api):
    configuration = api.get("/api/configuration").json()
    assert configuration["scheduling_strategies"] == ["projected_drain", "least_loaded", "round_robin"]
    assert configuration["tick_seconds"] == 1
