"""Validation of uploaded data and run options."""

from __future__ import annotations

import pytest

from app.simulation import (
    QueuePolicy,
    Request,
    Server,
    SimulationInputError,
    parse_requests_csv,
    parse_server_document,
    simulate,
)

HEADER = "t,request_id,work_units,mem_mb\n"


@pytest.mark.parametrize(
    "text",
    [
        "",
        HEADER,
        "t,request_id,work_units\n0,r1,10\n",
        HEADER + "0,r1,1.5,10\n",
        HEADER + "-1,r1,10,10\n",
        HEADER + "0,r1,0,10\n",
        HEADER + "0,r1,1,10\n1,r1,1,10\n",
        HEADER + "0,,1,10\n",
    ],
    ids=["empty", "header-only", "missing-column", "fractional-work", "negative-tick",
         "zero-work", "duplicate-id", "empty-id"],
)
def test_invalid_request_csv_is_rejected(text):
    with pytest.raises(SimulationInputError):
        parse_requests_csv(text)


def test_request_csv_tolerates_column_order_extra_columns_and_spaces():
    requests = parse_requests_csv("mem_mb,t,request_id,work_units,note\n 10 , 0 , r1 , 5 ,x\n")
    assert requests == [Request(0, "r1", 5, 10)]


def server_document(*servers):
    return {"tick_seconds": 1, "servers": list(servers)}


def server(sid, cpu=10, mem=100, rate=1):
    return {"id": sid, "cpu_units_per_tick": cpu, "mem_mb": mem, "rate_limit_per_sec": rate}


@pytest.mark.parametrize("sid", ["s1", "rack-1.a", "node_02", "A"])
def test_url_safe_server_ids_are_accepted(sid):
    assert parse_server_document(server_document(server(sid)))[0].id == sid


@pytest.mark.parametrize("sid", ["rack/1", "..", "-s1", "s 1", "s?1", "x" * 81])
def test_server_ids_that_break_api_paths_are_rejected(sid):
    with pytest.raises(SimulationInputError):
        parse_server_document(server_document(server(sid)))


@pytest.mark.parametrize(
    "document",
    [
        [],
        {"servers": {}},
        {"tick_seconds": 2, "servers": [server("s1")]},
        server_document(server("s1"), server("s1")),
        server_document(server("s1", cpu=0)),
        server_document(server("s1", mem=-1)),
        server_document(server("s1", rate=-1)),
        server_document(server("s1", cpu=1.5)),
        server_document(server("s1", cpu=True)),
    ],
)
def test_invalid_server_documents_are_rejected(document):
    with pytest.raises(SimulationInputError):
        parse_server_document(document)


def test_unknown_strategy_and_negative_limits_are_rejected():
    servers, requests = [Server("s1", 10, 100, 1)], [Request(0, "r1", 10, 10)]
    with pytest.raises(SimulationInputError):
        simulate(servers, requests, scheduling_strategy="random")
    with pytest.raises(SimulationInputError):
        simulate(servers, requests, queue_policy=QueuePolicy(max_queue_length=-1))
