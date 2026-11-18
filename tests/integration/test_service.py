from collections.abc import Iterator
from pathlib import Path
from time import monotonic, sleep

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from testcontainers.core.container import DockerContainer

from src.main import app
from src.services.ingest import read_events

pytestmark = pytest.mark.integration
EXAMPLE = Path(__file__).resolve().parents[2] / "examples/events.csv"
PREFIX = "/api/v1/process"


@pytest.fixture(scope="module")
def live_service() -> Iterator[tuple[TestClient, str, str]]:
    password = "ephemeral-test-db"
    container = DockerContainer("neo4j:5.26-community").with_env("NEO4J_AUTH", f"neo4j/{password}")
    with container.with_exposed_ports(7687) as running:
        uri = f"bolt://{running.get_container_host_ip()}:{running.get_exposed_port(7687)}"
        with GraphDatabase.driver(uri, auth=("neo4j", password)) as driver:
            deadline = monotonic() + 120
            while True:
                try:
                    driver.verify_connectivity()
                    break
                except (OSError, ServiceUnavailable):
                    if monotonic() >= deadline:
                        raise
                    sleep(1)
            with driver.session() as session:
                session.run("CREATE (:Actor {urn:'urn:actor:intern_bob', team2:true}), "
                            "(:Resource {urn:'urn:resource:billing_db:table:invoices', team1:true})").consume()
        with pytest.MonkeyPatch.context() as env:
            env.setenv("NEO4J_URI", uri)
            env.setenv("NEO4J_PASSWORD", password)
            with TestClient(app) as client:
                yield client, uri, password


def test_full_workflow(live_service: tuple[TestClient, str, str]) -> None:
    client, uri, password = live_service
    events = [e.model_dump(mode="json") for e in read_events(EXAMPLE)]
    assert client.get("/docs").status_code == 200
    assert len(client.get("/openapi.json").json()["paths"]) == 4
    assert client.post(PREFIX + "/ingest", json=events).json() == {
        "received": 5, "inserted": 5, "duplicates": 0
    }
    assert client.post(PREFIX + "/ingest", json=events).json()["duplicates"] == 5
    trace = client.get(PREFIX + "/traces/tx_9801").json()
    assert [s["action"] for s in trace] == ["READ", "WRITE", "READ"]
    assert [s["urn"] for s in trace] == [f"urn:step:tx_9801:{n}" for n in range(3)]
    assert client.get(PREFIX + "/anomalies/loops").json() == [
        {"trace_id": "tx_9801", "action": "READ", "step_indices": [0, 2]}
    ]
    outliers = client.get(PREFIX + "/metrics/bottlenecks?percentile=95").json()
    assert len(outliers) == 1
    assert outliers[0]["elapsed_ms"] == 1600
    assert outliers[0]["threshold_ms"] == pytest.approx(1444)
    late = dict(events[2], timestamp="2026-11-17T00:00:00Z")
    unrelated = dict(events[0], trace_id="aaa")
    assert client.post(PREFIX + "/ingest", json=[unrelated, late]).status_code == 409
    assert client.get(PREFIX + "/traces/aaa").status_code == 404
    assert client.get(PREFIX + "/traces/missing").status_code == 404
    assert client.get(PREFIX + "/metrics/bottlenecks?percentile=101").status_code == 422
    assert client.post(PREFIX + "/ingest", json=[{"trace_id": "bad"}]).status_code == 422
    with GraphDatabase.driver(uri, auth=("neo4j", password)) as driver, driver.session() as s:
        count = s.run("MATCH (a:ProcessStep)-[r:PRECEDES]->(b:ProcessStep) "
                      "RETURN count(r) AS n, all(x IN collect(a.trace_id=b.trace_id) WHERE x) AS ok").single()
        assert count and count["n"] == 3 and count["ok"]
        link = s.run("MATCH (:Actor {team2:true})-[:PERFORMED]->(:ProcessStep)"
                     "-[:MUTATES]->(:Resource {team1:true}) RETURN count(*) AS n").single()
        assert link and link["n"] == 1
