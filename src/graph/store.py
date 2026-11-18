from __future__ import annotations

import sysconfig
from pathlib import Path
from typing import Annotated

from neo4j import Driver, GraphDatabase, ManagedTransaction
from pydantic import Field

from src.config import Settings
from src.models.domain import Bottleneck, Event, Loop, Step, Summary
from src.services.ingest import TraceState, mutates, plan_steps

EventBatch = Annotated[list[Event], Field(min_length=1, max_length=50_000)]

# Lock the first step of each trace in a consistent order. A transient property causes
# a write lock. Rollback also removes any newly created placeholder.
LOCK = """
UNWIND $traces AS trace
WITH trace ORDER BY trace
MERGE (h:ProcessStep {urn: 'urn:step:' + trace + ':0'})
SET h._edte_write_lock = true
REMOVE h._edte_write_lock
"""

HISTORY = """
UNWIND $traces AS trace
MATCH (s:ProcessStep)
WHERE s.urn STARTS WITH ('urn:step:' + trace + ':') AND s.trace_id = trace
RETURN trace, s.step_index AS idx, s.order_stamp AS stamp, s.event_key AS key
"""

WRITE = """
UNWIND $rows AS row
MERGE (s:ProcessStep {urn: row.urn})
SET s.trace_id = row.trace_id, s.step_index = row.step_index,
    s.event_key = row.event_key, s.order_stamp = row.order_stamp,
    s.timestamp = datetime(row.order_stamp), s.duration_ms = row.duration_ms,
    s.action = row.action
MERGE (a:Actor {urn: row.actor_urn})
MERGE (sys:System {urn: row.system_urn})
MERGE (r:Resource {urn: row.resource_urn})
MERGE (a)-[:PERFORMED]->(s)
MERGE (s)-[:EXECUTES_ON]->(sys)
FOREACH (_ IN CASE WHEN row.mutates THEN [1] ELSE [] END | MERGE (s)-[:MUTATES]->(r))
FOREACH (_ IN CASE WHEN row.mutates THEN [] ELSE [1] END | MERGE (s)-[:ACCESSES]->(r))
"""

CONNECT = """
UNWIND $edges AS e
MATCH (a:ProcessStep {urn: e.previous}), (b:ProcessStep {urn: e.current})
MERGE (a)-[:PRECEDES]->(b)
"""

TRACE = """
MATCH (s:ProcessStep)
WHERE s.urn STARTS WITH ('urn:step:' + $trace + ':') AND s.trace_id = $trace
MATCH (a:Actor)-[:PERFORMED]->(s)-[:EXECUTES_ON]->(sys:System)
MATCH (s)-[:MUTATES|ACCESSES]->(r:Resource)
RETURN s.urn AS urn, s.trace_id AS trace_id, s.step_index AS step_index,
       s.order_stamp AS timestamp, s.duration_ms AS duration_ms, s.action AS action,
       a.urn AS actor_urn, sys.urn AS system_urn, r.urn AS resource_urn
ORDER BY step_index
"""

LOOPS = """
MATCH (s:ProcessStep) WHERE s.trace_id IS NOT NULL
WITH s ORDER BY s.trace_id, s.action, s.step_index
WITH s.trace_id AS trace_id, s.action AS action, collect(s.step_index) AS step_indices
WHERE size(step_indices) > 1
RETURN trace_id, action, step_indices ORDER BY trace_id, action
"""

BOTTLENECKS = """
MATCH (a:ProcessStep)-[:PRECEDES]->(b:ProcessStep)
WITH percentileCont(b.timestamp.epochMillis - a.timestamp.epochMillis, $fraction) AS threshold
MATCH (a:ProcessStep)-[:PRECEDES]->(b:ProcessStep)
WITH a, b, threshold, b.timestamp.epochMillis - a.timestamp.epochMillis AS delta
WHERE delta >= threshold
RETURN a.trace_id AS trace_id, a.step_index AS from_index, b.step_index AS to_index,
       a.action AS from_action, b.action AS to_action, delta AS elapsed_ms,
       threshold AS threshold_ms
ORDER BY elapsed_ms DESC, trace_id, from_index
"""


def persist(tx: ManagedTransaction, events: list[Event]) -> Summary:
    incoming: dict[str, set[str]] = {}
    for event in events:
        incoming.setdefault(event.trace_id, set()).add(event.event_key())
    traces = sorted(incoming)
    tx.run(LOCK, traces=traces).consume()
    states = {trace: TraceState() for trace in traces}
    for record in tx.run(HISTORY, traces=traces):
        trace = record["trace"]
        state = states[trace]
        if record["key"] in incoming[trace]:
            state.known.add(record["key"])
        if record["idx"] > state.tail_index:
            state.tail_index = record["idx"]
            state.tail_order = (record["stamp"], record["key"])
    steps = plan_steps(events, states)
    if steps:
        rows = [dict(s.model_dump(exclude={"timestamp"}), order_stamp=s.stamp(),
                     event_key=s.event_key(), mutates=mutates(s.action)) for s in steps]
        tx.run(WRITE, rows=rows).consume()
        edges = [{"previous": f"urn:step:{s.trace_id}:{s.step_index - 1}", "current": s.urn}
                 for s in steps if s.step_index]
        tx.run(CONNECT, edges=edges).consume()
    return Summary(received=len(events), inserted=len(steps), duplicates=len(events) - len(steps))


class Store:
    def __init__(self, driver: Driver, database: str) -> None:
        self.driver = driver
        self.database = database

    @classmethod
    def connect(cls, settings: Settings) -> Store:
        driver = GraphDatabase.driver(
            settings.uri, auth=(settings.user, settings.password.get_secret_value())
        )
        return cls(driver, settings.database)

    def initialize(self) -> None:
        local = Path(__file__).resolve().parents[2] / "schema/constraints.cypher"
        installed = Path(sysconfig.get_path("data")) / "share/edte-process-service/constraints.cypher"
        schema = local if local.exists() else installed
        with self.driver.session(database=self.database) as session:
            for statement in schema.read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    session.run(statement.strip()).consume()

    def ingest(self, events: list[Event]) -> Summary:
        with self.driver.session(database=self.database) as session:
            return session.execute_write(persist, events)

    def trace(self, trace: str) -> list[Step]:
        with self.driver.session(database=self.database) as session:
            return [Step.model_validate(r.data()) for r in session.run(TRACE, trace=trace)]

    def loops(self) -> list[Loop]:
        with self.driver.session(database=self.database) as session:
            return [Loop.model_validate(r.data()) for r in session.run(LOOPS)]

    def bottlenecks(self, percentile: float) -> list[Bottleneck]:
        with self.driver.session(database=self.database) as session:
            return [Bottleneck.model_validate(r.data()) for r in session.run(
                BOTTLENECKS, fraction=percentile / 100
            )]

    def close(self) -> None:
        self.driver.close()
