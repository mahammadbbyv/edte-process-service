from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, cast

from fastapi import Depends, FastAPI, HTTPException, Query, Request

from src.config import Settings
from src.graph.store import EventBatch, Store
from src.models.domain import Bottleneck, Loop, Step, Summary, TraceId
from src.services.ingest import LateEvent


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    graph = Store.connect(Settings())
    try:
        graph.initialize()
        app.state.graph = graph
        yield
    finally:
        graph.close()


app = FastAPI(title="EDTE Event & Process Trace Engine", version="0.1.0", lifespan=lifespan)


def graph(request: Request) -> Store:
    return cast(Store, request.app.state.graph)


Database = Annotated[Store, Depends(graph)]
PREFIX = "/api/v1/process"


@app.post(PREFIX + "/ingest", responses={409: {"description": "Event precedes published history"}})
def ingest(events: EventBatch, db: Database) -> Summary:
    """Validate and atomically merge a batch of events."""
    try:
        return db.ingest(events)
    except LateEvent as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get(PREFIX + "/traces/{trace_id}", responses={404: {"description": "Unknown trace"}})
def trace(trace_id: TraceId, db: Database) -> list[Step]:
    """Return steps with their actor, system, resource and execution duration."""
    steps = db.trace(trace_id)
    if not steps:
        raise HTTPException(404, "Trace not found")
    return steps


@app.get(PREFIX + "/anomalies/loops")
def loops(db: Database) -> list[Loop]:
    """Find actions that recur within the same trace."""
    return db.loops()


@app.get(PREFIX + "/metrics/bottlenecks")
def bottlenecks(db: Database, percentile: Annotated[float, Query(ge=0, le=100)] = 95) -> list[Bottleneck]:
    """Return adjacent step transitions at or above the selected elapsed-time percentile."""
    return db.bottlenecks(percentile)
