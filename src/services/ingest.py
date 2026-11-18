from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from src.models.domain import Event, Step


class LateEvent(ValueError):
    """An insertion would change already published step URNs."""


@dataclass
class TraceState:
    tail_index: int = -1
    tail_order: tuple[str, str] = ("", "")
    known: set[str] = field(default_factory=set)


def plan_steps(events: Sequence[Event], states: dict[str, TraceState]) -> list[Step]:
    rows = sorted((e.trace_id, e.stamp(), e.event_key(), i) for i, e in enumerate(events))
    result: list[Step] = []
    for trace, stamp, key, i in rows:
        state = states.setdefault(trace, TraceState())
        if key in state.known:
            continue
        if (stamp, key) < state.tail_order:
            raise LateEvent(f"Earlier event would renumber existing steps in {trace}")
        index = state.tail_index + 1
        result.append(Step(**events[i].model_dump(), urn=f"urn:step:{trace}:{index}",
                           step_index=index))
        state.known.add(key)
        state.tail_index, state.tail_order = index, (stamp, key)
    return result


# Unknown or read-only actions use ACCESSES. The action remains available verbatim.
WRITE_ACTIONS = {"write", "insert", "update", "delete", "create", "modify", "mutate", "upsert"}


def mutates(action: str) -> bool:
    return action.casefold() in WRITE_ACTIONS


def read_events(path: Path) -> Iterator[Event]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                yield Event.model_validate(row)
    elif suffix in {".jsonl", ".ndjson"}:
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield Event.model_validate_json(line)
    elif suffix == ".parquet":
        from pyarrow.parquet import ParquetFile

        for batch in ParquetFile(path).iter_batches(batch_size=5000):
            for item in batch.to_pylist():
                yield Event.model_validate(item)
    else:
        raise ValueError("Expected .csv, .jsonl, .ndjson or .parquet input")


def sorted_batches(events: Iterable[Event], size: int) -> Iterator[list[Event]]:
    """Spool before writing so interleaved traces and shuffled files order consistently."""
    if size < 1:
        raise ValueError("batch size must be positive")
    with tempfile.TemporaryDirectory() as temp:
        db = sqlite3.connect(Path(temp) / "events.db")
        try:
            db.execute("CREATE TABLE events(trace TEXT, stamp TEXT, key TEXT, body TEXT)")
            db.executemany("INSERT INTO events VALUES(?,?,?,?)", (
                (e.trace_id, e.stamp(), e.event_key(), e.model_dump_json()) for e in events
            ))
            cursor = db.execute("SELECT body FROM events ORDER BY trace, stamp, key")
            while rows := cursor.fetchmany(size):
                yield [Event.model_validate(json.loads(row[0])) for row in rows]
        finally:
            db.close()
