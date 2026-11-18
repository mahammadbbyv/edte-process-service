import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from src.models.domain import Event
from src.services.ingest import LateEvent, TraceState, mutates, plan_steps, read_events, sorted_batches

EXAMPLE = Path(__file__).resolve().parents[2] / "examples/events.csv"


class IngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.events = list(read_events(EXAMPLE))

    def test_csv_sorts_to_canonical_ids_and_preserves_milliseconds(self) -> None:
        steps = plan_steps(self.events, {})
        first = [s for s in steps if s.trace_id == "tx_9801"]
        self.assertEqual([s.action for s in first], ["READ", "WRITE", "READ"])
        self.assertEqual([s.urn for s in first], [f"urn:step:tx_9801:{i}" for i in range(3)])
        self.assertEqual([s.timestamp.microsecond for s in first], [0, 40_000, 640_000])
        self.assertEqual(steps[3].step_index, 0)

    def test_in_batch_duplicate_and_replay(self) -> None:
        state: dict[str, TraceState] = {}
        self.assertEqual(len(plan_steps(self.events + self.events, state)), 5)
        self.assertEqual(plan_steps(self.events, state), [])

    def test_stable_ties_independent_of_input_order(self) -> None:
        a = self.events[0]
        b = a.model_copy(update={"action": "RETRY"})
        self.assertEqual(plan_steps([a, b], {}), plan_steps([b, a], {}))

    def test_rejects_late_new_event(self) -> None:
        state: dict[str, TraceState] = {}
        plan_steps([self.events[0]], state)
        with self.assertRaisesRegex(LateEvent, "tx_9801"):
            plan_steps([self.events[2]], state)

    def test_replay_and_append(self) -> None:
        state: dict[str, TraceState] = {}
        plan_steps([self.events[2]], state)
        added = plan_steps([self.events[2], self.events[4]], state)
        self.assertEqual([s.urn for s in added], ["urn:step:tx_9801:1"])

    def test_batches_consistent_across_boundaries(self) -> None:
        for size in (1, 2, 3, 7):
            with self.subTest(size=size):
                state: dict[str, TraceState] = {}
                batches = list(sorted_batches(self.events, size))
                self.assertTrue(all(0 < len(b) <= size for b in batches))
                self.assertEqual([s for b in batches for s in plan_steps(b, state)],
                                 plan_steps(self.events, {}))

    def test_invalid_batch_size_and_empty_input(self) -> None:
        with self.assertRaises(ValueError):
            list(sorted_batches([], 0))
        self.assertEqual(list(sorted_batches([], 4)), [])

    def test_json_lines_supports_blank_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            path.write_text("\n\n".join(e.model_dump_json() for e in self.events), encoding="utf-8")
            self.assertEqual(list(read_events(path)), self.events)

    def test_input_failure_precedes_first_committed_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.ndjson"
            path.write_text(self.events[0].model_dump_json() + '\n{"bad":true}', encoding="utf-8")
            with self.assertRaises(ValidationError):
                next(sorted_batches(read_events(path), 1))

    def test_validates_trace_urns_duration_and_time(self) -> None:
        invalid = {"trace_id": "x:y", "actor_urn": "bob", "system_urn": "urn:actor:x",
                   "resource_urn": "urn:resource:x", "duration_ms": -1,
                   "timestamp": "2026-11-18T12:00:00.000001Z", "action": " "}
        for name, value in invalid.items():
            with self.subTest(name=name), self.assertRaises(ValidationError):
                Event.model_validate({**self.events[0].model_dump(), name: value})
        with self.assertRaises(ValidationError):
            Event.model_validate({**self.events[0].model_dump(), "timestamp": "2026-11-18T12:00:00"})

    def test_timezone_normalization_preserves_event_identity(self) -> None:
        e = self.events[2]
        offset = Event.model_validate({**e.model_dump(), "timestamp": "2026-11-18T13:00:00+01:00"})
        self.assertEqual(offset.event_key(), e.event_key())

    def test_write_actions_and_unknown_format(self) -> None:
        self.assertTrue(mutates("Update"))
        self.assertFalse(mutates("READ"))
        with self.assertRaisesRegex(ValueError, "Expected"):
            list(read_events(Path("input.xml")))
