from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.services.ingest import read_events


def test_parquet_round_trip(tmp_path: Path) -> None:
    csv = Path(__file__).resolve().parents[2] / "examples/events.csv"
    events = list(read_events(csv))
    path = tmp_path / "events.parquet"
    pq.write_table(pa.Table.from_pylist([e.model_dump(mode="json") for e in events]), path)
    assert list(read_events(path)) == events
