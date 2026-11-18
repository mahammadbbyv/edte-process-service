import argparse
from pathlib import Path

from src.config import Settings
from src.graph.store import Store
from src.services.ingest import read_events, sorted_batches


def main() -> None:
    parser = argparse.ArgumentParser(description="Load and inspect EDTE transaction traces")
    sub = parser.add_subparsers(dest="command", required=True)
    ingest = sub.add_parser("ingest-logs")
    ingest.add_argument("--file", required=True, type=Path)
    ingest.add_argument("--batch-size", type=int, default=5000)
    inspect = sub.add_parser("inspect-trace")
    inspect.add_argument("--trace-id", required=True)
    args = parser.parse_args()
    db = Store.connect(Settings())
    try:
        db.initialize()
        if args.command == "ingest-logs":
            received = inserted = 0
            for batch in sorted_batches(read_events(args.file), args.batch_size):
                summary = db.ingest(batch)
                received += summary.received
                inserted += summary.inserted
            print(f"received={received} inserted={inserted} duplicates={received - inserted}")
        else:
            steps = db.trace(args.trace_id)
            if not steps:
                parser.exit(1, "Trace not found\n")
            for s in steps:
                print(f"{s.step_index}\t{s.stamp()}\t{s.action}\t{s.duration_ms} ms\t{s.actor_urn}")
    except (OSError, ValueError) as exc:
        parser.exit(2, f"{exc}\n")
    finally:
        db.close()


if __name__ == "__main__":
    main()
