"""Four commands for collecting, replaying, reporting and querying observations."""

import argparse
import json
from .config import load_config
from .common import project_lock
from .pipeline import run_pipeline, replay_archives
from .reporting import generate_report
from .warehouse import open_warehouse


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/global.json", help="JSON dataset filters")
    parser.add_argument("--root", help="Data directory; overrides config and METHANE_ROOT")
    parser.add_argument("--output", default="reports", help="Directory for HTML, CSV and Parquet exports")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Collect live data; auto chooses full or incremental")
    run.add_argument("--mode", choices=["auto", "full", "incremental"], default="auto")
    run.add_argument("--no-report", action="store_true", help="Only update the warehouse")
    replay = commands.add_parser("replay", help="Process archived data without network access")
    replay.add_argument("--source", default="archives", help="Directory of archives or one manifest.json")
    replay.add_argument("--no-report", action="store_true", help="Only update the warehouse")
    commands.add_parser("report", help="Regenerate outputs from the committed warehouse")
    query = commands.add_parser("query", help="Run read-only SQL")
    query.add_argument("--sql", default="SHOW TABLES")
    args = parser.parse_args(argv)
    cfg = load_config(args.config, args.root)
    if args.command == "query":
        with project_lock(cfg.work), open_warehouse(cfg.db_path, read_only=True) as con:
            print(con.execute(args.sql).df().to_string(index=False))
        return
    if args.command == "run":
        result = run_pipeline(cfg, args.mode)
    elif args.command == "replay":
        result = replay_archives(cfg, args.source)
    else:
        result = {}
    if not getattr(args, "no_report", False):
        result["report"] = generate_report(cfg, args.output)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
