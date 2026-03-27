"""Command-line entry point for London Data Model."""

from __future__ import annotations

import argparse
from pathlib import Path

from london_data_model.pipelines.schools.pipeline import run as run_schools_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ldm")
    subparsers = parser.add_subparsers(dest="domain", required=True)

    schools_parser = subparsers.add_parser("schools", help="School discovery pipeline")
    schools_subparsers = schools_parser.add_subparsers(dest="action", required=True)

    run_parser = schools_subparsers.add_parser("run", help="Run the schools pipeline")
    run_parser.add_argument("--area", default="KT19", help="Configured search area identifier")
    run_parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional path to an area configuration file",
    )
    run_parser.add_argument(
        "--input-mode",
        choices=("sample", "official"),
        default=None,
        help="Optional override for the configured input mode",
    )
    run_parser.set_defaults(command=handle_schools_run)

    return parser


def handle_schools_run(args: argparse.Namespace) -> int:
    result = run_schools_pipeline(
        area=args.area,
        config_path=args.config,
        input_mode=args.input_mode,
    )
    print(result.message)
    return 0 if result.status in ("stub", "success") else 1


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    command = getattr(args, "command", None)
    if command is None:
        parser.print_help()
        return 1
    return command(args)


if __name__ == "__main__":
    raise SystemExit(main())
