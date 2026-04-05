"""Command-line interface for SWAT multi-simulation ensemble runs."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Entry point for ``python -m swatpytools.simulation``."""

    parser = argparse.ArgumentParser(
        prog="swatpytools.simulation",
        description="SWAT multi-simulation ensemble runner",
        epilog=(
            "Workflow:\n"
            "  1. Build a JSON config file (see example in docs)\n"
            "  2. python -m swatpytools.simulation sample --config config.json\n"
            "  3. python -m swatpytools.simulation run   --config config.json\n"
            "  4. python -m swatpytools.simulation status --results-dir ./results"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable DEBUG logging"
    )

    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    # ------------------------------------------------------------------
    # sample sub-command
    # ------------------------------------------------------------------
    sp = subparsers.add_parser(
        "sample",
        help="Generate Latin Hypercube parameter samples and save to CSV",
    )
    sp.add_argument(
        "--config",
        required=True,
        type=Path,
        metavar="CONFIG_JSON",
        help="Path to simulation_config.json",
    )
    sp.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="CSV",
        help="Output CSV path (default: results_dir/samples.csv from config)",
    )
    sp.add_argument(
        "--seed",
        type=int,
        default=None,
        metavar="INT",
        help="Override the random seed in config",
    )
    sp.add_argument(
        "--n-sims",
        type=int,
        default=None,
        metavar="INT",
        help="Override n_simulations in config",
    )

    # ------------------------------------------------------------------
    # run sub-command
    # ------------------------------------------------------------------
    rp = subparsers.add_parser(
        "run",
        help="Execute SWAT simulations in parallel",
    )
    rp.add_argument(
        "--config",
        required=True,
        type=Path,
        metavar="CONFIG_JSON",
        help="Path to simulation_config.json",
    )
    rp.add_argument(
        "--samples",
        type=Path,
        default=None,
        metavar="CSV",
        help="Pre-generated samples CSV (default: results_dir/samples.csv)",
    )
    rp.add_argument(
        "--workers",
        type=int,
        default=None,
        metavar="N",
        help="Max parallel workers (overrides config)",
    )
    rp.add_argument(
        "--no-resume",
        action="store_true",
        help="Re-run even if results already exist (overrides config.resume)",
    )
    rp.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing anything",
    )

    # ------------------------------------------------------------------
    # status sub-command
    # ------------------------------------------------------------------
    stp = subparsers.add_parser(
        "status",
        help="Show progress of a running or completed simulation run",
    )
    stp.add_argument(
        "--results-dir",
        required=True,
        type=Path,
        metavar="DIR",
        help="Path to results_dir (contains run_log.csv)",
    )

    # ------------------------------------------------------------------
    # Parse & dispatch
    # ------------------------------------------------------------------
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command == "sample":
        _cmd_sample(args)
    elif args.command == "run":
        _cmd_run(args)
    elif args.command == "status":
        _cmd_status(args)
    else:
        parser.print_help()
        sys.exit(0)


# ---------------------------------------------------------------------------
# Sub-command implementations
# ---------------------------------------------------------------------------


def _cmd_sample(args: argparse.Namespace) -> None:
    from .config import SimulationConfig
    from .sampling import generate_samples, save_samples

    config = SimulationConfig.from_json(args.config)

    if args.seed is not None:
        config.seed = args.seed
    if args.n_sims is not None:
        config.n_simulations = args.n_sims

    output_path = args.output or (config.results_dir / "samples.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Generating %d LHS samples for %d parameters (seed=%s) …",
        config.n_simulations,
        len(config.parameters),
        config.seed,
    )
    samples = generate_samples(config.parameters, config.n_simulations, config.seed)
    save_samples(samples, output_path)

    logging.info("Samples written to %s", output_path)
    print(f"Generated {len(samples)} samples → {output_path}")
    print(f"Parameters: {', '.join(p.identifier for p in config.parameters)}")


def _cmd_run(args: argparse.Namespace) -> None:
    import pandas as pd
    from .config import SimulationConfig
    from .runner import run_simulations
    from .sampling import load_samples

    config = SimulationConfig.from_json(args.config)

    if args.workers is not None:
        config.max_workers = args.workers
    if args.no_resume:
        config.resume = False

    samples: pd.DataFrame | None = None
    samples_path = args.samples or (config.results_dir / "samples.csv")
    if samples_path.exists():
        samples = load_samples(samples_path)
        logging.info("Loaded %d samples from %s", len(samples), samples_path)

    if args.dry_run:
        n = len(samples) if samples is not None else config.n_simulations
        print(
            f"[dry-run] Would run {n} simulations with {config.max_workers} workers.\n"
            f"  source_txtinout : {config.source_txtinout}\n"
            f"  work_dir        : {config.work_dir}\n"
            f"  results_dir     : {config.results_dir}\n"
            f"  parameters      : {', '.join(p.identifier for p in config.parameters)}"
        )
        return

    log = run_simulations(config, samples)
    _print_summary(log)


def _cmd_status(args: argparse.Namespace) -> None:
    import pandas as pd

    results_dir = args.results_dir
    log_path = results_dir / "run_log.csv"

    if not log_path.exists():
        print(f"No run_log.csv found in {results_dir}")
        return

    log = pd.read_csv(log_path)
    counts = log["status"].value_counts()

    samples_path = results_dir / "samples.csv"
    total_planned = len(pd.read_csv(samples_path)) if samples_path.exists() else "?"

    print(f"\nSimulation status — {results_dir}")
    print(f"  Total planned : {total_planned}")
    for status, count in counts.items():
        print(f"  {status:12s}: {count}")
    print(f"  Total logged  : {len(log)}")

    if "duration_s" in log.columns and not log["duration_s"].isna().all():
        avg = log["duration_s"].mean()
        total_h = log["duration_s"].sum() / 3600
        print(f"\n  Avg duration  : {avg:.1f}s / sim")
        print(f"  Total CPU time: {total_h:.2f}h")

    failed = log[log["status"].isin(["failed", "error", "timeout"])]
    if not failed.empty:
        print(f"\n  Failed sims: {list(failed['sim_id'])}")


def _print_summary(log: "pd.DataFrame") -> None:
    if log.empty:
        return
    counts = log["status"].value_counts().to_dict()
    total = len(log)
    completed = counts.get("completed", 0)
    print(
        f"\nDone. {completed}/{total} completed. "
        + ", ".join(f"{k}: {v}" for k, v in counts.items())
    )
