"""Parallel SWAT simulation runner for ensemble / sensitivity runs.

Each simulation gets its own copy of the TxtInOut directory.  After SWAT
finishes, the specified output files are collected into a per-simulation
results folder.  Runs are executed in parallel using
:class:`concurrent.futures.ProcessPoolExecutor`.

Worker pickling
---------------
On Windows, ``ProcessPoolExecutor`` uses the ``spawn`` start method, which
requires the worker function to be importable at module level (no lambdas or
closures).  :func:`_run_one` satisfies this requirement and receives only
JSON-serialisable primitive arguments.
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

from .config import SimulationConfig, ParameterSpec
from .params_handler import apply_sample_to_dir
from .sampling import generate_samples, save_samples

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level worker (must be picklable for multiprocessing on Windows)
# ---------------------------------------------------------------------------


def _run_one(
    sim_id: int,
    sample_row: dict[str, float],
    source_txtinout: str,
    run_dir: str,
    result_dir: str,
    swat_exe_name: str,
    output_files: list[str],
    parameters_dicts: list[dict[str, Any]],
    delete_run_dir: bool,
    timeout: int | None,
) -> dict[str, Any]:
    """Execute a single SWAT simulation.  Module-level for pickling.

    Returns a dict with keys: ``sim_id``, ``status``, ``duration_s``, ``error``.
    """
    start = time.perf_counter()
    run_dir_path = Path(run_dir)
    result_dir_path = Path(result_dir)

    try:
        # ---- Prepare run directory ----------------------------------------
        if run_dir_path.exists():
            shutil.rmtree(run_dir_path)
        shutil.copytree(source_txtinout, run_dir_path)

        # ---- Apply parameter changes ---------------------------------------
        parameters = [ParameterSpec.from_dict(d) for d in parameters_dicts]
        apply_sample_to_dir(run_dir_path, sample_row, parameters)

        # ---- Run SWAT --------------------------------------------------------
        swat_exe = run_dir_path / swat_exe_name
        proc = subprocess.run(
            [str(swat_exe)],
            cwd=str(run_dir_path),
            capture_output=True,
            timeout=timeout,
        )

        if proc.returncode != 0:
            stderr_snippet = proc.stderr.decode(errors="replace")[:500]
            return _result(sim_id, "failed", start,
                           f"SWAT exit code {proc.returncode}: {stderr_snippet}")

        # ---- Collect outputs -----------------------------------------------
        result_dir_path.mkdir(parents=True, exist_ok=True)
        missing: list[str] = []
        for fname in output_files:
            src = run_dir_path / fname
            if src.exists():
                shutil.copy2(src, result_dir_path / fname)
            else:
                missing.append(fname)

        status = "completed" if not missing else "partial"
        error = f"Missing output files: {missing}" if missing else None
        return _result(sim_id, status, start, error)

    except subprocess.TimeoutExpired:
        return _result(sim_id, "timeout", start,
                       f"Exceeded timeout of {timeout}s")
    except Exception as exc:  # noqa: BLE001
        return _result(sim_id, "error", start, repr(exc))
    finally:
        if delete_run_dir and run_dir_path.exists():
            shutil.rmtree(run_dir_path, ignore_errors=True)


def _result(
    sim_id: int,
    status: str,
    start: float,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "sim_id": sim_id,
        "status": status,
        "duration_s": round(time.perf_counter() - start, 2),
        "error": error or "",
    }


# ---------------------------------------------------------------------------
# Resume check
# ---------------------------------------------------------------------------


def _sim_is_complete(
    sim_id: int,
    results_dir: Path,
    output_files: list[str],
) -> bool:
    """Return True if all expected output files already exist for *sim_id*."""
    sim_result = results_dir / f"sim_{sim_id:04d}"
    return sim_result.is_dir() and all((sim_result / f).exists() for f in output_files)


# ---------------------------------------------------------------------------
# Public orchestrator
# ---------------------------------------------------------------------------


def run_simulations(
    config: SimulationConfig,
    samples: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Run all SWAT simulations defined in *config*.

    If *samples* is ``None``, a fresh Latin Hypercube sample set is generated
    using ``config.n_simulations`` and ``config.seed``.

    The function:

    1. Creates ``work_dir`` and ``results_dir``.
    2. Writes ``params_manifest.csv``, ``samples.csv``, and ``config.json``
       to ``results_dir`` for reproducibility.
    3. Skips completed simulations when ``config.resume`` is ``True``.
    4. Dispatches all pending simulations to a ``ProcessPoolExecutor``.
    5. Appends results to ``run_log.csv`` in ``results_dir``.

    Args:
        config: Fully configured :class:`~.config.SimulationConfig`.
        samples: Optional pre-generated sample DataFrame.  If supplied, its
            ``sim_id`` index is used directly.  Columns must match
            ``config.parameters`` identifiers.

    Returns:
        DataFrame of run results indexed by ``sim_id`` with columns
        ``status``, ``duration_s``, and ``error``.
    """
    config.validate()
    config.work_dir.mkdir(parents=True, exist_ok=True)
    config.results_dir.mkdir(parents=True, exist_ok=True)

    # ---- Generate or validate samples ------------------------------------
    if samples is None:
        logger.info(
            "Generating %d LHS samples (seed=%s) …", config.n_simulations, config.seed
        )
        samples = generate_samples(config.parameters, config.n_simulations, config.seed)

    _validate_samples(samples, config)

    # ---- Persist inputs for reproducibility ------------------------------
    samples_path = config.results_dir / "samples.csv"
    if not samples_path.exists():
        save_samples(samples, samples_path)
        logger.info("Samples written to %s", samples_path)

    manifest_path = config.results_dir / "params_manifest.csv"
    if not manifest_path.exists():
        samples.to_csv(manifest_path, index=True)
        logger.info("Manifest written to %s", manifest_path)

    config_path = config.results_dir / "simulation_config.json"
    if not config_path.exists():
        config.to_json(config_path)
        logger.info("Config written to %s", config_path)

    # ---- Determine which sims to run ------------------------------------
    all_ids = list(samples.index)
    if config.resume:
        pending = [
            i for i in all_ids
            if not _sim_is_complete(i, config.results_dir, config.output_files)
        ]
        skipped = len(all_ids) - len(pending)
        if skipped:
            logger.info("Resuming: %d already complete, %d to run", skipped, len(pending))
    else:
        pending = all_ids
        logger.info("Running all %d simulations (resume=False)", len(pending))

    if not pending:
        logger.info("All simulations already complete.")
        return _load_run_log(config.results_dir)

    # ---- Serialise worker args (primitive types only — pickling safe) ----
    source_str = str(config.source_txtinout)
    work_str = str(config.work_dir)
    results_str = str(config.results_dir)
    params_dicts = [p.to_dict() for p in config.parameters]

    n_workers = min(config.max_workers or os.cpu_count() or 1, len(pending))
    logger.info(
        "Launching %d workers for %d simulations …", n_workers, len(pending)
    )

    run_results: list[dict[str, Any]] = []

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(
                _run_one,
                sim_id=sim_id,
                sample_row=samples.loc[sim_id].to_dict(),
                source_txtinout=source_str,
                run_dir=str(Path(work_str) / f"sim_{sim_id:04d}"),
                result_dir=str(Path(results_str) / f"sim_{sim_id:04d}"),
                swat_exe_name=config.swat_exe_name,
                output_files=config.output_files,
                parameters_dicts=params_dicts,
                delete_run_dir=config.delete_run_dirs,
                timeout=config.timeout,
            ): sim_id
            for sim_id in pending
        }

        completed = 0
        total = len(pending)
        for future in as_completed(futures):
            result = future.result()
            run_results.append(result)
            completed += 1
            status_flag = "✓" if result["status"] == "completed" else "✗"
            logger.info(
                "[%d/%d] sim_%04d %s %s (%.1fs)%s",
                completed,
                total,
                result["sim_id"],
                status_flag,
                result["status"],
                result["duration_s"],
                f" — {result['error']}" if result["error"] else "",
            )

    # ---- Append to run log -----------------------------------------------
    _append_run_log(run_results, config.results_dir / "run_log.csv")
    logger.info("Run log updated: %s", config.results_dir / "run_log.csv")

    return _load_run_log(config.results_dir)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_samples(samples: pd.DataFrame, config: SimulationConfig) -> None:
    """Check that sample columns match the configured parameter identifiers."""
    expected = {p.identifier for p in config.parameters}
    actual = set(samples.columns)
    missing = expected - actual
    extra = actual - expected
    if missing:
        raise ValueError(f"Sample DataFrame is missing columns: {sorted(missing)}")
    if extra:
        logger.warning(
            "Sample DataFrame has extra columns (will be ignored): %s", sorted(extra)
        )


def _append_run_log(results: list[dict[str, Any]], log_path: Path) -> None:
    """Append run results to the CSV log (creates header if new file)."""
    fieldnames = ["sim_id", "status", "duration_s", "error"]
    write_header = not log_path.exists()
    with log_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(results)


def _load_run_log(results_dir: Path) -> pd.DataFrame:
    """Load the run_log.csv if it exists, or return an empty DataFrame."""
    log_path = results_dir / "run_log.csv"
    if log_path.exists():
        df = pd.read_csv(log_path)
        df["sim_id"] = df["sim_id"].astype(int)
        return df.set_index("sim_id")
    return pd.DataFrame(columns=["status", "duration_s", "error"])
