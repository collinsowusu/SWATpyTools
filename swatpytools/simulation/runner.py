"""Parallel SWAT simulation runner for ensemble / sensitivity runs.

Two parallelisation strategies are available, selected via
:attr:`~.config.SimulationConfig.strategy`:

``"rolling"`` (default)
    Each simulation gets its own fresh TxtInOut copy.  Up to *max_workers*
    copies exist on disk simultaneously; each is deleted after its outputs
    are collected.  Full isolation between simulations.

``"persistent"``
    Exactly *max_workers* TxtInOut copies are created upfront.  Each worker
    process owns one copy and runs its assigned batch of simulations
    sequentially in-place, reading parameter baselines once at startup.
    Eliminates the per-simulation copy/delete overhead — useful when
    *n_simulations* is large relative to *max_workers*.

Worker pickling
---------------
``loky.ProcessPoolExecutor`` is used when available (preferred) because it
uses cloudpickle serialization and avoids the ``__main__`` re-import problem
that causes ``BrokenProcessPool`` in Jupyter notebooks on Windows.  Falls
back to ``concurrent.futures.ProcessPoolExecutor`` if loky is not installed.
:func:`_run_one` and :func:`_run_batch` are module-level (no lambdas/closures)
to satisfy pickling requirements under either backend.
"""

from __future__ import annotations

import csv
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

try:
    from loky import ProcessPoolExecutor
    from concurrent.futures import as_completed
except ImportError:
    from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

import math

from .config import SimulationConfig, ParameterSpec
from .params_handler import apply_sample_to_dir, apply_sample_to_dir_inplace, read_baselines
from .sampling import generate_samples, save_samples

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level worker (must be picklable for multiprocessing on Windows)
# ---------------------------------------------------------------------------


def _expected_files(output_files: list[str], reach_filter: list[int] | None) -> list[str]:
    """Return the actual filenames expected in a result dir, accounting for reach extraction."""
    return [
        "reach_extract.csv" if (f == "output.rch" and reach_filter) else f
        for f in output_files
    ]


def _collect_outputs(
    run_dir: Path,
    result_dir: Path,
    output_files: list[str],
    reach_filter: list[int] | None,
) -> list[str]:
    """Copy output files from run_dir to result_dir.

    If reach_filter is set and output.rch is in output_files, the file is
    parsed and filtered to the specified reach IDs, then saved as
    reach_extract.csv rather than copying the full binary.
    """
    missing: list[str] = []
    for fname in output_files:
        src = run_dir / fname
        if fname == "output.rch" and reach_filter:
            if src.exists():
                from swatpytools.outputs.reach import read_reach
                df = read_reach(src, reach=reach_filter)
                df.to_csv(result_dir / "reach_extract.csv", index=False)
            else:
                missing.append(fname)
        else:
            if src.exists():
                shutil.copy2(src, result_dir / fname)
            else:
                missing.append(fname)
    return missing


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
    reach_filter: list[int] | None = None,
) -> dict[str, Any]:
    """Execute a single SWAT simulation.  Module-level for pickling.

    Returns a dict with keys: ``sim_id``, ``status``, ``duration_s``, ``error``.
    """
    start = time.perf_counter()
    run_dir_path = Path(run_dir)
    result_dir_path = Path(result_dir)

    try:
        # ---- Prepare run directory ----------------------------------------
        print(f"  [sim_{sim_id:04d}] Copying files ...", flush=True)
        if run_dir_path.exists():
            shutil.rmtree(run_dir_path)
        shutil.copytree(source_txtinout, run_dir_path)

        # ---- Apply parameter changes ---------------------------------------
        parameters = [ParameterSpec.from_dict(d) for d in parameters_dicts]
        apply_sample_to_dir(run_dir_path, sample_row, parameters)

        # ---- Run SWAT --------------------------------------------------------
        print(f"  [sim_{sim_id:04d}] Running SWAT ...", flush=True)
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
        missing = _collect_outputs(run_dir_path, result_dir_path, output_files, reach_filter)

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


def _run_batch(
    worker_id: int,
    sim_ids: list[int],
    sample_rows: dict[int, dict[str, float]],
    worker_dir: str,
    results_dir: str,
    swat_exe_name: str,
    output_files: list[str],
    parameters_dicts: list[dict[str, Any]],
    timeout: int | None,
    reach_filter: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Run a contiguous batch of simulations sequentially in a single worker directory.

    Used by the ``"persistent"`` strategy.  Parameter baselines are read once
    from *worker_dir* before the batch starts, so relative-method parameters
    always reference the original source values regardless of how many
    simulations have already modified the files in-place.

    Args:
        worker_id: Index of this worker (used for logging only).
        sim_ids: Ordered list of simulation IDs assigned to this worker.
        sample_rows: Mapping of ``sim_id → {param_identifier: value}``.
        worker_dir: Path to this worker's persistent TxtInOut copy.
        results_dir: Parent directory where per-sim output folders are written.
        swat_exe_name: Name of the SWAT executable within *worker_dir*.
        output_files: SWAT output filenames to collect after each run.
        parameters_dicts: Serialised :class:`~.config.ParameterSpec` dicts.
        timeout: Per-simulation timeout in seconds.  ``None`` = no limit.

    Returns:
        List of result dicts (one per sim_id) with keys ``sim_id``,
        ``status``, ``duration_s``, ``error``.
    """
    _batch_logger = logging.getLogger(__name__)
    worker_dir_path = Path(worker_dir)
    results_dir_path = Path(results_dir)

    parameters = [ParameterSpec.from_dict(d) for d in parameters_dicts]

    # Pre-read baseline values once — avoids compounding changes across sims
    baselines = read_baselines(worker_dir_path, parameters)
    _batch_logger.info("Worker %d: baselines loaded for %d parameter(s)", worker_id, len(baselines))

    batch_results: list[dict[str, Any]] = []

    for i, sim_id in enumerate(sim_ids, 1):
        start = time.perf_counter()
        result_dir_path = results_dir_path / f"sim_{sim_id:04d}"

        try:
            # ---- Apply parameters in-place using stored baselines ----------
            print(f"  [worker {worker_id}] sim_{sim_id:04d} ({i}/{len(sim_ids)}) Applying parameters ...", flush=True)
            apply_sample_to_dir_inplace(
                worker_dir_path, sample_rows[sim_id], parameters, baselines
            )

            # ---- Run SWAT --------------------------------------------------
            print(f"  [worker {worker_id}] sim_{sim_id:04d} ({i}/{len(sim_ids)}) Running SWAT ...", flush=True)
            swat_exe = worker_dir_path / swat_exe_name
            proc = subprocess.run(
                [str(swat_exe)],
                cwd=str(worker_dir_path),
                capture_output=True,
                timeout=timeout,
            )

            if proc.returncode != 0:
                stderr_snippet = proc.stderr.decode(errors="replace")[:500]
                result = _result(
                    sim_id, "failed", start,
                    f"SWAT exit code {proc.returncode}: {stderr_snippet}",
                )
            else:
                # ---- Collect outputs ---------------------------------------
                result_dir_path.mkdir(parents=True, exist_ok=True)
                missing = _collect_outputs(worker_dir_path, result_dir_path, output_files, reach_filter)
                status = "completed" if not missing else "partial"
                error = f"Missing output files: {missing}" if missing else None
                result = _result(sim_id, status, start, error)

        except subprocess.TimeoutExpired:
            result = _result(sim_id, "timeout", start, f"Exceeded timeout of {timeout}s")
        except Exception as exc:  # noqa: BLE001
            result = _result(sim_id, "error", start, repr(exc))

        batch_results.append(result)
        status_flag = "✓" if result["status"] == "completed" else "✗"
        _batch_logger.info(
            "Worker %d [%d/%d] sim_%04d %s %s (%.1fs)%s",
            worker_id, i, len(sim_ids), sim_id,
            status_flag, result["status"], result["duration_s"],
            f" — {result['error']}" if result["error"] else "",
        )

    return batch_results


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
    reach_filter: list[int] | None = None,
) -> bool:
    """Return True if all expected output files already exist for *sim_id*."""
    sim_result = results_dir / f"sim_{sim_id:04d}"
    expected = _expected_files(output_files, reach_filter)
    return sim_result.is_dir() and all((sim_result / f).exists() for f in expected)


# ---------------------------------------------------------------------------
# Public orchestrator
# ---------------------------------------------------------------------------


def run_simulations(
    config: SimulationConfig,
    samples: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Run all SWAT simulations defined in *config*.

    Selects the parallelisation strategy from ``config.strategy``:

    - ``"rolling"`` — per-simulation TxtInOut copy, rolling window of
      *max_workers* active copies (default, full isolation).
    - ``"persistent"`` — *max_workers* copies created upfront; each worker
      runs its assigned batch sequentially in-place (fewer copy operations).

    If *samples* is ``None``, a fresh Latin Hypercube sample set is generated
    using ``config.n_simulations`` and ``config.seed``.

    The function:

    1. Creates ``work_dir`` and ``results_dir``.
    2. Writes ``params_manifest.csv``, ``samples.csv``, and ``config.json``
       to ``results_dir`` for reproducibility.
    3. Skips completed simulations when ``config.resume`` is ``True``.
    4. Dispatches simulations via the chosen strategy.
    5. Appends results to ``run_log.csv`` in ``results_dir``.

    Args:
        config: Fully configured :class:`~.config.SimulationConfig`.
        samples: Optional pre-generated sample DataFrame.  Columns must match
            ``config.parameters`` identifiers.

    Returns:
        DataFrame of run results indexed by ``sim_id`` with columns
        ``status``, ``duration_s``, and ``error``.
    """
    # Configure logging to stdout if the caller has not set up any handlers,
    # so that progress messages are visible without extra boilerplate in scripts.
    if not logging.root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
        )

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
            if not _sim_is_complete(i, config.results_dir, config.output_files, config.reach_filter)
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

    n_workers = min(config.max_workers or os.cpu_count() or 1, len(pending))
    logger.info(
        "Strategy: %s | Workers: %d | Simulations: %d",
        config.strategy, n_workers, len(pending),
    )

    if config.strategy == "persistent":
        run_results = _run_persistent(config, samples, pending, n_workers)
    else:
        run_results = _run_rolling(config, samples, pending, n_workers)

    # ---- Append to run log -----------------------------------------------
    _append_run_log(run_results, config.results_dir / "run_log.csv")
    logger.info("Run log updated: %s", config.results_dir / "run_log.csv")

    return _load_run_log(config.results_dir)


# ---------------------------------------------------------------------------
# Rolling strategy (original approach)
# ---------------------------------------------------------------------------


def _run_rolling(
    config: SimulationConfig,
    samples: pd.DataFrame,
    pending: list[int],
    n_workers: int,
) -> list[dict[str, Any]]:
    """Execute pending simulations with the rolling per-sim copy strategy."""
    source_str = str(config.source_txtinout)
    work_str = str(config.work_dir)
    results_str = str(config.results_dir)
    params_dicts = [p.to_dict() for p in config.parameters]

    run_results: list[dict[str, Any]] = []
    completed = 0
    total = len(pending)

    # Submit in rolling batches of max_workers so that only n_workers TxtInOut
    # copies exist on disk simultaneously.
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        pending_iter = iter(pending)
        active: dict = {}

        def _submit_next() -> None:
            sim_id = next(pending_iter, None)
            if sim_id is None:
                return
            f = executor.submit(
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
                reach_filter=config.reach_filter,
            )
            active[f] = sim_id

        for _ in range(n_workers):
            _submit_next()

        while active:
            for future in as_completed(active):
                result = future.result()
                del active[future]
                run_results.append(result)
                completed += 1
                status_flag = "✓" if result["status"] == "completed" else "✗"
                logger.info(
                    "[%d/%d] sim_%04d %s %s (%.1fs)%s",
                    completed, total, result["sim_id"],
                    status_flag, result["status"], result["duration_s"],
                    f" — {result['error']}" if result["error"] else "",
                )
                _submit_next()
                break  # re-enter as_completed with updated active dict

    return run_results


# ---------------------------------------------------------------------------
# Persistent strategy (new approach)
# ---------------------------------------------------------------------------


def _run_persistent(
    config: SimulationConfig,
    samples: pd.DataFrame,
    pending: list[int],
    n_workers: int,
) -> list[dict[str, Any]]:
    """Execute pending simulations with the persistent worker-copy strategy.

    Creates exactly *n_workers* TxtInOut copies.  Simulations are divided into
    contiguous chunks; each worker runs its chunk sequentially in-place.
    Worker copies are deleted after all batches complete (respects
    ``config.delete_run_dirs``).
    """
    params_dicts = [p.to_dict() for p in config.parameters]
    results_str = str(config.results_dir)

    # ---- Create worker directories ---------------------------------------
    worker_dirs: list[Path] = []
    logger.info("Creating %d persistent worker directories …", n_workers)
    for i in range(n_workers):
        wdir = config.work_dir / f"worker_{i:02d}"
        if wdir.exists():
            shutil.rmtree(wdir)
        shutil.copytree(config.source_txtinout, wdir)
        worker_dirs.append(wdir)
    logger.info("Worker directories ready.")

    # ---- Split pending sims into contiguous chunks ----------------------
    chunk_size = math.ceil(len(pending) / n_workers)
    chunks: list[list[int]] = [
        pending[i * chunk_size: (i + 1) * chunk_size]
        for i in range(n_workers)
    ]
    chunks = [c for c in chunks if c]  # drop empty tail chunks

    run_results: list[dict[str, Any]] = []
    completed = 0
    total = len(pending)

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(
                _run_batch,
                worker_id=i,
                sim_ids=chunk,
                sample_rows={sid: samples.loc[sid].to_dict() for sid in chunk},
                worker_dir=str(wdir),
                results_dir=results_str,
                swat_exe_name=config.swat_exe_name,
                output_files=config.output_files,
                parameters_dicts=params_dicts,
                timeout=config.timeout,
                reach_filter=config.reach_filter,
            ): i
            for i, (wdir, chunk) in enumerate(zip(worker_dirs, chunks))
        }

        for future in as_completed(futures):
            worker_id = futures[future]
            batch_results = future.result()
            run_results.extend(batch_results)
            completed += len(batch_results)
            logger.info(
                "Worker %d finished: %d/%d simulations complete",
                worker_id, completed, total,
            )

    # ---- Clean up worker directories ------------------------------------
    if config.delete_run_dirs:
        for wdir in worker_dirs:
            shutil.rmtree(wdir, ignore_errors=True)
        logger.info("Worker directories deleted.")
    else:
        logger.info(
            "Worker directories retained (delete_run_dirs=False): %s",
            config.work_dir,
        )

    return run_results


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
    """Load the run_log.csv if it exists, or return an empty DataFrame.

    Deduplicates by sim_id keeping the last entry so that resumed runs
    supersede earlier failed/error entries for the same simulation.
    """
    log_path = results_dir / "run_log.csv"
    if log_path.exists():
        df = pd.read_csv(log_path)
        df["sim_id"] = df["sim_id"].astype(int)
        df = df.drop_duplicates(subset="sim_id", keep="last")
        return df.set_index("sim_id")
    return pd.DataFrame(columns=["status", "duration_s", "error"])
