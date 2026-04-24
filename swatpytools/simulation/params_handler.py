"""Apply SWAT parameter changes to a simulation directory.

Handles both pipe-delimited parameter files (.hru, .mgt, .gw, .rte, etc.)
via :mod:`swatpytools.inputs.params`, and the array-format .sol file via a
dedicated regex-based parser.

Change methods
--------------
``"v"`` (value / absolute replacement)
    The sampled value is written directly, clamped to ``[abs_min, abs_max]``.

``"r"`` (relative change)
    ``new_value = (1 + sample) * initial_value``, clamped to bounds.
    The initial value is read from the *current* file contents (which, in a
    fresh simulation copy, equals the source TxtInOut value).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from ..inputs.params import batch_update, read_param_file, write_param_file

if TYPE_CHECKING:
    from .config import ParameterSpec

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# .sol file helpers
# ---------------------------------------------------------------------------

# SWAT labels that appear on the left side of ":" in .sol files.
# Keys are the canonical SWAT parameter name (uppercase).
_SOL_LABEL_MAP: dict[str, str] = {
    "SOL_AWC": "Ave. AW Incl. Rock Frag",
    "SOL_K":   "Ksat. (est.)",
    "SOL_BD":  "Bulk Density Moist",
    "SOL_CBN": "Organic Carbon",
    "CLAY":    "Clay",
    "SILT":    "Silt",
    "SAND":    "Sand",
    "ROCK":    "Rock Fragments",
    "SOL_ALB": "Soil Albedo (Moist)",
    "USLE_K":  "Erosion K",
    "SOL_EC":  "Salinity (EC, Form 5)",
    "SOL_CAL": "Soil CACO3",
    "SOL_PH":  "Soil pH",
}

_NUMBER_RE = re.compile(r"-?\d+\.?\d*")


def _is_water_sol(lines: list[str]) -> bool:
    """Return True if this .sol file represents a water body or dummy HRU.

    Water-body HRUs use ``WATER`` or ``DUMMY`` as the soil texture identifier.
    Modifying their parameters would corrupt the SWAT model, so they must be
    skipped.
    """
    for line in lines[:15]:
        # Split each word and check for exact matches to avoid false positives
        # (e.g. "WETLAND" containing "WET")
        words = line.upper().split()
        if "WATER" in words or "DUMMY" in words:
            return True
    return False


def _find_sol_param_line(lines: list[str], param_name: str) -> int:
    """Return the index of the line containing *param_name* in a .sol file.

    Searches for the canonical label from :data:`_SOL_LABEL_MAP` using a
    case-insensitive prefix match on the left side of the colon.  A prefix
    match is used because ArcSWAT appends units in brackets after some labels
    (e.g. ``"Bulk Density Moist [g/cc]:"``).

    Returns -1 if not found.
    """
    label = _SOL_LABEL_MAP.get(param_name.upper(), param_name.upper()).upper()
    for i, line in enumerate(lines):
        left = line.split(":", 1)[0].strip().upper()
        if left.startswith(label):
            return i
    return -1


def read_sol_param_layer1(sol_path: Path, param_name: str) -> float:
    """Read the first-layer value of a SWAT soil array parameter.

    Args:
        sol_path: Path to the ``.sol`` file.
        param_name: Parameter name, e.g. ``"SOL_AWC"``.

    Returns:
        Numeric value of the first soil layer.

    Raises:
        ValueError: If the parameter is not found in the file.
    """
    lines = sol_path.read_text().splitlines()
    idx = _find_sol_param_line(lines, param_name)
    if idx < 0:
        raise ValueError(f"{param_name} not found in {sol_path.name}")
    after_colon = lines[idx].split(":", 1)[1] if ":" in lines[idx] else lines[idx]
    m = _NUMBER_RE.search(after_colon)
    if not m:
        raise ValueError(
            f"No numeric value for {param_name} in {sol_path.name} "
            f"(line {idx}: {lines[idx].rstrip()})"
        )
    return float(m.group())


def write_sol_param_layer1(sol_path: Path, param_name: str, new_value: float) -> None:
    """Overwrite the first-layer value of a SWAT soil array parameter in-place.

    Preserves all other layer values and the original number of decimal places.

    Args:
        sol_path: Path to the ``.sol`` file.
        param_name: Parameter name, e.g. ``"SOL_AWC"``.
        new_value: New value to write for the first layer.
    """
    lines = sol_path.read_text().splitlines(keepends=True)
    idx = _find_sol_param_line([l.rstrip("\n") for l in lines], param_name)
    if idx < 0:
        logger.warning("write_sol_param_layer1: %s not found in %s — skipped",
                       param_name, sol_path.name)
        return

    line = lines[idx]
    colon_pos = line.index(":")
    before = line[: colon_pos + 1]
    after = line[colon_pos + 1 :]

    m = _NUMBER_RE.search(after)
    if not m:
        return

    orig_str = m.group()
    # Preserve original decimal precision
    decimals = len(orig_str.split(".")[1]) if "." in orig_str else 3
    new_str = f"{new_value:.{decimals}f}"

    # Replace only the first numeric match
    new_after = after[: m.start()] + new_str + after[m.end() :]
    lines[idx] = before + new_after
    sol_path.write_text("".join(lines))


# ---------------------------------------------------------------------------
# Core application logic
# ---------------------------------------------------------------------------


def apply_sample_to_dir(
    run_dir: Path,
    sample_row: dict[str, float],
    parameters: list["ParameterSpec"],
) -> None:
    """Apply one row of sampled parameter values to a simulation directory.

    ``run_dir`` must be a **fresh copy** of the source TxtInOut so that
    initial values (used by the ``"r"`` method) can be read directly from
    the files in place.

    Args:
        run_dir: Per-simulation TxtInOut directory (already copied from source).
        sample_row: Mapping of ``parameter.identifier`` → sampled value.
        parameters: List of :class:`~.config.ParameterSpec` objects in the
            same order as the columns of the sample DataFrame.
    """
    # Group 'v' method params by extension for bulk batch_update calls
    v_params_by_ext: dict[str, dict[str, float]] = {}

    for param in parameters:
        value = sample_row[param.identifier]

        if param.extension == "sol":
            _apply_sol(run_dir, param, value)
            continue

        if param.method == "v":
            clamped = float(np.clip(value, param.abs_min, param.abs_max))
            v_params_by_ext.setdefault(param.extension, {})[param.name] = clamped
        else:  # 'r'
            _apply_relative_nonsol(run_dir, param, value)

    # Flush grouped absolute-value updates (one batch_update call per extension)
    for ext, updates in v_params_by_ext.items():
        try:
            batch_update(run_dir, ext, updates)
        except FileNotFoundError:
            logger.warning(
                "No .%s files found in %s — parameters %s skipped",
                ext,
                run_dir.name,
                list(updates.keys()),
            )


def _apply_relative_nonsol(
    run_dir: Path, param: "ParameterSpec", factor: float
) -> None:
    """Apply relative change to every file matching *param.extension*."""
    files = sorted(run_dir.glob(f"*.{param.extension}"))
    if not files:
        logger.warning(
            "No .%s files found for %s — skipped", param.extension, param.identifier
        )
        return

    for fpath in files:
        try:
            current = read_param_file(fpath)
        except Exception as exc:
            logger.warning("Could not read %s: %s", fpath.name, exc)
            continue

        init_val = current.get(param.name)
        if init_val is None:
            logger.debug("%s not found in %s — skipped", param.name, fpath.name)
            continue

        new_val = float(np.clip((1.0 + factor) * float(init_val), param.abs_min, param.abs_max))
        try:
            write_param_file(fpath, {param.name: new_val})
        except Exception as exc:
            logger.warning("Could not write %s in %s: %s", param.name, fpath.name, exc)


def _apply_sol(run_dir: Path, param: "ParameterSpec", value: float) -> None:  # used by rolling strategy
    """Apply a parameter change to all ``.sol`` files in *run_dir*."""
    sol_files = sorted(run_dir.glob("*.sol"))
    if not sol_files:
        logger.warning("No .sol files found in %s", run_dir.name)
        return

    for sol_path in sol_files:
        lines = sol_path.read_text().splitlines()
        if _is_water_sol(lines):
            logger.debug("Skipping water/DUMMY soil: %s", sol_path.name)
            continue

        try:
            if param.method == "v":
                new_val = float(np.clip(value, param.abs_min, param.abs_max))
            else:  # 'r'
                init_val = read_sol_param_layer1(sol_path, param.name)
                new_val = float(
                    np.clip((1.0 + value) * init_val, param.abs_min, param.abs_max)
                )
            write_sol_param_layer1(sol_path, param.name, new_val)
        except Exception as exc:
            logger.warning("Could not update %s in %s: %s", param.name, sol_path.name, exc)


# ---------------------------------------------------------------------------
# Persistent-worker strategy helpers
# ---------------------------------------------------------------------------


def read_baselines(
    run_dir: Path,
    parameters: list["ParameterSpec"],
) -> dict[str, dict[str, float]]:
    """Pre-read initial parameter values from a worker directory.

    Called **once** per worker copy before its batch of simulations starts.
    Only relative (``method='r'``) parameters need baselines; absolute
    (``method='v'``) parameters are stored directly from the sample.

    Args:
        run_dir: Worker TxtInOut directory (already copied from source).
        parameters: List of :class:`~.config.ParameterSpec` objects.

    Returns:
        Nested dict ``{param_identifier: {filename: initial_value}}``.
        Only populated for ``method='r'`` parameters.
    """
    baselines: dict[str, dict[str, float]] = {}

    for param in parameters:
        if param.method != "r":
            continue

        per_file: dict[str, float] = {}

        if param.extension == "sol":
            for sol_path in sorted(run_dir.glob("*.sol")):
                lines = sol_path.read_text().splitlines()
                if _is_water_sol(lines):
                    continue
                try:
                    per_file[sol_path.name] = read_sol_param_layer1(sol_path, param.name)
                except Exception as exc:
                    logger.warning(
                        "read_baselines: could not read %s from %s: %s",
                        param.name, sol_path.name, exc,
                    )
        else:
            for fpath in sorted(run_dir.glob(f"*.{param.extension}")):
                try:
                    current = read_param_file(fpath)
                    val = current.get(param.name)
                    if val is not None:
                        per_file[fpath.name] = float(val)
                except Exception as exc:
                    logger.warning(
                        "read_baselines: could not read %s from %s: %s",
                        param.name, fpath.name, exc,
                    )

        baselines[param.identifier] = per_file

    return baselines


def apply_sample_to_dir_inplace(
    run_dir: Path,
    sample_row: dict[str, float],
    parameters: list["ParameterSpec"],
    baselines: dict[str, dict[str, float]],
) -> None:
    """Apply one sample to a worker directory using pre-read baselines.

    Used by the ``"persistent"`` strategy.  Unlike :func:`apply_sample_to_dir`,
    relative parameters are computed from *baselines* rather than from the
    current file state, so successive simulations in the same directory do not
    compound changes.

    Args:
        run_dir: Worker TxtInOut directory (modified in-place).
        sample_row: ``{param.identifier: sampled_value}`` for this simulation.
        parameters: Parameter specs in the same order as the sample DataFrame.
        baselines: Pre-read initial values from :func:`read_baselines`.
    """
    v_params_by_ext: dict[str, dict[str, float]] = {}

    for param in parameters:
        value = sample_row[param.identifier]

        if param.extension == "sol":
            _apply_sol_inplace(run_dir, param, value, baselines.get(param.identifier, {}))
            continue

        if param.method == "v":
            clamped = float(np.clip(value, param.abs_min, param.abs_max))
            v_params_by_ext.setdefault(param.extension, {})[param.name] = clamped
        else:  # 'r'
            _apply_relative_inplace(
                run_dir, param, value, baselines.get(param.identifier, {})
            )

    for ext, updates in v_params_by_ext.items():
        try:
            batch_update(run_dir, ext, updates)
        except FileNotFoundError:
            logger.warning(
                "No .%s files found in %s — parameters %s skipped",
                ext, run_dir.name, list(updates.keys()),
            )


def _apply_relative_inplace(
    run_dir: Path,
    param: "ParameterSpec",
    factor: float,
    file_baselines: dict[str, float],
) -> None:
    """Write ``(1 + factor) * baseline`` to every matching file, using stored baselines."""
    files = sorted(run_dir.glob(f"*.{param.extension}"))
    if not files:
        logger.warning(
            "No .%s files found for %s — skipped", param.extension, param.identifier
        )
        return

    for fpath in files:
        init_val = file_baselines.get(fpath.name)
        if init_val is None:
            logger.debug("%s not in baselines for %s — skipped", param.name, fpath.name)
            continue
        new_val = float(np.clip((1.0 + factor) * init_val, param.abs_min, param.abs_max))
        try:
            write_param_file(fpath, {param.name: new_val})
        except Exception as exc:
            logger.warning(
                "Could not write %s in %s: %s", param.name, fpath.name, exc
            )


def _apply_sol_inplace(
    run_dir: Path,
    param: "ParameterSpec",
    value: float,
    file_baselines: dict[str, float],
) -> None:
    """Apply a .sol parameter using stored baselines (persistent strategy)."""
    for sol_path in sorted(run_dir.glob("*.sol")):
        lines = sol_path.read_text().splitlines()
        if _is_water_sol(lines):
            logger.debug("Skipping water/DUMMY soil: %s", sol_path.name)
            continue
        try:
            if param.method == "v":
                new_val = float(np.clip(value, param.abs_min, param.abs_max))
            else:  # 'r'
                init_val = file_baselines.get(sol_path.name)
                if init_val is None:
                    logger.debug(
                        "%s not in baselines for %s — skipped", param.name, sol_path.name
                    )
                    continue
                new_val = float(np.clip((1.0 + value) * init_val, param.abs_min, param.abs_max))
            write_sol_param_layer1(sol_path, param.name, new_val)
        except Exception as exc:
            logger.warning(
                "Could not update %s in %s: %s", param.name, sol_path.name, exc
            )
