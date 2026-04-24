"""SWAT parameter file reader, writer, and batch updater.

Handles the fixed-format `value | PARAM_NAME : description` layout used in
SWAT .hru, .mgt, .gw, .rte, .sub, .pnd, .sep files.

The .sol file uses a different array-based format and is not supported here.

File format
-----------
Each data line follows the pattern (right-aligned value, then `|` separator):
    <value>    | PARAM_NAME : Description text

This module preserves the exact column widths and formatting of the original
file, replacing only the numeric value portion left of the `|`.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Regex: capture (value_field)(pipe)(rest_of_line)
_LINE_RE = re.compile(r"^([^|]+)(\|)(.+)$")
# Extract param name from the right side: up to the first colon or end
_PARAM_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_#()]*)")


def read_param_file(path: str | Path) -> dict[str, Any]:
    """Read a SWAT parameter file into a dict of {param_name: value}.

    Skips comment/header lines (those without a `|` separator).
    Values are returned as int if they contain no decimal point, else float.

    Args:
        path: Path to the SWAT parameter file (.hru, .mgt, .gw, etc.)

    Returns:
        Dict mapping uppercase parameter names to their numeric values.
    """
    path = Path(path)
    params = {}
    with path.open() as f:
        for line in f:
            m = _LINE_RE.match(line.rstrip("\n"))
            if not m:
                continue
            value_str = m.group(1).strip()
            rest = m.group(3)
            name_m = _PARAM_RE.match(rest)
            if not name_m:
                continue
            param_name = name_m.group(1).upper()
            try:
                value = int(value_str) if "." not in value_str else float(value_str)
            except ValueError:
                value = value_str  # keep as string if unparseable
            params[param_name] = value
    return params


def write_param_file(
    path: str | Path,
    updates: dict[str, Any],
    *,
    inplace: bool = True,
    output_path: str | Path | None = None,
) -> Path:
    """Write updated parameter values back to a SWAT parameter file.

    Preserves all whitespace, column widths, header lines, and comments.
    Only the numeric value portion (left of `|`) is changed.

    Args:
        path: Path to the source SWAT parameter file.
        updates: Dict of {param_name: new_value}. Keys are case-insensitive.
        inplace: If True (default), overwrite the source file.
        output_path: Write to this path instead (overrides inplace).

    Returns:
        Path to the written file.
    """
    path = Path(path)
    updates_upper = {k.upper(): v for k, v in updates.items()}
    out_path = path if inplace else Path(output_path or path)

    lines_out = []
    with path.open() as f:
        for line in f:
            stripped = line.rstrip("\n")
            m = _LINE_RE.match(stripped)
            if m:
                rest = m.group(3)
                name_m = _PARAM_RE.match(rest)
                if name_m:
                    param_name = name_m.group(1).upper()
                    if param_name in updates_upper:
                        new_val = updates_upper[param_name]
                        old_field = m.group(1)  # preserve field width
                        new_field = _format_value(new_val, old_field)
                        stripped = new_field + "|" + rest
                        logger.debug("Updated %s → %s", param_name, new_val)
            lines_out.append(stripped + "\n")

    out_path.write_text("".join(lines_out))
    return out_path


def update_param(
    path: str | Path,
    param_name: str,
    new_value: Any,
    *,
    inplace: bool = True,
    output_path: str | Path | None = None,
) -> Path:
    """Update a single parameter in a SWAT file. Convenience wrapper.

    Args:
        path: Path to the SWAT parameter file.
        param_name: Parameter name (case-insensitive).
        new_value: New numeric value.
        inplace: Overwrite the source file (default True).
        output_path: Write to this path instead.

    Returns:
        Path to the written file.
    """
    return write_param_file(
        path,
        {param_name: new_value},
        inplace=inplace,
        output_path=output_path,
    )


def get_hrus_by_landuse(
    txtinout_dir: str | Path,
    landuse_codes: list[str],
) -> list[int]:
    """Return HRU numbers whose land use class matches any of *landuse_codes*.

    Reads the header line of every ``.hru`` file in *txtinout_dir* and
    extracts the ``Luse:XXXX`` token written by ArcSWAT.  Matching is
    case-insensitive.

    Args:
        txtinout_dir: Path to the TxtInOut directory.
        landuse_codes: Land use class codes to match, e.g. ``["AGRR"]``.

    Returns:
        Sorted list of integer HRU numbers (last 4 digits of the file stem)
        for all HRU files whose land use matches.

    Example::

        hru_ids = get_hrus_by_landuse("./TxtInOut", ["AGRR"])
        batch_update("./TxtInOut", "mgt", {"TDRAIN": 1}, hru_filter=hru_ids)
    """
    txtinout_dir = Path(txtinout_dir)
    target = {c.upper() for c in landuse_codes}
    _luse_re = re.compile(r"Luse:(\w+)", re.IGNORECASE)

    matched: list[int] = []
    for hru_file in sorted(txtinout_dir.glob("*.hru")):
        with hru_file.open() as f:
            header = f.readline()
        m = _luse_re.search(header)
        if m and m.group(1).upper() in target:
            matched.append(int(hru_file.stem[-4:]))

    logger.info(
        "get_hrus_by_landuse(%s): %d HRU(s) matched", landuse_codes, len(matched)
    )
    return matched


def batch_update(
    txtinout_dir: str | Path,
    extension: str,
    updates: dict[str, Any],
    *,
    hru_filter: list[int] | None = None,
    landuse_filter: list[str] | None = None,
    dry_run: bool = False,
) -> list[Path]:
    """Apply parameter updates to all SWAT files with the given extension.

    Args:
        txtinout_dir: Path to the TxtInOut directory.
        extension: File extension without dot, e.g. 'hru', 'mgt', 'gw'.
        updates: Dict of {param_name: new_value} to apply.
        hru_filter: If provided, only update files whose HRU number (last 4
            digits of the stem) is in this list.
        landuse_filter: If provided, only update files whose land use class
            (from the ``.hru`` header ``Luse:XXXX`` token) matches any of
            these codes, e.g. ``["AGRR"]``.  Case-insensitive.  When both
            *hru_filter* and *landuse_filter* are given, only HRUs that
            satisfy both are updated (intersection).
        dry_run: If True, log what would change but don't write any files.

    Returns:
        List of paths that were (or would be) updated.
    """
    txtinout_dir = Path(txtinout_dir)
    ext = extension.lstrip(".")
    files = sorted(txtinout_dir.glob(f"*.{ext}"))

    if not files:
        raise FileNotFoundError(
            f"No .{ext} files found in {txtinout_dir}"
        )

    if landuse_filter is not None:
        luse_hrus = set(get_hrus_by_landuse(txtinout_dir, landuse_filter))
        hru_filter = sorted(luse_hrus & set(hru_filter)) if hru_filter is not None else sorted(luse_hrus)

    if hru_filter is not None:
        hru_set = set(hru_filter)
        files = [f for f in files if int(f.stem[-4:]) in hru_set]

    updated = []
    for fpath in files:
        if dry_run:
            logger.info("[dry-run] Would update %s: %s", fpath.name, updates)
        else:
            write_param_file(fpath, updates, inplace=True)
            logger.debug("Updated %s", fpath.name)
        updated.append(fpath)

    if not dry_run:
        logger.info("Updated %d .%s files", len(updated), ext)
    return updated


def _format_value(value: Any, original_field: str) -> str:
    """Format a new value to fit within the original field width.

    Preserves the original field width (number of characters including
    leading/trailing spaces). The value is right-aligned within that width.
    """
    field_width = len(original_field)

    if isinstance(value, float):
        # Determine original decimal places from the existing field
        orig_stripped = original_field.strip()
        if "." in orig_stripped:
            orig_decimals = len(orig_stripped.split(".")[-1])
        else:
            orig_decimals = 6
        formatted = f"{value:.{orig_decimals}f}"
    elif isinstance(value, int):
        formatted = str(value)
    else:
        formatted = str(value)

    # Right-align value in (field_width - 4) chars, then append 4 trailing spaces.
    # The 4 trailing spaces are critical: they separate the value from the '|'
    # pipe character that follows, which SWAT's Fortran list-directed I/O reader
    # cannot parse as a numeric separator if it immediately follows the value.
    return formatted.rjust(field_width - 4) + "    "
