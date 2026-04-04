"""Writers for SWAT LUC output files (lup.dat and fileN.dat).

lup.dat format (schedule file):
    Each line: UPDATE_NUM  MONTH  DAY  YEAR  FILENAME
    Example:
        1    1    1 2011 file1.dat
        2    1    1 2016 file2.dat

fileN.dat format (HRU fractions):
    Header: HRU_ID, HRU_AREA
    Data: HRU_ID,  HRU_FR
    Example:
        HRU_ID, HRU_AREA
        1,  0.17148
        2,  0.051052
"""

import logging
from pathlib import Path
from typing import List

from .parsers import HRUDefinition
from .redistribute import RedistributionResult

logger = logging.getLogger(__name__)


def write_lup_dat(
    output_path: Path,
    results: List[RedistributionResult],
    hrus: List[HRUDefinition],
    style: str = "swat2012",
):
    """Write lup.dat schedule file and corresponding fileN.dat fraction files.

    Args:
        output_path: Path to the output directory.
        results: List of RedistributionResult (one per update year), sorted by date.
        hrus: All HRU definitions (sorted by hru_id).
        style: Ignored (kept for API compatibility). Always writes separate files.
    """
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    hrus_sorted = sorted(hrus, key=lambda h: h.hru_id)
    lup_path = output_path / "lup.dat"

    with open(lup_path, "w") as f:
        for i, result in enumerate(results):
            update_num = i + 1
            file_name = f"file{update_num}.dat"

            # Format: UPDATE_NUM  MONTH  DAY  YEAR  FILENAME
            f.write(
                f"{update_num:5d}{result.month:5d}{result.day:5d}"
                f" {result.year} {file_name}\n"
            )

            # Write the corresponding fileN.dat
            _write_file_dat(output_path / file_name, result, hrus_sorted)

    logger.info(f"Wrote {lup_path} ({len(results)} updates)")
    logger.info(f"Wrote {len(results)} fileN.dat files to {output_path}")


def _write_file_dat(
    file_path: Path,
    result: RedistributionResult,
    hrus_sorted: List[HRUDefinition],
):
    """Write a single fileN.dat with updated HRU fractions.

    Format:
        HRU_ID, HRU_AREA
        1,  0.17148
        2,  0.051052
        ...
    """
    with open(file_path, "w") as f:
        f.write("HRU_ID, HRU_FR\n")
        for hru in hrus_sorted:
            hru_fr = result.hru_fractions.get(hru.hru_id, hru.hru_fr)
            f.write(f"{hru.hru_id},  {hru_fr:.6g}\n")

    logger.info(f"Wrote {file_path}")
