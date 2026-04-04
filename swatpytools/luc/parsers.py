"""Parsers for SWAT project files: HRU definitions, HRU report, and lookup table."""

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import logging

logger = logging.getLogger(__name__)


@dataclass
class HRUDefinition:
    """Definition of a single HRU parsed from SWAT project files."""

    hru_id: int          # Watershed-level HRU number (1-164)
    subbasin: int        # Subbasin number (1-25)
    hru_in_sub: int      # HRU number within subbasin
    landuse: str         # SWAT land use code (e.g., FRSD, AGRL)
    soil: str            # Soil type ID (e.g., 570620)
    slope: str           # Slope class string (e.g., "0-2", "2-6", "6-9999")
    hru_fr: float        # Fractional area within subbasin
    area_ha: float = 0.0  # Area in hectares (from HRU report)
    filename: str = ""   # Base filename (e.g., "000010001")


# Regex for .hru file header line
_HRU_HEADER_RE = re.compile(
    r"HRU:(\d+)\s+Subbasin:(\d+)\s+HRU:(\d+)\s+Luse:(\w+)\s+Soil:\s*(\S+)\s+Slope:\s*([\d\-]+)"
)

# Regex for HRU_FR value line
_HRU_FR_RE = re.compile(r"^\s*([\d.]+)\s*\|\s*HRU_FR")

# Regex for HRU lines in HRULandUseSoilsReport.txt
_REPORT_HRU_RE = re.compile(
    r"^\s*(\d+)\s+.*-->\s+(\w+)/(\w+)/([\d\-]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)"
)

# Simpler fallback regex for report HRU lines
_REPORT_HRU_SIMPLE_RE = re.compile(
    r"^\s*(\d+)\s+.*-->\s+(\w+)/(\w+)/([\d\-]+)\s+([\d.]+)"
)

# Regex for subbasin header in report
_REPORT_SUBBASIN_RE = re.compile(
    r"SUBBASIN\s*#\s+(\d+)\s+([\d.]+)"
)


def parse_hru_files(txtinout_dir: Path) -> List[HRUDefinition]:
    """Parse all .hru files in TxtInOut to extract HRU definitions.

    Returns a list of HRUDefinition sorted by hru_id.
    """
    txtinout_dir = Path(txtinout_dir)
    hru_files = sorted(txtinout_dir.glob("0*.hru"))

    hrus = []
    for hru_file in hru_files:
        if hru_file.stem == "output":
            continue

        try:
            with open(hru_file, "r") as f:
                header = f.readline()
                fr_line = f.readline()
        except Exception as e:
            logger.warning(f"Failed to read {hru_file}: {e}")
            continue

        header_match = _HRU_HEADER_RE.search(header)
        if not header_match:
            logger.warning(f"Could not parse header in {hru_file.name}")
            continue

        fr_match = _HRU_FR_RE.search(fr_line)
        if not fr_match:
            logger.warning(f"Could not parse HRU_FR in {hru_file.name}")
            continue

        hru = HRUDefinition(
            hru_id=int(header_match.group(1)),
            subbasin=int(header_match.group(2)),
            hru_in_sub=int(header_match.group(3)),
            landuse=header_match.group(4),
            soil=header_match.group(5),
            slope=header_match.group(6),
            hru_fr=float(fr_match.group(1)),
            filename=hru_file.stem,
        )
        hrus.append(hru)

    hrus.sort(key=lambda h: h.hru_id)
    logger.info(f"Parsed {len(hrus)} HRU files from {txtinout_dir}")
    return hrus


def parse_hru_report(report_path: Path) -> List[HRUDefinition]:
    """Parse HRULandUseSoilsReport.txt for HRU area data.

    Returns a list of HRUDefinition with area_ha populated.
    """
    report_path = Path(report_path)
    hrus = []
    current_subbasin = 0
    current_subbasin_area = 0.0

    with open(report_path, "r") as f:
        for line in f:
            # Check for subbasin header
            sub_match = _REPORT_SUBBASIN_RE.search(line)
            if sub_match:
                current_subbasin = int(sub_match.group(1))
                current_subbasin_area = float(sub_match.group(2))
                continue

            # Check for HRU line (try detailed regex first, then simple)
            hru_match = _REPORT_HRU_SIMPLE_RE.search(line)
            if hru_match and current_subbasin > 0:
                # Check it's actually an HRU line (has --> pattern)
                if "-->" not in line:
                    continue

                hru_id = int(hru_match.group(1))
                landuse = hru_match.group(2)
                soil = hru_match.group(3)
                slope = hru_match.group(4)
                area_ha = float(hru_match.group(5))

                # Determine hru_in_sub from count of HRUs in this subbasin so far
                hru_in_sub = sum(1 for h in hrus if h.subbasin == current_subbasin) + 1

                hru = HRUDefinition(
                    hru_id=hru_id,
                    subbasin=current_subbasin,
                    hru_in_sub=hru_in_sub,
                    landuse=landuse,
                    soil=soil,
                    slope=slope,
                    hru_fr=area_ha / current_subbasin_area if current_subbasin_area > 0 else 0.0,
                    area_ha=area_ha,
                )
                hrus.append(hru)

    hrus.sort(key=lambda h: h.hru_id)
    logger.info(f"Parsed {len(hrus)} HRU entries from report")
    return hrus


def parse_lookup_table(path: Path) -> Dict[int, str]:
    """Parse NLCD-to-SWAT land use code lookup table.

    Expected format (CSV with header):
        "Value","Landuse"
        11,WATR
        21,URLD
        ...

    Returns dict mapping NLCD code (int) to SWAT code (str).
    """
    path = Path(path)
    lookup = {}

    with open(path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) >= 2:
                try:
                    nlcd_code = int(row[0].strip().strip('"'))
                    swat_code = row[1].strip().strip('"')
                    lookup[nlcd_code] = swat_code
                except ValueError:
                    continue

    logger.info(f"Loaded {len(lookup)} NLCD-to-SWAT mappings from {path}")
    return lookup


def build_subbasin_index(hrus: List[HRUDefinition]) -> Dict[int, List[HRUDefinition]]:
    """Group HRUs by subbasin number.

    Returns dict mapping subbasin number to list of HRUDefinitions.
    """
    index = {}
    for hru in hrus:
        index.setdefault(hru.subbasin, []).append(hru)
    return index


def build_match_indexes(hrus_in_subbasin: List[HRUDefinition]):
    """Build lookup indexes for the redistribution matching algorithm.

    Returns three dicts for cascading match levels:
        by_lulc_soil_slope: (lulc, soil, slope) -> [HRUDefinition, ...]
        by_lulc_soil: (lulc, soil) -> [HRUDefinition, ...]
        by_lulc: lulc -> [HRUDefinition, ...]
    """
    by_lulc_soil_slope = {}
    by_lulc_soil = {}
    by_lulc = {}

    for hru in hrus_in_subbasin:
        key3 = (hru.landuse, hru.soil, hru.slope)
        by_lulc_soil_slope.setdefault(key3, []).append(hru)

        key2 = (hru.landuse, hru.soil)
        by_lulc_soil.setdefault(key2, []).append(hru)

        by_lulc.setdefault(hru.landuse, []).append(hru)

    return by_lulc_soil_slope, by_lulc_soil, by_lulc
