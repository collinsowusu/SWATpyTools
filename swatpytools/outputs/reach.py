"""Parser for SWAT output.rch (reach/streamflow output).

Reads the fixed-format output.rch file produced by SWAT into a tidy DataFrame.
Supports optional time-indexing and monthly/annual aggregation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Column names matching SWAT output.rch format (62 data cols after the row label)
_RCH_COLS = [
    "RCH", "GIS", "MON", "AREA_km2",
    "FLOW_IN", "FLOW_OUT", "EVAP", "TLOSS",
    "SED_IN", "SED_OUT", "SEDCONC",
    "ORGN_IN", "ORGN_OUT", "ORGP_IN", "ORGP_OUT",
    "NO3_IN", "NO3_OUT", "NH4_IN", "NH4_OUT",
    "NO2_IN", "NO2_OUT", "MINP_IN", "MINP_OUT",
    "CHLA_IN", "CHLA_OUT", "CBOD_IN", "CBOD_OUT",
    "DISOX_IN", "DISOX_OUT",
    "SOLPST_IN", "SOLPST_OUT", "SORPST_IN", "SORPST_OUT",
    "REACTPST", "VOLPST", "SETTLPST", "RESUSP_PST",
    "DIFFUSEPST", "REACBEDPST", "BURYPST", "BED_PST",
    "BACTP_OUT", "BACTLP_OUT",
    "CMETAL1", "CMETAL2", "CMETAL3",
    "TOT_N", "TOT_P", "NO3CONC", "WTMP",
    "SALT1", "SALT2", "SALT3", "SALT4", "SALT5",
    "SALT6", "SALT7", "SALT8", "SALT9", "SALT10",
    "SAR", "EC",
]

# Units for common variables (for display/labelling)
UNITS = {
    "FLOW_IN": "m³/s", "FLOW_OUT": "m³/s", "EVAP": "m³/s", "TLOSS": "m³/s",
    "SED_IN": "tons", "SED_OUT": "tons", "SEDCONC": "mg/L",
    "ORGN_IN": "kg", "ORGN_OUT": "kg", "ORGP_IN": "kg", "ORGP_OUT": "kg",
    "NO3_IN": "kg", "NO3_OUT": "kg", "TOT_N": "kg", "TOT_P": "kg",
    "NO3CONC": "mg/L", "WTMP": "°C",
}


def read_reach(
    path: str | Path,
    reach: int | list[int] | None = None,
    start_date: str | None = None,
    nyskip: int = 0,
    annual: bool = False,
) -> pd.DataFrame:
    """Read SWAT output.rch into a DataFrame.

    Args:
        path: Path to output.rch file.
        reach: Reach number(s) to return. None = all reaches.
        start_date: Simulation start date as 'YYYY-MM-DD'. If provided, a DATE
            column is added (monthly frequency, skipping warmup years).
        nyskip: Number of warmup years to skip when building dates (requires
            start_date). Must match the SWAT NYSKIP setting.
        annual: If True, return only annual summary rows (MON == 0).
            If False (default), return only monthly rows (MON 1-12).

    Returns:
        DataFrame with columns from _RCH_COLS, filtered and optionally dated.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"output.rch not found: {path}")

    logger.info("Reading %s", path)

    # Row label ("REACH") + 62 data columns = 63 tokens per row
    col_names = ["_label"] + _RCH_COLS
    df = pd.read_csv(
        path,
        sep=r"\s+",
        skiprows=9,
        names=col_names,
        header=None,
    )
    df = df.drop(columns=["_label"])
    df["RCH"] = pd.to_numeric(df["RCH"], errors="coerce")
    df = df.dropna(subset=["RCH"])
    df["RCH"] = df["RCH"].astype(int)
    df["MON"] = df["MON"].astype(int)

    # Split monthly vs annual rows
    if annual:
        df = df[df["MON"] == 0].copy()
    else:
        df = df[df["MON"].between(1, 12)].copy()

    # Filter to requested reach(es)
    if reach is not None:
        reaches = [reach] if isinstance(reach, int) else list(reach)
        df = df[df["RCH"].isin(reaches)].copy()

    # Optionally build a DATE column
    if start_date is not None and not annual:
        df = _add_dates(df, start_date, nyskip, id_col="RCH")

    df = df.reset_index(drop=True)
    logger.info("Loaded %d rows, %d reaches", len(df), df["RCH"].nunique())
    return df


def _add_dates(
    df: pd.DataFrame, start_date: str, nyskip: int, id_col: str
) -> pd.DataFrame:
    """Add a DATE column (first day of each month) aligned to simulation months.

    Assigns dates by sorting within each ID (reach or subbasin) in the order
    rows appear, then mapping to monthly periods starting after the warmup.
    """
    start = pd.Timestamp(start_date) + pd.DateOffset(years=nyskip)
    # Build one date series per unique ID
    result_parts = []
    for gid, group in df.groupby(id_col, sort=False):
        n = len(group)
        dates = pd.date_range(start=start, periods=n, freq="MS")
        group = group.copy()
        group["DATE"] = dates
        result_parts.append(group)
    return pd.concat(result_parts).sort_values(["DATE", id_col])
