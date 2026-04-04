"""Parser for SWAT output.sub (subbasin-level output).

Reads the fixed-format output.sub file produced by SWAT into a tidy DataFrame.

Note on MON encoding
--------------------
SWAT encodes the subbasin output.sub MON column as a combined float:
    MON_value = timestep * 1000 + area_ha
e.g. 1.12199E+03 → timestep=1 (January), area=121.99 ha

This module decodes these into separate MON (int) and AREA_ha (float) columns.
MON=13 indicates an annual summary row.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Column names for output.sub (27 data cols after row label)
# Note: MON_AREA is a combined field decoded into MON + AREA_ha
_SUB_COLS = [
    "SUB", "GIS", "MON_AREA",
    "PRECIP", "SNOWMELT", "PET", "ET", "SW",
    "PERC", "SURQ", "GWQ", "WYLD",
    "SYLD", "ORGN", "ORGP", "NSURQ", "SOLP", "SEDP",
    "LATQ", "LATNO3", "GWNO3",
    "CHOLA", "CBOD", "DOX", "TNO3", "QTILE", "TVAP",
]

# Units for common subbasin variables
UNITS = {
    "PRECIP": "mm", "SNOWMELT": "mm", "PET": "mm", "ET": "mm",
    "SW": "mm", "PERC": "mm", "SURQ": "mm", "GWQ": "mm", "WYLD": "mm",
    "SYLD": "t/ha", "ORGN": "kg/ha", "ORGP": "kg/ha",
    "NSURQ": "kg/ha", "SOLP": "kg/ha", "SEDP": "kg/ha",
    "LATQ": "mm", "LATNO3": "kg/ha", "GWNO3": "kg/ha",
    "TNO3": "kg/ha", "QTILE": "mm",
}


def read_subbasin(
    path: str | Path,
    subbasin: int | list[int] | None = None,
    start_date: str | None = None,
    nyskip: int = 0,
    annual: bool = False,
) -> pd.DataFrame:
    """Read SWAT output.sub into a DataFrame.

    Args:
        path: Path to output.sub file.
        subbasin: Subbasin number(s) to return. None = all subbasins.
        start_date: Simulation start date as 'YYYY-MM-DD'. If provided, a DATE
            column is added (monthly, after warmup skip).
        nyskip: Warmup years to skip when building dates (requires start_date).
        annual: If True, return only annual summary rows (MON == 13).
            If False (default), return only monthly rows (MON 1-12).

    Returns:
        DataFrame with decoded MON and AREA_ha columns plus all hydrology vars.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"output.sub not found: {path}")

    logger.info("Reading %s", path)

    col_names = ["_label"] + _SUB_COLS
    df = pd.read_csv(
        path,
        sep=r"\s+",
        skiprows=9,
        names=col_names,
        header=None,
    )
    df = df.drop(columns=["_label"])
    df["SUB"] = pd.to_numeric(df["SUB"], errors="coerce")
    df["MON_AREA"] = pd.to_numeric(df["MON_AREA"], errors="coerce")
    df = df.dropna(subset=["SUB", "MON_AREA"])
    df["SUB"] = df["SUB"].astype(int)

    # Decode combined MON_AREA field
    df["MON"] = (df["MON_AREA"] // 1000).astype(int)
    df["AREA_ha"] = (df["MON_AREA"] % 1000).round(2)
    df = df.drop(columns=["MON_AREA"])

    # Reorder: ID cols first
    id_cols = ["SUB", "GIS", "MON", "AREA_ha"]
    var_cols = [c for c in df.columns if c not in id_cols]
    df = df[id_cols + var_cols]

    # Split monthly vs annual
    if annual:
        df = df[df["MON"] == 13].copy()
    else:
        df = df[df["MON"].between(1, 12)].copy()

    # Filter subbasins
    if subbasin is not None:
        subs = [subbasin] if isinstance(subbasin, int) else list(subbasin)
        df = df[df["SUB"].isin(subs)].copy()

    # Optionally add DATE column
    if start_date is not None and not annual:
        from .reach import _add_dates
        df = _add_dates(df, start_date, nyskip, id_col="SUB")

    df = df.reset_index(drop=True)
    logger.info("Loaded %d rows, %d subbasins", len(df), df["SUB"].nunique())
    return df
