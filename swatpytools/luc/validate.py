"""Validation and comparison utilities for SWAT LUC results."""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from .parsers import HRUDefinition, build_subbasin_index
from .redistribute import RedistributionResult

logger = logging.getLogger(__name__)


def validate_hru_fr_sums(
    results: List[RedistributionResult],
    hrus: List[HRUDefinition],
    tolerance: float = 1e-4,
) -> List[str]:
    """Validate that HRU_FR sums to 1.0 per subbasin for each update year.

    Returns a list of warning messages (empty if all valid).
    """
    sub_index = build_subbasin_index(hrus)
    warnings = []

    for result in results:
        for sub_id, sub_hrus in sorted(sub_index.items()):
            fr_sum = sum(
                result.hru_fractions.get(h.hru_id, 0.0) for h in sub_hrus
            )
            if abs(fr_sum - 1.0) > tolerance:
                msg = (
                    f"Year {result.year}: Subbasin {sub_id} HRU_FR sum = "
                    f"{fr_sum:.8f} (off by {fr_sum - 1.0:+.8f})"
                )
                warnings.append(msg)
                logger.warning(msg)

    if not warnings:
        logger.info("All HRU_FR sums validated (within tolerance)")

    return warnings


def compare_methods(
    raster_results: List[RedistributionResult],
    shapefile_results: List[RedistributionResult],
    hrus: List[HRUDefinition],
) -> pd.DataFrame:
    """Compare HRU_FR values between raster and shapefile methods.

    Returns a DataFrame with columns:
        year, hru_id, subbasin, landuse, raster_fr, shapefile_fr, diff, pct_diff
    """
    rows = []

    for r_result, s_result in zip(raster_results, shapefile_results):
        assert r_result.year == s_result.year, "Results must be for the same years"

        for hru in hrus:
            r_fr = r_result.hru_fractions.get(hru.hru_id, hru.hru_fr)
            s_fr = s_result.hru_fractions.get(hru.hru_id, hru.hru_fr)
            diff = r_fr - s_fr
            pct_diff = (diff / r_fr * 100) if r_fr > 0 else 0.0

            rows.append({
                "year": r_result.year,
                "hru_id": hru.hru_id,
                "subbasin": hru.subbasin,
                "landuse": hru.landuse,
                "raster_fr": r_fr,
                "shapefile_fr": s_fr,
                "diff": diff,
                "pct_diff": pct_diff,
            })

    df = pd.DataFrame(rows)

    # Log summary statistics per year
    for year in df["year"].unique():
        year_df = df[df["year"] == year]
        r_vals = year_df["raster_fr"].values
        s_vals = year_df["shapefile_fr"].values

        # R-squared
        ss_res = np.sum((r_vals - s_vals) ** 2)
        ss_tot = np.sum((r_vals - np.mean(r_vals)) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        max_diff = year_df["diff"].abs().max()
        mean_diff = year_df["diff"].abs().mean()

        logger.info(
            f"Year {year} method comparison: R²={r_squared:.4f}, "
            f"max |diff|={max_diff:.6f}, mean |diff|={mean_diff:.6f}"
        )

    return df


def compare_with_baseline(
    results: List[RedistributionResult],
    hrus: List[HRUDefinition],
    method_name: str = "",
) -> pd.DataFrame:
    """Compare new HRU_FR with original values from .hru files.

    Returns a DataFrame showing changes per HRU and aggregated by land use.
    """
    rows = []

    for result in results:
        for hru in hrus:
            new_fr = result.hru_fractions.get(hru.hru_id, hru.hru_fr)
            change = new_fr - hru.hru_fr

            rows.append({
                "year": result.year,
                "hru_id": hru.hru_id,
                "subbasin": hru.subbasin,
                "landuse": hru.landuse,
                "soil": hru.soil,
                "slope": hru.slope,
                "original_fr": hru.hru_fr,
                "new_fr": new_fr,
                "change": change,
                "pct_change": (change / hru.hru_fr * 100) if hru.hru_fr > 0 else 0.0,
                "method": method_name,
            })

    return pd.DataFrame(rows)


def generate_summary_report(
    results: List[RedistributionResult],
    hrus: List[HRUDefinition],
    method_name: str = "",
) -> str:
    """Generate a text summary report of land use changes.

    Shows per-subbasin land use distribution before and after each update.
    """
    sub_index = build_subbasin_index(hrus)
    lines = []
    lines.append(f"SWAT LUC Summary Report - {method_name} Method")
    lines.append("=" * 70)

    for result in results:
        lines.append(f"\nUpdate Year: {result.year}-{result.month:02d}-{result.day:02d}")
        lines.append("-" * 70)

        # Watershed-level summary by land use
        lu_original = {}
        lu_new = {}
        for hru in hrus:
            lu_original[hru.landuse] = lu_original.get(hru.landuse, 0) + hru.hru_fr
            new_fr = result.hru_fractions.get(hru.hru_id, hru.hru_fr)
            lu_new[hru.landuse] = lu_new.get(hru.landuse, 0) + new_fr

        lines.append(f"\n{'Land Use':<10} {'Original':>12} {'Updated':>12} {'Change':>12}")
        lines.append(f"{'-'*10} {'-'*12} {'-'*12} {'-'*12}")
        for lu in sorted(set(lu_original) | set(lu_new)):
            orig = lu_original.get(lu, 0)
            new = lu_new.get(lu, 0)
            lines.append(f"{lu:<10} {orig:12.6f} {new:12.6f} {new - orig:+12.6f}")

        # Per-subbasin summary
        lines.append(f"\n{'Sub':>4} {'HRUs':>5} {'Max Change':>12} {'Most Changed LU':>16}")
        lines.append(f"{'-'*4} {'-'*5} {'-'*12} {'-'*16}")

        for sub_id, sub_hrus in sorted(sub_index.items()):
            max_change = 0
            max_lu = ""
            for hru in sub_hrus:
                new_fr = result.hru_fractions.get(hru.hru_id, hru.hru_fr)
                change = abs(new_fr - hru.hru_fr)
                if change > max_change:
                    max_change = change
                    max_lu = hru.landuse

            lines.append(
                f"{sub_id:4d} {len(sub_hrus):5d} {max_change:12.6f} {max_lu:>16}"
            )

        # Unmatched pixels summary
        if result.unmatched_pixels:
            lines.append(f"\nUnmatched pixels (no matching HRU in subbasin):")
            for sub_id, codes in sorted(result.unmatched_pixels.items()):
                for code, count in sorted(codes.items()):
                    lines.append(f"  Subbasin {sub_id}: {count} pixels with LULC '{code}'")

    return "\n".join(lines)
