"""Core redistribution algorithm for SWAT Land Use Change module.

Implements the Pai & Saraswat (2011) methodology: when land use changes,
pixels that no longer match their HRU's base land use are redistributed
to matching HRUs within the same subbasin using a cascading match approach.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

from .parsers import HRUDefinition, build_match_indexes, build_subbasin_index

logger = logging.getLogger(__name__)


@dataclass
class PixelDistribution:
    """Per-HRU land use distribution from a new LULC raster.

    For the raster method, soil_slope_detail provides per-pixel soil/slope
    info for precise redistribution. For the shapefile method, this is empty
    and the HRU's base soil/slope is used instead.
    """

    hru_id: int
    pixel_counts: Dict[str, int]  # SWAT_code -> pixel count
    total_pixels: int
    # Raster method only: {swat_code: {(soil, slope): count}}
    soil_slope_detail: Dict[str, Dict[Tuple[str, str], int]] = field(default_factory=dict)


@dataclass
class RedistributionResult:
    """Result of redistribution for one update year."""

    year: int
    month: int
    day: int
    hru_fractions: Dict[int, float]  # hru_id -> new HRU_FR
    unmatched_pixels: Dict[int, Dict[str, int]] = field(default_factory=dict)
    # {subbasin: {swat_code: unmatched_count}}


def redistribute_subbasin(
    sub_hrus: List[HRUDefinition],
    distributions: Dict[int, PixelDistribution],
    use_pixel_detail: bool = False,
) -> Tuple[Dict[int, float], Dict[str, int]]:
    """Redistribute pixels within a single subbasin.

    Args:
        sub_hrus: HRU definitions for this subbasin.
        distributions: Per-HRU pixel distributions from new LULC overlay.
        use_pixel_detail: If True, use per-pixel soil/slope data (raster method).
            If False, use HRU-level soil/slope (shapefile method).

    Returns:
        (new_hru_fr dict, unmatched dict {swat_code: count})
    """
    by_lss, by_ls, by_l = build_match_indexes(sub_hrus)
    hru_lookup = {h.hru_id: h for h in sub_hrus}

    # Initialize pixel counts
    redistributed = {h.hru_id: 0 for h in sub_hrus}
    total_pixels = sum(d.total_pixels for d in distributions.values())
    unmatched = {}

    if total_pixels == 0:
        # No valid pixels in this subbasin - return original fractions
        return {h.hru_id: h.hru_fr for h in sub_hrus}, unmatched

    for hru_id, dist in distributions.items():
        hru = hru_lookup.get(hru_id)
        if hru is None:
            logger.warning(f"HRU {hru_id} not found in subbasin definitions")
            continue

        for swat_code, count in dist.pixel_counts.items():
            if count == 0:
                continue

            if swat_code == hru.landuse:
                # Homogeneous - pixels match the HRU's base land use
                redistributed[hru_id] += count
            else:
                # Non-homogeneous - need to redistribute
                if use_pixel_detail and dist.soil_slope_detail.get(swat_code):
                    # Raster method: redistribute using per-pixel soil/slope
                    detail = dist.soil_slope_detail[swat_code]
                    for (soil, slope), px_count in detail.items():
                        _assign_pixels(
                            px_count, swat_code, soil, slope,
                            hru_id, by_lss, by_ls, by_l,
                            redistributed, unmatched,
                        )
                else:
                    # Shapefile method: use HRU's own soil/slope for all pixels
                    _assign_pixels(
                        count, swat_code, hru.soil, hru.slope,
                        hru_id, by_lss, by_ls, by_l,
                        redistributed, unmatched,
                    )

    # Calculate new HRU_FR
    new_hru_fr = {}
    for hru in sub_hrus:
        new_hru_fr[hru.hru_id] = redistributed[hru.hru_id] / total_pixels

    # Validate and normalize
    fr_sum = sum(new_hru_fr.values())
    if abs(fr_sum - 1.0) > 1e-6 and fr_sum > 0:
        logger.debug(
            f"Subbasin {sub_hrus[0].subbasin}: HRU_FR sum = {fr_sum:.8f}, normalizing"
        )
        for hru_id in new_hru_fr:
            new_hru_fr[hru_id] /= fr_sum

    return new_hru_fr, unmatched


def _assign_pixels(
    count: int,
    swat_code: str,
    soil: str,
    slope: str,
    source_hru_id: int,
    by_lss: dict,
    by_ls: dict,
    by_l: dict,
    redistributed: Dict[int, int],
    unmatched: Dict[str, int],
):
    """Assign non-homogeneous pixels using cascading match.

    Match levels:
        1. Same land use + soil + slope
        2. Same land use + soil
        3. Same land use only
        4. No match -> keep with source HRU
    """
    # Level 1: exact match (lulc + soil + slope)
    targets = by_lss.get((swat_code, soil, slope))
    if targets:
        _distribute_proportional(count, targets, redistributed)
        return

    # Level 2: lulc + soil
    targets = by_ls.get((swat_code, soil))
    if targets:
        _distribute_proportional(count, targets, redistributed)
        return

    # Level 3: lulc only
    targets = by_l.get(swat_code)
    if targets:
        _distribute_proportional(count, targets, redistributed)
        return

    # No match: keep with source HRU
    redistributed[source_hru_id] += count
    unmatched[swat_code] = unmatched.get(swat_code, 0) + count


def _distribute_proportional(
    count: int,
    targets: List[HRUDefinition],
    redistributed: Dict[int, int],
):
    """Distribute pixels proportionally among target HRUs based on original HRU_FR."""
    if len(targets) == 1:
        redistributed[targets[0].hru_id] += count
        return

    total_fr = sum(t.hru_fr for t in targets)
    if total_fr == 0:
        # Equal distribution as fallback
        per_hru = count / len(targets)
        remainder = count
        for i, t in enumerate(targets):
            if i == len(targets) - 1:
                redistributed[t.hru_id] += remainder
            else:
                share = round(per_hru)
                redistributed[t.hru_id] += share
                remainder -= share
        return

    # Proportional distribution maintaining integer pixel counts
    remainder = count
    for i, t in enumerate(targets):
        if i == len(targets) - 1:
            # Last target gets the remainder to avoid rounding loss
            redistributed[t.hru_id] += remainder
        else:
            share = round(count * t.hru_fr / total_fr)
            redistributed[t.hru_id] += share
            remainder -= share


def redistribute_all(
    hrus: List[HRUDefinition],
    all_distributions: Dict[int, PixelDistribution],
    year: int,
    month: int,
    day: int,
    use_pixel_detail: bool = False,
) -> RedistributionResult:
    """Run redistribution for all subbasins for one update year.

    Args:
        hrus: All HRU definitions.
        all_distributions: Per-HRU pixel distributions from spatial overlay.
        year, month, day: Date of this land use update.
        use_pixel_detail: Whether to use per-pixel soil/slope (raster method).

    Returns:
        RedistributionResult with new HRU_FR values for all HRUs.
    """
    sub_index = build_subbasin_index(hrus)
    all_fractions = {}
    all_unmatched = {}

    for sub_id, sub_hrus in sorted(sub_index.items()):
        # Collect distributions for HRUs in this subbasin
        sub_dists = {
            h.hru_id: all_distributions[h.hru_id]
            for h in sub_hrus
            if h.hru_id in all_distributions
        }

        new_fr, unmatched = redistribute_subbasin(
            sub_hrus, sub_dists, use_pixel_detail
        )
        all_fractions.update(new_fr)

        if unmatched:
            all_unmatched[sub_id] = unmatched
            for code, cnt in unmatched.items():
                logger.info(
                    f"Subbasin {sub_id}: {cnt} pixels with LULC '{code}' "
                    f"had no matching HRU (kept with source)"
                )

    return RedistributionResult(
        year=year,
        month=month,
        day=day,
        hru_fractions=all_fractions,
        unmatched_pixels=all_unmatched,
    )
