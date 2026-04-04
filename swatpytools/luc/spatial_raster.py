"""Raster-based spatial overlay method for SWAT LUC.

Performs pixel-level overlay of new LULC rasters with the HRU raster,
soil raster, and slope raster. This is the method faithful to the
Pai & Saraswat (2011) SWAT2009_LUC tool.
"""

import logging
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.windows import from_bounds

from .config import SLOPE_CODE_MAP, LUCConfig
from .parsers import HRUDefinition, build_subbasin_index
from .redistribute import PixelDistribution, RedistributionResult, redistribute_all

logger = logging.getLogger(__name__)


def _read_grid(path: Path) -> Tuple[np.ndarray, rasterio.Affine, rasterio.crs.CRS, dict]:
    """Read an ESRI ArcInfo Binary Grid or GeoTIFF."""
    with rasterio.open(path) as ds:
        arr = ds.read(1)
        return arr, ds.transform, ds.crs, {
            "bounds": ds.bounds,
            "shape": ds.shape,
            "nodata": ds.nodata,
        }


def _build_corrected_hru_raster(
    config: LUCConfig,
    hrus: List[HRUDefinition],
) -> Tuple[np.ndarray, rasterio.Affine, dict]:
    """Create a corrected HRU raster with only valid (post-threshold) HRU IDs.

    Rasterizes hru1.shp onto the same grid as the HRU raster to produce
    a clean array with values 1-164 (matching the 164 final HRUs).
    """
    import geopandas as gpd

    # Read the reference grid for shape/transform
    with rasterio.open(config.hru_raster_path) as ref_ds:
        transform = ref_ds.transform
        shape = ref_ds.shape
        meta = {
            "bounds": ref_ds.bounds,
            "shape": ref_ds.shape,
            "nodata": 0,
        }

    # Read shapefile
    gdf = gpd.read_file(config.hru_shapefile_path)

    # Build (geometry, hru_id) pairs for rasterization
    shapes = [(geom, hru_id) for geom, hru_id in zip(gdf.geometry, gdf["HRU_ID"])]

    # Rasterize
    hru_array = rasterize(
        shapes,
        out_shape=shape,
        transform=transform,
        fill=0,
        dtype=np.int16,
    )

    valid_count = np.count_nonzero(hru_array > 0)
    logger.info(
        f"Corrected HRU raster: {valid_count} valid pixels, "
        f"{len(np.unique(hru_array[hru_array > 0]))} unique HRU IDs"
    )

    return hru_array, transform, meta


def _align_nlcd_to_hru(
    nlcd_path: Path,
    hru_bounds,
    hru_transform: rasterio.Affine,
    hru_shape: Tuple[int, int],
) -> np.ndarray:
    """Read an NLCD raster windowed and aligned to the HRU grid extent.

    The NLCD raster is larger than the HRU grid but shares the same CRS
    and resolution (30m). This function reads only the overlapping region
    and pads/clips to match the HRU grid shape exactly.
    """
    with rasterio.open(nlcd_path) as nlcd_ds:
        # Compute the window in NLCD pixel coordinates that covers HRU bounds
        nlcd_window = from_bounds(
            hru_bounds.left, hru_bounds.bottom,
            hru_bounds.right, hru_bounds.top,
            nlcd_ds.transform,
        )

        # Round window to integer pixel boundaries
        nlcd_window = nlcd_window.round_offsets().round_lengths()

        # Read the windowed data
        nlcd_data = nlcd_ds.read(1, window=nlcd_window)
        nodata = nlcd_ds.nodata if nlcd_ds.nodata is not None else 255

    # Handle shape mismatch due to rounding
    result = np.full(hru_shape, nodata, dtype=nlcd_data.dtype)
    rows = min(nlcd_data.shape[0], hru_shape[0])
    cols = min(nlcd_data.shape[1], hru_shape[1])
    result[:rows, :cols] = nlcd_data[:rows, :cols]

    return result


def _build_slope_string_array(
    slope_array: np.ndarray,
    slope_nodata: float,
) -> np.ndarray:
    """Convert slope raster codes to slope class string indices.

    Returns an integer array where values map to SLOPE_CODE_MAP keys.
    The actual string lookup happens during redistribution.
    """
    # The slope array already contains the codes (1, 2, 999)
    # We just need to handle nodata
    result = slope_array.copy()
    if slope_nodata is not None:
        result[slope_array == slope_nodata] = 0
    return result


def compute_distributions_raster(
    config: LUCConfig,
    hrus: List[HRUDefinition],
    lookup: Dict[int, str],
    nlcd_path: Path,
) -> Dict[int, PixelDistribution]:
    """Compute per-HRU LULC distributions using raster-based pixel overlay.

    Args:
        config: LUC configuration.
        hrus: HRU definitions.
        lookup: NLCD code to SWAT code mapping.
        nlcd_path: Path to the new NLCD raster.

    Returns:
        Dict mapping hru_id to PixelDistribution.
    """
    # Build corrected HRU raster
    hru_array, hru_transform, hru_meta = _build_corrected_hru_raster(config, hrus)
    hru_shape = hru_meta["shape"]
    hru_bounds = hru_meta["bounds"]

    # Read soil and slope rasters
    soil_array, _, _, soil_meta = _read_grid(config.soil_raster_path)
    slope_array, _, _, slope_meta = _read_grid(config.slope_raster_path)

    # Align NLCD to HRU grid
    nlcd_array = _align_nlcd_to_hru(nlcd_path, hru_bounds, hru_transform, hru_shape)

    # Create valid mask (where HRU raster has valid data)
    valid_mask = hru_array > 0

    # Build HRU-to-subbasin lookup
    hru_to_sub = {h.hru_id: h.subbasin for h in hrus}
    hru_to_def = {h.hru_id: h for h in hrus}
    valid_hru_ids = set(hru_to_sub.keys())

    # Initialize distributions
    distributions = {}
    for hru in hrus:
        distributions[hru.hru_id] = PixelDistribution(
            hru_id=hru.hru_id,
            pixel_counts={},
            total_pixels=0,
            soil_slope_detail={},
        )

    # Process all valid pixels using vectorized operations
    valid_rows, valid_cols = np.where(valid_mask)
    hru_vals = hru_array[valid_rows, valid_cols]
    nlcd_vals = nlcd_array[valid_rows, valid_cols]
    soil_vals = soil_array[valid_rows, valid_cols]
    slope_vals = slope_array[valid_rows, valid_cols]

    logger.info(f"Processing {len(valid_rows)} valid pixels for raster overlay")

    # Convert NLCD codes to SWAT codes
    # Filter out negative/nodata NLCD values first
    nlcd_valid_mask = nlcd_vals >= 0
    nlcd_vals = np.where(nlcd_valid_mask, nlcd_vals, 0)

    # Build a vectorized lookup array
    max_nlcd = max(int(nlcd_vals.max()) + 1, max(lookup.keys()) + 1)
    nlcd_to_idx = {}  # SWAT code string -> integer index
    idx_to_swat = {}  # integer index -> SWAT code string
    idx_counter = 0
    nlcd_map_array = np.full(max_nlcd, -1, dtype=np.int16)

    for nlcd_code, swat_code in lookup.items():
        if nlcd_code < max_nlcd:
            if swat_code not in nlcd_to_idx:
                nlcd_to_idx[swat_code] = idx_counter
                idx_to_swat[idx_counter] = swat_code
                idx_counter += 1
            nlcd_map_array[nlcd_code] = nlcd_to_idx[swat_code]

    swat_idx_vals = nlcd_map_array[nlcd_vals]
    valid_nlcd_mask = (swat_idx_vals >= 0) & nlcd_valid_mask

    # Filter to valid NLCD pixels only
    hru_vals = hru_vals[valid_nlcd_mask]
    swat_idx_vals = swat_idx_vals[valid_nlcd_mask]
    soil_vals = soil_vals[valid_nlcd_mask]
    slope_vals = slope_vals[valid_nlcd_mask]

    unmapped_count = (~valid_nlcd_mask).sum()
    if unmapped_count > 0:
        logger.warning(f"{unmapped_count} pixels had NLCD codes not in lookup table")

    # Aggregate per HRU using numpy
    unique_hrus = np.unique(hru_vals)
    for hru_id_val in unique_hrus:
        hru_id = int(hru_id_val)
        if hru_id not in valid_hru_ids:
            continue

        hru_mask = hru_vals == hru_id_val
        hru_swat = swat_idx_vals[hru_mask]
        hru_soil = soil_vals[hru_mask]
        hru_slope = slope_vals[hru_mask]

        dist = distributions[hru_id]
        dist.total_pixels = int(hru_mask.sum())

        # Count per SWAT code
        unique_swat, counts = np.unique(hru_swat, return_counts=True)
        for swat_idx, cnt in zip(unique_swat, counts):
            swat_code = idx_to_swat[int(swat_idx)]
            dist.pixel_counts[swat_code] = int(cnt)

            # Build soil/slope detail for non-homogeneous pixels
            hru_def = hru_to_def.get(hru_id)
            if hru_def and swat_code != hru_def.landuse:
                code_mask = hru_swat == swat_idx
                code_soils = hru_soil[code_mask]
                code_slopes = hru_slope[code_mask]

                detail = {}
                for s_soil, s_slope in zip(
                    code_soils.astype(str), code_slopes.astype(int)
                ):
                    slope_str = SLOPE_CODE_MAP.get(int(s_slope), str(s_slope))
                    key = (str(s_soil), slope_str)
                    detail[key] = detail.get(key, 0) + 1

                dist.soil_slope_detail[swat_code] = detail

    logger.info(f"Raster overlay complete: {len(distributions)} HRU distributions computed")
    return distributions


def run_raster_method(
    config: LUCConfig,
    hrus: List[HRUDefinition],
    lookup: Dict[int, str],
) -> List[RedistributionResult]:
    """Run the full raster-based LUC workflow for all update years.

    Returns a list of RedistributionResult, one per update year.
    """
    results = []

    for year, month, day, raster_path in config.update_rasters:
        logger.info(f"Raster method: processing {year}-{month:02d}-{day:02d} ({raster_path.name})")

        distributions = compute_distributions_raster(config, hrus, lookup, raster_path)
        result = redistribute_all(
            hrus, distributions, year, month, day, use_pixel_detail=True
        )
        results.append(result)

        # Log summary
        changed = sum(
            1 for h in hrus
            if abs(result.hru_fractions.get(h.hru_id, h.hru_fr) - h.hru_fr) > 1e-6
        )
        logger.info(f"  {changed}/{len(hrus)} HRUs changed fractional area")

    return results
