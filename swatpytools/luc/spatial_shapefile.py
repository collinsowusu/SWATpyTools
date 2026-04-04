"""Shapefile-based spatial overlay method for SWAT LUC.

Uses hru1.shp polygons with rasterio.mask to perform zonal statistics
for each HRU polygon against new LULC rasters.
"""

import logging
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.mask import mask as rasterio_mask

from .config import LUCConfig
from .parsers import HRUDefinition
from .redistribute import PixelDistribution, RedistributionResult, redistribute_all

logger = logging.getLogger(__name__)


def compute_distributions_shapefile(
    config: LUCConfig,
    hrus: List[HRUDefinition],
    lookup: Dict[int, str],
    nlcd_path: Path,
) -> Dict[int, PixelDistribution]:
    """Compute per-HRU LULC distributions using shapefile-based zonal statistics.

    For each HRU polygon in hru1.shp, extracts the NLCD pixel values
    within that polygon and counts occurrences of each SWAT land use code.

    Args:
        config: LUC configuration.
        hrus: HRU definitions.
        lookup: NLCD code to SWAT code mapping.
        nlcd_path: Path to the new NLCD raster.

    Returns:
        Dict mapping hru_id to PixelDistribution.
    """
    # Read shapefile
    gdf = gpd.read_file(config.hru_shapefile_path)

    # Build HRU_ID to HRUDefinition lookup
    hru_lookup = {h.hru_id: h for h in hrus}

    distributions = {}

    with rasterio.open(nlcd_path) as nlcd_ds:
        nodata = nlcd_ds.nodata if nlcd_ds.nodata is not None else 255

        for _, row in gdf.iterrows():
            hru_id = int(row["HRU_ID"])
            geometry = row["geometry"]

            if hru_id not in hru_lookup:
                logger.warning(f"HRU_ID {hru_id} from shapefile not found in HRU definitions")
                continue

            # Extract pixels within this HRU polygon
            try:
                out_image, out_transform = rasterio_mask(
                    nlcd_ds, [geometry], crop=True, nodata=nodata
                )
            except ValueError as e:
                logger.warning(f"Failed to mask HRU {hru_id}: {e}")
                distributions[hru_id] = PixelDistribution(
                    hru_id=hru_id, pixel_counts={}, total_pixels=0
                )
                continue

            # Flatten and filter nodata
            pixels = out_image[0].flatten()
            valid_pixels = pixels[(pixels != nodata) & (pixels > 0)]

            # Convert NLCD codes to SWAT codes and count
            pixel_counts = {}
            total_valid = 0
            for nlcd_code in np.unique(valid_pixels):
                nlcd_int = int(nlcd_code)
                swat_code = lookup.get(nlcd_int)
                if swat_code is None:
                    continue
                count = int((valid_pixels == nlcd_code).sum())
                pixel_counts[swat_code] = pixel_counts.get(swat_code, 0) + count
                total_valid += count

            distributions[hru_id] = PixelDistribution(
                hru_id=hru_id,
                pixel_counts=pixel_counts,
                total_pixels=total_valid,
                soil_slope_detail={},  # Not available in shapefile method
            )

    logger.info(
        f"Shapefile overlay complete: {len(distributions)} HRU distributions computed"
    )
    return distributions


def run_shapefile_method(
    config: LUCConfig,
    hrus: List[HRUDefinition],
    lookup: Dict[int, str],
) -> List[RedistributionResult]:
    """Run the full shapefile-based LUC workflow for all update years.

    Returns a list of RedistributionResult, one per update year.
    """
    results = []

    for year, month, day, raster_path in config.update_rasters:
        logger.info(
            f"Shapefile method: processing {year}-{month:02d}-{day:02d} ({raster_path.name})"
        )

        distributions = compute_distributions_shapefile(config, hrus, lookup, raster_path)
        result = redistribute_all(
            hrus, distributions, year, month, day, use_pixel_detail=False
        )
        results.append(result)

        changed = sum(
            1 for h in hrus
            if abs(result.hru_fractions.get(h.hru_id, h.hru_fr) - h.hru_fr) > 1e-6
        )
        logger.info(f"  {changed}/{len(hrus)} HRUs changed fractional area")

    return results
