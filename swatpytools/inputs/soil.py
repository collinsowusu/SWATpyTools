"""SSURGO soil shapefile → raster preparation for ArcSWAT/QSWAT.

Converts SSURGO soil shapefiles (downloaded from Web Soil Survey) into
integer GeoTIFF rasters keyed on MUKEY, ready for use as SWAT soil inputs.

Replaces the old `prepSoilRaster` method using rasterio + geopandas only
(no geocube dependency).
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds

logger = logging.getLogger(__name__)


def prepare_soil_raster(
    soil_dir: str | Path,
    output_path: str | Path | None = None,
    crs: str = "EPSG:32616",
    resolution: float = 30.0,
    mosaic: bool = True,
    nodata: int = 0,
) -> Path:
    """Convert SSURGO soil shapefiles to a MUKEY integer GeoTIFF raster.

    Reads all .shp files in soil_dir, reprojects to crs, rasterizes the
    MUKEY field at the given resolution, and writes a compressed GeoTIFF.

    Args:
        soil_dir: Directory containing SSURGO .shp files.
        output_path: Output GeoTIFF path. Defaults to soil_dir/soil_mukey.tif.
        crs: Target projected CRS (default: EPSG:32616, UTM Zone 16N).
        resolution: Pixel size in CRS units (default: 30 m).
        mosaic: If True (default), merge all shapefiles into one raster.
            If False, write one raster per shapefile.
        nodata: NoData fill value (default: 0, avoids ArcSWAT integer errors).

    Returns:
        Path to the output raster (or last written raster if mosaic=False).
    """
    soil_dir = Path(soil_dir)
    shp_files = sorted(soil_dir.glob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp files found in {soil_dir}")

    logger.info("Found %d shapefile(s) in %s", len(shp_files), soil_dir)

    gdfs = []
    for shp in shp_files:
        gdf = gpd.read_file(shp)
        if "MUKEY" not in gdf.columns:
            raise ValueError(f"{shp.name} has no MUKEY column")
        gdf["MUKEY_INT"] = gdf["MUKEY"].astype(int)
        gdf = gdf.to_crs(crs)
        gdfs.append(gdf)
        logger.debug("Loaded %s (%d polygons)", shp.name, len(gdf))

    if mosaic:
        output_path = output_path or soil_dir / "soil_mukey.tif"
        combined = gpd.GeoDataFrame(
            pd.concat(gdfs, ignore_index=True), geometry="geometry", crs=crs
        )
        out_path = _rasterize_mukey(combined, Path(output_path), resolution, nodata)
        logger.info("Mosaiced raster written to %s", out_path)
        return out_path
    else:
        last_path = None
        for gdf, shp in zip(gdfs, shp_files):
            stem = shp.stem
            out_p = soil_dir / f"{stem}_mukey.tif"
            _rasterize_mukey(gdf, out_p, resolution, nodata)
            logger.info("Raster written to %s", out_p)
            last_path = out_p
        return last_path


def _rasterize_mukey(
    gdf: gpd.GeoDataFrame,
    output_path: Path,
    resolution: float,
    nodata: int,
) -> Path:
    """Rasterize the MUKEY_INT column of a GeoDataFrame to a GeoTIFF."""
    import pandas as pd  # local import — already available in env

    bounds = gdf.total_bounds  # (minx, miny, maxx, maxy)
    width = int(np.ceil((bounds[2] - bounds[0]) / resolution))
    height = int(np.ceil((bounds[3] - bounds[1]) / resolution))
    transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], width, height)

    shapes = (
        (geom, value)
        for geom, value in zip(gdf.geometry, gdf["MUKEY_INT"])
        if geom is not None and not geom.is_empty
    )

    raster = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=nodata,
        dtype=np.int32,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=np.int32,
        crs=gdf.crs,
        transform=transform,
        nodata=nodata,
        compress="deflate",
    ) as dst:
        dst.write(raster, 1)

    return output_path
