"""CF-compliant NetCDF export for SWAT subbasin and reach outputs.

Converts SWAT output DataFrames (from read_subbasin / read_reach) into
CF-1.8 compliant spatiotemporal NetCDF files by rasterizing subbasin/reach
polygons for each timestep.

Replaces the old `subbasinToNetCDF` method using xarray + rioxarray
(no netCDF4 or geocube dependency).

Dependencies: xarray, rioxarray, geopandas, rasterio (all already in stack).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

import geopandas as gpd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# CF standard names for common SWAT variables
_CF_STANDARD_NAMES = {
    "PRECIP": "precipitation_amount",
    "SNOWMELT": "snowfall_amount",
    "PET": "water_potential_evapotranspiration_amount",
    "ET": "water_evapotranspiration_amount",
    "SW": "soil_moisture_content",
    "PERC": "soil_water_percolation_flux",
    "SURQ": "surface_runoff_amount",
    "GWQ": "subsurface_runoff_amount",
    "WYLD": "runoff_amount",
    "FLOW_IN": "water_volume_transport_into_sea_water",
    "FLOW_OUT": "water_volume_transport_out_of_sea_water",
    "SEDCONC": "mass_concentration_of_sediment_in_sea_water",
}

_CF_UNITS = {
    # Subbasin
    "PRECIP": "mm", "SNOWMELT": "mm", "PET": "mm", "ET": "mm",
    "SW": "mm", "PERC": "mm", "SURQ": "mm", "GWQ": "mm", "WYLD": "mm",
    "SYLD": "t/ha", "LATQ": "mm",
    # Reach
    "FLOW_IN": "m3 s-1", "FLOW_OUT": "m3 s-1",
    "SED_OUT": "t", "SEDCONC": "mg/L",
    "TOT_N": "kg", "TOT_P": "kg",
    "WTMP": "degC",
}


def to_netcdf(
    df: pd.DataFrame,
    shapefile: str | Path,
    variables: Sequence[str],
    output_path: str | Path,
    id_col: str = "SUB",
    date_col: str = "DATE",
    resolution: float = 30.0,
    crs: str = "EPSG:32616",
    shapefile_id_col: str | None = None,
    title: str = "SWAT model output",
    compress_level: int = 4,
) -> Path:
    """Export a SWAT output DataFrame to a CF-compliant NetCDF file.

    Args:
        df: Output DataFrame from read_subbasin() or read_reach(), must have
            DATE and id_col columns.
        shapefile: Path to the subbasin or reach shapefile (.shp).
        variables: List of variable column names to export (e.g. ['ET', 'WYLD']).
        output_path: Path to write the output .nc file.
        id_col: Column identifying subbasin or reach number (default 'SUB').
        date_col: Column with datetime values (default 'DATE').
        resolution: Output raster resolution in CRS units (default 30 m).
        crs: Target projected CRS for the output grid (default EPSG:32616).
        shapefile_id_col: Column in the shapefile that maps to id_col values.
            Auto-detected if None (looks for 'Subbasin', 'SUB', 'RCH', 'REACH').
        title: Global title attribute written to the NetCDF file.
        compress_level: zlib compression level 1–9 (default 4).

    Returns:
        Path to the written NetCDF file.

    Raises:
        ImportError: If xarray or rioxarray is not installed.
        ValueError: If variables or id_col are not found in df.
    """
    try:
        import xarray as xr
        import rioxarray  # noqa: F401 — registers .rio accessor
    except ImportError:
        raise ImportError(
            "xarray and rioxarray are required for NetCDF export:\n"
            "  pip install xarray rioxarray"
        )

    import rasterio
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds

    output_path = Path(output_path)
    shapefile = Path(shapefile)

    # --- Validate inputs ---
    missing_vars = [v for v in variables if v not in df.columns]
    if missing_vars:
        raise ValueError(f"Variables not in DataFrame: {missing_vars}")
    if id_col not in df.columns:
        raise ValueError(f"id_col '{id_col}' not in DataFrame")
    if date_col not in df.columns:
        raise ValueError(f"date_col '{date_col}' not in DataFrame")

    # --- Load and reproject shapefile ---
    gdf = gpd.read_file(shapefile).to_crs(crs)

    # Auto-detect the id column in the shapefile
    if shapefile_id_col is None:
        candidates = ["Subbasin", "SUB", "SUBBASIN", "RCH", "REACH", "HRU_ID"]
        for cand in candidates:
            if cand in gdf.columns:
                shapefile_id_col = cand
                break
        if shapefile_id_col is None:
            raise ValueError(
                f"Cannot auto-detect id column in shapefile. "
                f"Available columns: {list(gdf.columns)}. "
                f"Use shapefile_id_col parameter."
            )
    logger.info("Using shapefile id column: '%s'", shapefile_id_col)

    # --- Build the raster grid template ---
    bounds = gdf.total_bounds  # (minx, miny, maxx, maxy)
    width = int(np.ceil((bounds[2] - bounds[0]) / resolution))
    height = int(np.ceil((bounds[3] - bounds[1]) / resolution))
    transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], width, height)

    # Rasterize the id field — each pixel gets the subbasin/reach integer id
    id_raster = rasterize(
        shapes=(
            (geom, int(id_val))
            for geom, id_val in zip(gdf.geometry, gdf[shapefile_id_col])
        ),
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype=np.int32,
    )

    # Build x/y coordinate arrays (pixel centres)
    x_coords = np.array([bounds[0] + resolution * (i + 0.5) for i in range(width)])
    y_coords = np.array([bounds[3] - resolution * (j + 0.5) for j in range(height)])

    dates = pd.to_datetime(df[date_col].unique())
    dates = np.sort(dates)

    total_pixels = width * height
    if total_pixels > 1_000_000:
        logger.warning(
            "Large grid: %dx%d = %d pixels. Consider increasing resolution "
            "(e.g. resolution=250 for subbasin-level output) to reduce file size and runtime.",
            width, height, total_pixels,
        )
    logger.info(
        "Building NetCDF: %d timesteps, %d vars, grid %dx%d",
        len(dates), len(variables), width, height,
    )

    # --- Build xarray Dataset ---
    data_vars = {}
    for var in variables:
        cube = np.full((len(dates), height, width), np.nan, dtype=np.float32)

        for t_idx, date in enumerate(dates):
            t_df = df[df[date_col] == date]
            value_map = dict(zip(t_df[id_col].astype(int), t_df[var].astype(float)))
            # Vectorised fill: apply value_map to id_raster
            arr = np.full((height, width), np.nan, dtype=np.float32)
            for sub_id, val in value_map.items():
                arr[id_raster == sub_id] = val

            cube[t_idx] = arr

        attrs = {
            "units": _CF_UNITS.get(var, "unknown"),
            "long_name": var,
        }
        if var in _CF_STANDARD_NAMES:
            attrs["standard_name"] = _CF_STANDARD_NAMES[var]

        data_vars[var] = xr.DataArray(
            cube,
            dims=["time", "y", "x"],
            coords={"time": dates, "y": y_coords, "x": x_coords},
            attrs=attrs,
        )

    ds = xr.Dataset(data_vars)

    # Attach CRS via rioxarray
    ds = ds.rio.write_crs(crs)
    ds = ds.rio.write_transform(transform)

    # Global attributes (CF conventions)
    ds.attrs = {
        "title": title,
        "Conventions": "CF-1.8",
        "source": "swatpytools.outputs.netcdf",
        "history": f"Created {pd.Timestamp.now().isoformat()}",
    }

    # x/y coordinate attributes
    ds["x"].attrs = {
        "units": "m",
        "long_name": "x coordinate of projection",
        "standard_name": "projection_x_coordinate",
    }
    ds["y"].attrs = {
        "units": "m",
        "long_name": "y coordinate of projection",
        "standard_name": "projection_y_coordinate",
    }

    # Write with compression
    encoding = {
        var: {
            "dtype": "float32",
            "zlib": True,
            "complevel": compress_level,
            "_FillValue": np.float32(-9999.0),
        }
        for var in variables
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(output_path, encoding=encoding, engine="netcdf4")
    logger.info("NetCDF written to %s (%.1f MB)", output_path, output_path.stat().st_size / 1e6)
    return output_path
