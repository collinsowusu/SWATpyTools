"""SWAT Land Use Change (LUC) file generator.

Generates lup.dat and HRU fraction files for activating SWAT's dynamic
land use change module, following the Pai & Saraswat (2011) methodology.

Usage:
    # CLI:
    python -m swatpytools.luc --project-dir ./ArcSWAT_Project \\
        --update 2016 1 1 ./Landuse_Rasters/NLCD_2016_PRJ.tif \\
        --update 2019 1 1 ./Landuse_Rasters/NLCD_2019_NEW.tif

    # Python API:
    from swatpytools.luc import LUCConfig, run_luc
    config = LUCConfig.from_project_dir(...)
    results = run_luc(config)
"""

from .config import LUCConfig
from .parsers import HRUDefinition, parse_hru_files, parse_lookup_table
from .redistribute import RedistributionResult
from .spatial_raster import run_raster_method
from .spatial_shapefile import run_shapefile_method
from .validate import validate_hru_fr_sums, compare_methods, generate_summary_report
from .writers import write_lup_dat


def run_luc(config: LUCConfig):
    """Run the full LUC workflow using the configured method.

    Returns:
        dict with keys:
            "hrus": list of HRUDefinition
            "raster_results": list of RedistributionResult (or None)
            "shapefile_results": list of RedistributionResult (or None)
    """
    hrus = parse_hru_files(config.txtinout_dir)
    lookup = parse_lookup_table(config.lookup_table_path)

    raster_results = None
    shapefile_results = None

    if config.method in ("raster", "both"):
        raster_results = run_raster_method(config, hrus, lookup)

    if config.method in ("shapefile", "both"):
        shapefile_results = run_shapefile_method(config, hrus, lookup)

    return {
        "hrus": hrus,
        "raster_results": raster_results,
        "shapefile_results": shapefile_results,
    }


__all__ = [
    "LUCConfig",
    "HRUDefinition",
    "RedistributionResult",
    "run_luc",
    "run_raster_method",
    "run_shapefile_method",
    "validate_hru_fr_sums",
    "compare_methods",
    "generate_summary_report",
    "write_lup_dat",
    "parse_hru_files",
    "parse_lookup_table",
]
