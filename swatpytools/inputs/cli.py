"""Command-line interface for SWAT input preparation utilities.

Usage
-----
::

    # Prepare soil raster from SSURGO shapefiles
    python -m swatpytools.inputs soil --config soil_config.json
    python -m swatpytools.inputs soil --soil-dir ./Soils/SSURGO --output ./soil_mukey.tif

Config file format (``soil_config.json``)::

    {
      "soil_dir":    "./Soils/SSURGO",
      "output_path": "./Soils/soil_mukey.tif",
      "crs":         "EPSG:32616",
      "resolution":  30.0,
      "mosaic":      true
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Entry point for ``python -m swatpytools.inputs``."""

    parser = argparse.ArgumentParser(
        prog="swatpytools.inputs",
        description="SWAT input preparation utilities",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging")

    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    # ------------------------------------------------------------------
    # soil sub-command
    # ------------------------------------------------------------------
    sp = subparsers.add_parser(
        "soil",
        help="Convert SSURGO soil shapefiles to a MUKEY GeoTIFF raster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Convert SSURGO soil shapefiles to an integer MUKEY GeoTIFF raster\n"
            "for use as the SWAT soil input layer.\n\n"
            "Supply either --config (JSON file) or individual flags.\n"
            "Individual flags override values from --config."
        ),
    )
    sp.add_argument(
        "--config",
        type=Path,
        metavar="JSON",
        default=None,
        help="JSON config file with soil preparation settings",
    )
    sp.add_argument(
        "--soil-dir",
        type=Path,
        metavar="DIR",
        default=None,
        help="Directory containing SSURGO .shp files",
    )
    sp.add_argument(
        "--output",
        type=Path,
        metavar="TIFF",
        default=None,
        help="Output GeoTIFF path (default: soil_dir/soil_mukey.tif)",
    )
    sp.add_argument(
        "--crs",
        type=str,
        default=None,
        metavar="EPSG:XXXX",
        help="Target projected CRS (default: EPSG:32616)",
    )
    sp.add_argument(
        "--resolution",
        type=float,
        default=None,
        metavar="METRES",
        help="Pixel size in CRS units (default: 30.0)",
    )
    sp.add_argument(
        "--no-mosaic",
        action="store_true",
        help="Write one raster per shapefile instead of a single mosaic",
    )

    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command == "soil":
        _cmd_soil(args, parser)
    else:
        parser.print_help()
        sys.exit(0)


def _cmd_soil(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    # ---- Load JSON config if provided, then let CLI flags override ----
    cfg: dict = {}
    if args.config:
        if not args.config.exists():
            parser.error(f"Config file not found: {args.config}")
        with open(args.config) as f:
            cfg = json.load(f)
        logging.info("Loaded config from %s", args.config)

    soil_dir = args.soil_dir or (Path(cfg["soil_dir"]) if "soil_dir" in cfg else None)
    output_path = args.output or (Path(cfg["output_path"]) if "output_path" in cfg else None)
    crs = args.crs or cfg.get("crs", "EPSG:32616")
    resolution = args.resolution or cfg.get("resolution", 30.0)
    mosaic = not args.no_mosaic if args.no_mosaic else cfg.get("mosaic", True)

    if soil_dir is None:
        parser.error("--soil-dir is required (or provide 'soil_dir' in --config)")

    # ---- Run ----
    try:
        from .soil import prepare_soil_raster
    except ImportError as exc:
        parser.error(
            f"Spatial dependencies not installed: {exc}\n"
            "Install with: pip install swatpytools[spatial]"
        )

    logging.info(
        "Preparing soil raster — dir=%s  crs=%s  resolution=%s  mosaic=%s",
        soil_dir, crs, resolution, mosaic,
    )
    out = prepare_soil_raster(
        soil_dir=soil_dir,
        output_path=output_path,
        crs=crs,
        resolution=resolution,
        mosaic=mosaic,
    )
    print(f"Soil raster written to: {out}")
