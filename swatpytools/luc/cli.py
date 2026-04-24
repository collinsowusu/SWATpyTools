"""Command-line interface for SWAT LUC file generation.

Supports both individual flags and a JSON config file.  Individual flags
always take precedence over config file values.

Config file format (``luc_config.json``)::

    {
      "project_dir":   "./ArcSWAT_Project",
      "method":        "auto",
      "output_dir":    "./output",
      "lookup_table":  "./Tables/luc.txt",
      "updates": [
        {"year": 2016, "month": 1, "day": 1, "raster": "./Landuse_Rasters/NLCD_2016_PRJ.tif"},
        {"year": 2019, "month": 1, "day": 1, "raster": "./Landuse_Rasters/NLCD_2019_NEW.tif"}
      ]
    }
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from .config import LUCConfig
from .parsers import parse_hru_files, parse_lookup_table
from .spatial_raster import run_raster_method
from .spatial_shapefile import run_shapefile_method
from .validate import (
    compare_methods,
    compare_with_baseline,
    generate_summary_report,
    validate_hru_fr_sums,
)
from .writers import write_lup_dat


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="SWAT Land Use Change (LUC) File Generator",
        epilog=(
            "Generates lup.dat and HRU fraction files for SWAT's dynamic land use change module.\n\n"
            "Supply either --config (JSON file) or individual flags.\n"
            "Individual flags always override values from --config."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config", type=Path, default=None, metavar="JSON",
        help="JSON config file with all LUC settings (individual flags override)",
    )
    parser.add_argument(
        "--project-dir", default=None, type=Path,
        help="Path to the ArcSWAT project directory (contains Watershed/, Scenarios/)",
    )
    parser.add_argument(
        "--method", choices=["raster", "shapefile", "both", "auto"], default=None,
        help="Spatial overlay method (default: auto)",
    )
    parser.add_argument(
        "--update", action="append", nargs=4,
        metavar=("YEAR", "MONTH", "DAY", "RASTER_PATH"),
        help="Land use update: YEAR MONTH DAY /path/to/nlcd.tif (repeatable, overrides config)",
    )
    parser.add_argument(
        "--lookup-table", type=Path, default=None,
        help="Path to NLCD-to-SWAT lookup table (auto-detected if not specified)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Output directory for lup.dat and fileN.dat (default: ../output)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args(argv)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # ---- Load JSON config, then let CLI flags override ----------------
    cfg: dict = {}
    if args.config:
        if not args.config.exists():
            parser.error(f"Config file not found: {args.config}")
        with open(args.config) as f:
            cfg = json.load(f)
        logging.info("Loaded config from %s", args.config)

    project_dir = args.project_dir or (Path(cfg["project_dir"]) if "project_dir" in cfg else None)
    method      = args.method      or cfg.get("method", "auto")
    output_dir  = args.output_dir  or (Path(cfg["output_dir"]) if "output_dir" in cfg else None)
    lookup_arg  = args.lookup_table or (Path(cfg["lookup_table"]) if "lookup_table" in cfg else None)

    # CLI --update overrides config updates entirely (or use config updates)
    raw_updates = args.update or [
        (str(u["year"]), str(u["month"]), str(u["day"]), u["raster"])
        for u in cfg.get("updates", [])
    ]

    if project_dir is None:
        parser.error("--project-dir is required (or provide 'project_dir' in --config)")
    if not raw_updates:
        parser.error("At least one update is required (--update flag or 'updates' in --config)")

    # Parse update rasters
    update_rasters = []
    for year_s, month_s, day_s, raster_path in raw_updates:
        update_rasters.append((
            int(year_s), int(month_s), int(day_s), Path(raster_path)
        ))

    # Build configuration
    lookup_path = lookup_arg
    if lookup_path is None:
        # Try to auto-detect
        candidates = [
            project_dir.parent / "Tables" / "luc.txt",
            project_dir / "Tables" / "luc.txt",
        ]
        for candidate in candidates:
            if candidate.exists():
                lookup_path = candidate
                break
        if lookup_path is None:
            parser.error("Could not auto-detect lookup table. Use --lookup-table.")

    config = LUCConfig.from_project_dir(
        project_dir=project_dir,
        lookup_table_path=lookup_path,
        update_rasters=update_rasters,
        output_dir=output_dir,
        method=method,
        output_style="swat2009",
    )

    # Parse SWAT project
    logging.info("Parsing SWAT project files...")
    hrus = parse_hru_files(config.txtinout_dir)
    lookup = parse_lookup_table(config.lookup_table_path)

    raster_results = None
    shapefile_results = None

    # Run spatial methods
    if config.method in ("raster", "both"):
        logging.info("Running raster-based method...")
        raster_results = run_raster_method(config, hrus, lookup)

    if config.method in ("shapefile", "both"):
        logging.info("Running shapefile-based method...")
        shapefile_results = run_shapefile_method(config, hrus, lookup)

    # Select primary results for output
    primary_results = raster_results or shapefile_results
    primary_method = "raster" if raster_results else "shapefile"

    # Validate
    warnings = validate_hru_fr_sums(primary_results, hrus)
    if warnings:
        for w in warnings:
            logging.warning(w)

    # Write output files
    write_lup_dat(config.output_dir, primary_results, hrus)

    # Generate reports
    report = generate_summary_report(primary_results, hrus, primary_method)
    report_path = config.output_dir / "luc_summary_report.txt"
    with open(report_path, "w") as f:
        f.write(report)
    logging.info(f"Summary report written to {report_path}")
    print(report)

    # Compare methods if both were run
    if raster_results and shapefile_results:
        comparison_df = compare_methods(raster_results, shapefile_results, hrus)
        comparison_path = config.output_dir / "method_comparison.csv"
        comparison_df.to_csv(comparison_path, index=False)
        logging.info(f"Method comparison written to {comparison_path}")

    # Baseline comparison
    baseline_df = compare_with_baseline(primary_results, hrus, primary_method)
    baseline_path = config.output_dir / "baseline_comparison.csv"
    baseline_df.to_csv(baseline_path, index=False)
    logging.info(f"Baseline comparison written to {baseline_path}")

    logging.info("Done!")


if __name__ == "__main__":
    main()
