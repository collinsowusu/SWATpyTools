"""Configuration for SWAT Land Use Change file generation."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple


# Slope raster code to slope class string mapping
SLOPE_CODE_MAP = {1: "0-2", 2: "2-6", 999: "6-9999"}


@dataclass
class LUCConfig:
    """Configuration for the SWAT LUC file generator.

    Attributes:
        project_dir: Root of the ArcSWAT project (contains Watershed/, Scenarios/).
        txtinout_dir: Path to Scenarios/Default/TxtInOut/.
        hru_raster_path: Path to the HRU raster grid directory (hrus1/).
        hru_shapefile_path: Path to hru1.shp.
        soil_raster_path: Path to landsoils1/ grid directory.
        slope_raster_path: Path to landslope1/ grid directory.
        base_landuse_raster_path: Path to landuse1/ grid directory (base NLCD).
        hru_report_path: Path to HRULandUseSoilsReport.txt.
        lookup_table_path: Path to NLCD-to-SWAT lookup table.
        update_rasters: List of (year, month, day, raster_path) for each LUC update.
        output_dir: Directory where lup.dat and fileN.dat will be written.
        method: Spatial overlay method - "auto" (default), "raster", "shapefile", or "both".
            "auto" selects "raster" if all grid files exist, otherwise falls back to "shapefile".
        output_style: "swat2012" (inline lup.dat) or "swat2009" (separate fileN.dat).
    """

    project_dir: Path
    txtinout_dir: Path = None
    hru_raster_path: Path = None
    hru_shapefile_path: Path = None
    soil_raster_path: Path = None
    slope_raster_path: Path = None
    base_landuse_raster_path: Path = None
    hru_report_path: Path = None
    lookup_table_path: Path = None
    update_rasters: List[Tuple[int, int, int, Path]] = field(default_factory=list)
    output_dir: Path = None
    method: str = "auto"
    output_style: str = "swat2012"

    def __post_init__(self):
        self.project_dir = Path(self.project_dir)
        if self.update_rasters:
            self.update_rasters = [
                (y, m, d, Path(p)) for y, m, d, p in self.update_rasters
            ]
            self.update_rasters.sort(key=lambda x: (x[0], x[1], x[2]))

    @classmethod
    def from_project_dir(
        cls,
        project_dir: str | Path,
        lookup_table_path: str | Path,
        update_rasters: List[Tuple[int, int, int, str | Path]],
        output_dir: str | Path = None,
        method: str = "auto",
        output_style: str = "swat2012",
    ) -> "LUCConfig":
        """Auto-discover paths from standard ArcSWAT project layout."""
        project_dir = Path(project_dir)
        watershed = project_dir / "Watershed"

        config = cls(
            project_dir=project_dir,
            txtinout_dir=project_dir / "Scenarios" / "Default" / "TxtInOut",
            hru_raster_path=watershed / "Grid" / "hrus1",
            hru_shapefile_path=watershed / "Shapes" / "hru1.shp",
            soil_raster_path=watershed / "Grid" / "landsoils1",
            slope_raster_path=watershed / "Grid" / "landslope1",
            base_landuse_raster_path=watershed / "Grid" / "landuse1",
            hru_report_path=watershed / "text" / "HRULandUseSoilsReport.txt",
            lookup_table_path=Path(lookup_table_path),
            update_rasters=update_rasters,
            output_dir=Path(output_dir) if output_dir else project_dir.parent / "output",
            method=method,
            output_style=output_style,
        )
        config._resolve_method()
        config._validate_paths()
        return config

    def _resolve_method(self):
        """Resolve 'auto' method: use raster if grids exist, else fall back to shapefile."""
        import logging
        if self.method != "auto":
            return
        raster_grids = [self.hru_raster_path, self.soil_raster_path, self.slope_raster_path]
        if all(p is not None and p.exists() for p in raster_grids):
            self.method = "raster"
            logging.info("Auto-selected method: raster (all grid files found)")
        else:
            self.method = "shapefile"
            missing = [str(p) for p in raster_grids if p is None or not p.exists()]
            logging.warning(
                "Auto-selected method: shapefile (raster grids missing: %s)",
                ", ".join(missing),
            )

    def _validate_paths(self):
        """Check that required input paths exist."""
        required = [
            ("project_dir", self.project_dir),
            ("txtinout_dir", self.txtinout_dir),
            ("hru_shapefile_path", self.hru_shapefile_path),
            ("lookup_table_path", self.lookup_table_path),
        ]
        if self.method in ("raster", "both"):
            required.extend([
                ("hru_raster_path", self.hru_raster_path),
                ("soil_raster_path", self.soil_raster_path),
                ("slope_raster_path", self.slope_raster_path),
            ])

        missing = [(name, path) for name, path in required if not path.exists()]
        if missing:
            details = "\n".join(f"  {name}: {path}" for name, path in missing)
            raise FileNotFoundError(f"Required paths not found:\n{details}")

        for year, month, day, raster_path in self.update_rasters:
            if not raster_path.exists():
                raise FileNotFoundError(
                    f"Update raster for {year}-{month:02d}-{day:02d} not found: {raster_path}"
                )
