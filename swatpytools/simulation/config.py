"""Configuration dataclasses for SWAT multi-simulation ensemble runs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ParameterSpec:
    """Specification for a single SWAT parameter to vary across simulations.

    Attributes:
        name: Parameter name as it appears in SWAT files (e.g. ``"CN2"``).
        extension: File extension without dot (e.g. ``"mgt"``, ``"hru"``, ``"sol"``).
        method: Change method:

            - ``"v"`` — absolute value replacement (sample value used directly).
            - ``"r"`` — relative change: ``new = (1 + sample) * initial``.

        lower: Lower bound for LHS sampling.
        upper: Upper bound for LHS sampling.
        abs_min: Physical lower clamp applied after computing the new value.
        abs_max: Physical upper clamp applied after computing the new value.
    """

    name: str
    extension: str
    method: str
    lower: float
    upper: float
    abs_min: float = float("-inf")
    abs_max: float = float("inf")

    def __post_init__(self) -> None:
        self.name = self.name.upper()
        self.extension = self.extension.lstrip(".")
        if self.method not in ("v", "r"):
            raise ValueError(f"method must be 'v' or 'r', got '{self.method}'")
        if self.lower >= self.upper:
            raise ValueError(
                f"lower ({self.lower}) must be < upper ({self.upper}) "
                f"for parameter '{self.identifier}'"
            )

    @property
    def identifier(self) -> str:
        """SWAT-CUP style identifier, e.g. ``'CN2.mgt'``."""
        return f"{self.name}.{self.extension}"

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "extension": self.extension,
            "method": self.method,
            "lower": self.lower,
            "upper": self.upper,
            "abs_min": None if self.abs_min == float("-inf") else self.abs_min,
            "abs_max": None if self.abs_max == float("inf") else self.abs_max,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ParameterSpec":
        return cls(
            name=d["name"],
            extension=d["extension"],
            method=d["method"],
            lower=float(d["lower"]),
            upper=float(d["upper"]),
            abs_min=float(d["abs_min"]) if d.get("abs_min") is not None else float("-inf"),
            abs_max=float(d["abs_max"]) if d.get("abs_max") is not None else float("inf"),
        )

    @classmethod
    def from_swatcup_id(
        cls,
        swatcup_id: str,
        method: str,
        lower: float,
        upper: float,
        abs_min: float = float("-inf"),
        abs_max: float = float("inf"),
    ) -> "ParameterSpec":
        """Build from a SWAT-CUP style string like ``'CN2.mgt'``.

        Args:
            swatcup_id: ``'PARAMNAME.ext'`` string.
            method: ``'v'`` or ``'r'``.
            lower: Sampling lower bound.
            upper: Sampling upper bound.
            abs_min: Absolute physical minimum.
            abs_max: Absolute physical maximum.
        """
        parts = swatcup_id.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Expected 'NAME.ext' format, got '{swatcup_id}'")
        return cls(
            name=parts[0],
            extension=parts[1],
            method=method,
            lower=lower,
            upper=upper,
            abs_min=abs_min,
            abs_max=abs_max,
        )


@dataclass
class SimulationConfig:
    """Configuration for a SWAT multi-simulation ensemble run.

    Defines the parameter space to explore, simulation infrastructure, and
    output collection strategy.  Typically loaded from a JSON file so that
    runs are fully reproducible.

    Attributes:
        source_txtinout: Path to the master TxtInOut directory.  Must contain
            the SWAT executable (``swat_exe_name``).
        parameters: List of :class:`ParameterSpec` objects defining the
            parameter space.
        n_simulations: Number of LHS samples (= number of SWAT runs).
        work_dir: Parent directory for per-simulation TxtInOut copies.
        results_dir: Directory where output files are collected after each run.
        seed: Random seed for reproducible LHS sampling.  ``None`` = random.
        max_workers: Maximum parallel worker processes.  ``None`` = CPU count.
        delete_run_dirs: Remove per-simulation TxtInOut copies after collecting
            outputs.  Strongly recommended to conserve disk space.
        output_files: SWAT output files to collect from each simulation.
        swat_exe_name: Name of the SWAT executable within TxtInOut.
        timeout: Per-simulation timeout in seconds.  ``None`` = no limit.
        resume: Skip simulations where all ``output_files`` already exist in
            ``results_dir``.
    """

    source_txtinout: Path
    parameters: list[ParameterSpec]
    n_simulations: int = 500
    work_dir: Path = field(default_factory=lambda: Path("./simulation_runs"))
    results_dir: Path = field(default_factory=lambda: Path("./simulation_results"))
    seed: int | None = None
    max_workers: int | None = None
    delete_run_dirs: bool = True
    output_files: list[str] = field(
        default_factory=lambda: ["output.rch", "output.sub", "output.std", "watout.dat"]
    )
    swat_exe_name: str = "swat.exe"
    timeout: int | None = None
    resume: bool = True

    def __post_init__(self) -> None:
        self.source_txtinout = Path(self.source_txtinout)
        self.work_dir = Path(self.work_dir)
        self.results_dir = Path(self.results_dir)
        if self.max_workers is None:
            self.max_workers = os.cpu_count() or 1

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """Raise informative errors if the configuration is invalid."""
        if not self.source_txtinout.is_dir():
            raise FileNotFoundError(
                f"source_txtinout not found: {self.source_txtinout}"
            )
        exe = self.source_txtinout / self.swat_exe_name
        if not exe.exists():
            raise FileNotFoundError(
                f"SWAT executable not found: {exe}\n"
                f"Place '{self.swat_exe_name}' inside source_txtinout."
            )
        if not self.parameters:
            raise ValueError("parameters list is empty — add at least one ParameterSpec.")
        if self.n_simulations < 1:
            raise ValueError(f"n_simulations must be >= 1, got {self.n_simulations}")
        # Check for duplicate identifiers
        ids = [p.identifier for p in self.parameters]
        if len(ids) != len(set(ids)):
            from collections import Counter
            dupes = [k for k, v in Counter(ids).items() if v > 1]
            raise ValueError(f"Duplicate parameter identifiers: {dupes}")

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_txtinout": str(self.source_txtinout),
            "parameters": [p.to_dict() for p in self.parameters],
            "n_simulations": self.n_simulations,
            "work_dir": str(self.work_dir),
            "results_dir": str(self.results_dir),
            "seed": self.seed,
            "max_workers": self.max_workers,
            "delete_run_dirs": self.delete_run_dirs,
            "output_files": list(self.output_files),
            "swat_exe_name": self.swat_exe_name,
            "timeout": self.timeout,
            "resume": self.resume,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SimulationConfig":
        d = dict(d)
        d["parameters"] = [ParameterSpec.from_dict(p) for p in d["parameters"]]
        d["source_txtinout"] = Path(d["source_txtinout"])
        d["work_dir"] = Path(d["work_dir"])
        d["results_dir"] = Path(d["results_dir"])
        return cls(**d)

    def to_json(self, path: str | Path) -> Path:
        """Serialise config to a JSON file for reproducibility.

        Args:
            path: Output JSON path.

        Returns:
            Path to the written file.
        """
        path = Path(path)
        path.write_text(json.dumps(self.to_dict(), indent=2))
        return path

    @classmethod
    def from_json(cls, path: str | Path) -> "SimulationConfig":
        """Load config from a JSON file written by :meth:`to_json`.

        Args:
            path: Path to the JSON config file.
        """
        with open(path) as f:
            return cls.from_dict(json.load(f))
