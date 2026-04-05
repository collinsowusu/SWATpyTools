"""Latin Hypercube Sampling for SWAT calibration parameter spaces.

Uses ``scipy.stats.qmc.LatinHypercube`` to generate well-distributed samples
across the parameter space defined by a list of :class:`~.config.ParameterSpec`
objects.  Samples are returned as a :class:`pandas.DataFrame` with columns
named by the SWAT-CUP-style parameter identifier (e.g. ``CN2.mgt``) and a
zero-based integer ``sim_id`` index.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from scipy.stats import qmc

if TYPE_CHECKING:
    from .config import ParameterSpec


def generate_samples(
    parameters: list["ParameterSpec"],
    n_samples: int,
    seed: int | None = None,
) -> pd.DataFrame:
    """Generate Latin Hypercube samples across the given parameter space.

    Args:
        parameters: Ordered list of :class:`~.config.ParameterSpec` objects.
            Column order in the returned DataFrame matches this list.
        n_samples: Number of samples to generate.
        seed: Integer seed for reproducibility.  ``None`` gives a random draw.

    Returns:
        DataFrame of shape ``(n_samples, len(parameters))`` with columns
        named by ``parameter.identifier`` and index named ``sim_id``
        (0-based integers).  Values are rounded to 6 significant figures.

    Example::

        from swatpytools.calibration import ParameterSpec, generate_samples

        params = [
            ParameterSpec.from_swatcup_id("CN2.mgt",  "r", -0.09, 0.2, 35, 98),
            ParameterSpec.from_swatcup_id("ESCO.hru", "v", 0.0,  1.0,  0,  1),
        ]
        df = generate_samples(params, n_samples=500, seed=42)
    """
    n_params = len(parameters)
    if n_params == 0:
        raise ValueError("parameters list must not be empty")

    sampler = qmc.LatinHypercube(d=n_params, seed=seed)
    unit_samples = sampler.random(n=n_samples)  # shape (n_samples, n_params)

    l_bounds = [p.lower for p in parameters]
    u_bounds = [p.upper for p in parameters]
    scaled = qmc.scale(unit_samples, l_bounds, u_bounds)

    columns = [p.identifier for p in parameters]
    df = pd.DataFrame(np.round(scaled, 6), columns=columns)
    df.index.name = "sim_id"
    return df


def save_samples(df: pd.DataFrame, path: str | Path) -> Path:
    """Save a sample DataFrame to CSV.

    The CSV includes the ``sim_id`` index column and parameter identifier
    headers, making it human-readable and loadable with :func:`load_samples`.

    Args:
        df: Sample DataFrame returned by :func:`generate_samples`.
        path: Output file path.

    Returns:
        Resolved path to the written CSV file.
    """
    path = Path(path)
    df.to_csv(path, index=True)
    return path.resolve()


def load_samples(path: str | Path) -> pd.DataFrame:
    """Load a sample DataFrame from a CSV written by :func:`save_samples`.

    Args:
        path: Path to the CSV file.

    Returns:
        DataFrame with integer ``sim_id`` as index.
    """
    df = pd.read_csv(path, index_col="sim_id")
    df.index = df.index.astype(int)
    return df
