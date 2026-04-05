"""SWAT multi-simulation ensemble runner.

Generates a Latin Hypercube parameter sample set and executes a SWAT
simulation for each sample in parallel, collecting output files for
post-processing.

Intended use case
-----------------
Running ensembles of SWAT simulations using final calibrated parameter
ranges from SWAT-CUP (or similar calibration tools) to support:

- Uncertainty / sensitivity analysis
- Multi-scenario comparison
- Validation across parameter space

Workflow
--------
1. Define varied parameters as :class:`ParameterSpec` objects.
2. Build a :class:`SimulationConfig` (or load one from JSON).
3. Generate samples with :func:`generate_samples`.
4. Run all simulations with :func:`run_simulations`.
5. Post-process collected outputs from ``results_dir/sim_XXXX/``.

Quick start::

    from swatpytools.simulation import (
        ParameterSpec, SimulationConfig,
        generate_samples, run_simulations,
    )

    params = [
        ParameterSpec.from_swatcup_id("CN2.mgt",       "r", -0.09, 0.20, 35, 98),
        ParameterSpec.from_swatcup_id("GW_REVAP.gw",   "v",  0.01, 0.20, 0.02, 0.20),
        ParameterSpec.from_swatcup_id("ESCO.hru",       "v",  0.00, 1.00, 0.00, 1.00),
        ParameterSpec.from_swatcup_id("CH_N2.rte",      "v",  0.01, 0.10, -0.01, 0.30),
        ParameterSpec.from_swatcup_id("CH_K2.rte",      "v", 60.00, 102.0, 60.0, 500.0),
        ParameterSpec.from_swatcup_id("ALPHA_BNK.rte",  "v",  0.20, 1.00, 0.00, 1.00),
        ParameterSpec.from_swatcup_id("SOL_AWC.sol",    "r", -0.20, 0.10, 0.00, 1.00),
        ParameterSpec.from_swatcup_id("HRU_SLP.hru",    "r", -0.50,-0.10, 0.00, 1.00),
    ]

    config = SimulationConfig(
        source_txtinout="./ArcSWAT_Project/Scenarios/Default/TxtInOut",
        parameters=params,
        n_simulations=500,
        seed=42,
        max_workers=8,
        delete_run_dirs=True,
        work_dir="./simulation_runs",
        results_dir="./simulation_results",
    )

    samples = generate_samples(config.parameters, config.n_simulations, config.seed)
    log = run_simulations(config, samples)

CLI
---
::

    python -m swatpytools.simulation sample --config config.json
    python -m swatpytools.simulation run    --config config.json
    python -m swatpytools.simulation status --results-dir ./simulation_results

Output layout
-------------
After a successful run::

    simulation_results/
    ├── simulation_config.json   # Full config for reproducibility
    ├── samples.csv              # Parameter values for every sim_id
    ├── params_manifest.csv      # Same as samples.csv (human-readable copy)
    ├── run_log.csv              # sim_id, status, duration_s, error
    ├── sim_0000/
    │   ├── output.rch
    │   ├── output.sub
    │   ├── output.std
    │   └── watout.dat
    ├── sim_0001/
    │   └── ...
    └── ...
"""

from .config import ParameterSpec, SimulationConfig
from .params_handler import apply_sample_to_dir
from .runner import run_simulations
from .sampling import generate_samples, load_samples, save_samples

__all__ = [
    "ParameterSpec",
    "SimulationConfig",
    "generate_samples",
    "save_samples",
    "load_samples",
    "run_simulations",
    "apply_sample_to_dir",
]
