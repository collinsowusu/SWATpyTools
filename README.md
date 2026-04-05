# SWATpyTools — SWAT Helper Toolbox

A Python toolkit for preparing inputs, post-processing outputs, and calibrating
[SWAT](https://swat.tamu.edu/) (Soil and Water Assessment Tool) watershed models.

**Version:** 0.3.0  
**SWAT version tested against:** SWAT Dec 19, Ver 2022/Rev 687  
**Python:** 3.10+

---

## Table of Contents

1. [Installation](#installation)
2. [Package Structure](#package-structure)
3. [Module Reference](#module-reference)
   - [swatpytools.luc — Land Use Change](#swatpytoolsluc--land-use-change)
   - [swatpytools.simulation — Multi-Simulation Runner](#swatpytoolssimulation--multi-simulation-runner)
   - [swatpytools.outputs — Output Readers](#swatpytoolsoutputs--output-readers)
   - [swatpytools.metrics — Performance Metrics](#swatpytoolsmetrics--performance-metrics)
   - [swatpytools.viz — Visualization](#swatpytoolsviz--visualization)
   - [swatpytools.inputs — Input Preparation](#swatpytoolsinputs--input-preparation)
4. [Quick-Start Examples](#quick-start-examples)
5. [Project Layout](#project-layout)
6. [Notes on SWAT Output Formats](#notes-on-swat-output-formats)

---

## Installation

Clone or download the repository, then install dependencies:

```bash
pip install rasterio geopandas numpy pandas scipy plotly xarray rioxarray netCDF4
```

The package is used directly from the repository root (no `pip install` required):

```python
import sys
sys.path.insert(0, "/path/to/SWAT_LU_Updates")
import swatpytools
```

---

## Package Structure

```
swatpytools/
├── luc/          Land Use Change file generator (Pai & Saraswat 2011)
├── simulation/   LHS sampling + parallel multi-simulation ensemble runner
├── outputs/      SWAT output readers + NetCDF exporter
├── metrics/      Model performance metrics (NSE, KGE, PBIAS, RMSE, …)
├── viz/          Interactive Plotly visualizations
└── inputs/       Input preparation (soil rasters, parameter editor)
```

---

## Module Reference

---

### `swatpytools.luc` — Land Use Change

Generates `lup.dat` and `fileN.dat` HRU fraction files for activating SWAT's
dynamic Land Use Change (LUC) module, following the
[Pai & Saraswat (2011)](https://doi.org/10.13031/2013.38290) SWAT2009_LUC methodology.

#### Key concept

SWAT normally uses a single static land use layer. The LUC module lets you
supply updated NLCD rasters for specific years. At each update year, HRU
fractional areas (`HRU_FR`) are recomputed so land use transitions are reflected
in the simulation without redefining soils or slopes.

#### Spatial methods

| Method               | Description                                             | When to use                                   |
| -------------------- | ------------------------------------------------------- | --------------------------------------------- |
| `raster` _(default)_ | Pixel-level overlay of NLCD with HRU/soil/slope rasters | Preferred — faithful to Pai & Saraswat (2011) |
| `shapefile`          | Zonal statistics using HRU polygon boundaries           | Fallback when raster grids are missing        |
| `auto`               | Selects `raster` if grids exist, else `shapefile`       | Default when no `--method` is given           |
| `both`               | Runs both methods and writes a comparison CSV           | Useful for validation                         |

#### CLI usage

```bash
python -m swatpytools.luc \
  --project-dir ./ArcSWAT_Project \
  --update 2016 1 1 ./Landuse_Rasters/NLCD_2016_PRJ.tif \
  --update 2019 1 1 ./Landuse_Rasters/NLCD_2019_NEW.tif \
  --lookup-table ./Tables/luc.txt \
  --output-dir ./output
```

Options:

| Flag               | Default                  | Description                                                |
| ------------------ | ------------------------ | ---------------------------------------------------------- |
| `--project-dir`    | _(required)_             | ArcSWAT project root (contains `Watershed/`, `Scenarios/`) |
| `--update`         | _(required, repeatable)_ | `YEAR MONTH DAY /path/to/nlcd.tif`                         |
| `--lookup-table`   | auto-detected            | NLCD code → SWAT code mapping (`luc.txt`)                  |
| `--output-dir`     | `../output`              | Where to write `lup.dat` and `fileN.dat`                   |
| `--method`         | `auto`                   | `auto`, `raster`, `shapefile`, or `both`                   |
| `--verbose` / `-v` | off                      | Enable DEBUG logging                                       |

#### Python API

```python
from swatpytools.luc import LUCConfig, run_luc

config = LUCConfig.from_project_dir(
    project_dir="./ArcSWAT_Project",
    lookup_table_path="./Tables/luc.txt",
    update_rasters=[
        (2016, 1, 1, "./Landuse_Rasters/NLCD_2016_PRJ.tif"),
        (2019, 1, 1, "./Landuse_Rasters/NLCD_2019_NEW.tif"),
    ],
    output_dir="./output",
    method="auto",          # raster if grids exist, else shapefile
    output_style="swat2009",
)

results = run_luc(config)
# results["raster_results"]  → list of RedistributionResult
# results["shapefile_results"] → None (unless method="both")
# results["hrus"]             → list of HRUDefinition
```

#### Output files

| File                            | Description                                                         |
| ------------------------------- | ------------------------------------------------------------------- |
| `output/lup.dat`                | Update schedule read by SWAT (`UPDATE_NUM MONTH DAY YEAR FILENAME`) |
| `output/file1.dat`              | HRU fractional areas for update 1 (CSV: `HRU_ID, HRU_AREA`)         |
| `output/file2.dat`              | HRU fractional areas for update 2                                   |
| `output/luc_summary_report.txt` | Land use change summary per subbasin                                |
| `output/method_comparison.csv`  | Side-by-side raster vs shapefile comparison (method=`both`)         |

#### Enabling LUC in SWAT

After generating the output files, copy them to `TxtInOut/`. The `lup.dat` must reference the correct
filenames (`file1.dat`, `file2.dat`, …).

---

### `swatpytools.simulation` — Multi-Simulation Runner

Runs ensembles of SWAT simulations by varying parameters across a Latin
Hypercube sample set. Designed for post-calibration use: parameter ranges
come from SWAT-CUP (or equivalent) final calibrated bounds, and the module
executes all simulations in parallel and collects output files for
post-processing.

Typical use cases:

- **Uncertainty analysis** — quantify output variability across the calibrated
  parameter space
- **Sensitivity analysis** — identify which parameters drive output variance
- **Scenario ensembles** — compare multiple plausible parameter sets

#### Parameter change methods

| Code  | Method               | Formula                                    |
| ----- | -------------------- | ------------------------------------------ |
| `"v"` | Absolute replacement | `new_value = sample`                       |
| `"r"` | Relative change      | `new_value = (1 + sample) × initial_value` |

Both methods clamp the result to `[abs_min, abs_max]` physical bounds.

#### CLI usage

```bash
# Step 1 — generate samples and save to CSV
python -m swatpytools.simulation sample --config simulation_config.json

# Step 2 — run all simulations in parallel
python -m swatpytools.simulation run --config simulation_config.json --workers 8

# Check progress at any time
python -m swatpytools.simulation status --results-dir ./simulation_results
```

#### Config file format

Save as `simulation_config.json`:

```json
{
  "source_txtinout": "./ArcSWAT_Project/Scenarios/Default/TxtInOut",
  "n_simulations": 500,
  "seed": 42,
  "max_workers": 8,
  "delete_run_dirs": true,
  "work_dir": "./simulation_runs",
  "results_dir": "./simulation_results",
  "swat_exe_name": "swat.exe",
  "timeout": null,
  "resume": true,
  "output_files": ["output.rch", "output.sub", "output.std", "watout.dat"],
  "parameters": [
    {
      "name": "CN2",
      "extension": "mgt",
      "method": "r",
      "lower": -0.09,
      "upper": 0.2,
      "abs_min": 35,
      "abs_max": 98
    },
    {
      "name": "GW_REVAP",
      "extension": "gw",
      "method": "v",
      "lower": 0.01,
      "upper": 0.2,
      "abs_min": 0.02,
      "abs_max": 0.2
    },
    {
      "name": "ESCO",
      "extension": "hru",
      "method": "v",
      "lower": 0.0,
      "upper": 1.0,
      "abs_min": 0.0,
      "abs_max": 1.0
    },
    {
      "name": "CH_N2",
      "extension": "rte",
      "method": "v",
      "lower": 0.01,
      "upper": 0.1,
      "abs_min": -0.01,
      "abs_max": 0.3
    },
    {
      "name": "CH_K2",
      "extension": "rte",
      "method": "v",
      "lower": 60.0,
      "upper": 102.0,
      "abs_min": 60.0,
      "abs_max": 500.0
    },
    {
      "name": "ALPHA_BNK",
      "extension": "rte",
      "method": "v",
      "lower": 0.2,
      "upper": 1.0,
      "abs_min": 0.0,
      "abs_max": 1.0
    },
    {
      "name": "SOL_AWC",
      "extension": "sol",
      "method": "r",
      "lower": -0.2,
      "upper": 0.1,
      "abs_min": 0.0,
      "abs_max": 1.0
    },
    {
      "name": "HRU_SLP",
      "extension": "hru",
      "method": "r",
      "lower": -0.5,
      "upper": -0.1,
      "abs_min": 0.0,
      "abs_max": 1.0
    }
  ]
}
```

#### Python API

```python
from swatpytools.simulation import (
    ParameterSpec, SimulationConfig,
    generate_samples, run_simulations,
)

params = [
    ParameterSpec.from_swatcup_id("CN2.mgt",       "r", -0.09, 0.20, 35, 98),
    ParameterSpec.from_swatcup_id("GW_REVAP.gw",   "v",  0.01, 0.20, 0.02, 0.20),
    ParameterSpec.from_swatcup_id("ESCO.hru",       "v",  0.00, 1.00, 0.00, 1.00),
    ParameterSpec.from_swatcup_id("SOL_AWC.sol",    "r", -0.20, 0.10, 0.00, 1.00),
    # ... add remaining parameters
]

config = SimulationConfig(
    source_txtinout="./ArcSWAT_Project/Scenarios/Default/TxtInOut",
    parameters=params,
    n_simulations=500,
    seed=42,
    max_workers=8,
    delete_run_dirs=True,      # saves disk space — removes run copies after each sim
)

# Optional: generate and inspect samples before running
samples = generate_samples(config.parameters, config.n_simulations, config.seed)
print(samples.head())

# Run all simulations
log = run_simulations(config, samples)
print(log["status"].value_counts())
```

#### Output structure

```
simulation_results/
├── simulation_config.json   # Full config for exact reproducibility
├── samples.csv              # LHS parameter values (sim_id → param values)
├── params_manifest.csv      # Human-readable copy of samples.csv
├── run_log.csv              # sim_id, status, duration_s, error per run
├── sim_0000/
│   ├── output.rch           # Reach-level output (streamflow, sediment, nutrients)
│   ├── output.sub           # Subbasin water balance
│   ├── output.std           # Standard output summary
│   └── watout.dat           # Watershed water balance
├── sim_0001/
│   └── ...
└── sim_0499/
    └── ...
```

Use `swatpytools.outputs.read_reach` and `read_subbasin` to load individual
simulation results, and `swatpytools.metrics` to compute NSE/KGE/PBIAS across
the ensemble.

> **Disk space note:** Each SWAT simulation requires a full TxtInOut copy
> (~1,337 files). Set `delete_run_dirs=True` (default) to remove each copy
> immediately after output files are collected. Collected outputs are
> typically 5–20 MB per simulation.

> **Windows path length:** Keep `work_dir` near a drive root
> (e.g. `C:/swat_runs/`) to avoid the 260-character Windows path limit.

---

### `swatpytools.outputs` — Output Readers

Reads SWAT fixed-format output files into clean pandas DataFrames.

---

#### `read_reach(path, ...)` — `output.rch`

Reads streamflow, sediment, and nutrient outputs at the reach level.

```python
from swatpytools.outputs import read_reach

df = read_reach(
    path="TxtInOut/output.rch",
    reach=25,               # int or list of ints; None = all reaches
    start_date="1980-01-01",
    nyskip=3,               # warmup years (matches SWAT NYSKIP setting)
    annual=False,           # False = monthly rows; True = annual summary rows
)
```

Returns a DataFrame with columns:

| Column               | Units | Description                                           |
| -------------------- | ----- | ----------------------------------------------------- |
| `RCH`                | —     | Reach number                                          |
| `MON`                | —     | Month (1–12); 0 = annual summary                      |
| `DATE`               | —     | First day of month (added when `start_date` is given) |
| `AREA_km2`           | km²   | Drainage area                                         |
| `FLOW_IN`            | m³/s  | Flow entering reach                                   |
| `FLOW_OUT`           | m³/s  | Flow leaving reach                                    |
| `SED_IN` / `SED_OUT` | tons  | Sediment in/out                                       |
| `SEDCONC`            | mg/L  | Sediment concentration                                |
| `TOT_N` / `TOT_P`    | kg    | Total nitrogen / phosphorus                           |
| `WTMP`               | °C    | Water temperature                                     |
| _(+ 50 more)_        |       | Nutrients, pesticides, bacteria, salts                |

---

#### `read_subbasin(path, ...)` — `output.sub`

Reads water balance outputs at the subbasin level.

```python
from swatpytools.outputs import read_subbasin

df = read_subbasin(
    path="TxtInOut/output.sub",
    subbasin=[1, 5, 10],    # int or list; None = all subbasins
    start_date="1980-01-01",
    nyskip=3,
    annual=False,
)
```

Returns a DataFrame with columns:

| Column                 | Units | Description                                            |
| ---------------------- | ----- | ------------------------------------------------------ |
| `SUB`                  | —     | Subbasin number                                        |
| `MON`                  | —     | Month (1–12); 13 = annual summary                      |
| `AREA_ha`              | ha    | Subbasin area (decoded from SWAT's combined MON field) |
| `DATE`                 | —     | First day of month (added when `start_date` is given)  |
| `PRECIP`               | mm    | Precipitation                                          |
| `SNOWMELT`             | mm    | Snow melt                                              |
| `PET` / `ET`           | mm    | Potential / Actual evapotranspiration                  |
| `SW`                   | mm    | Soil water content                                     |
| `PERC`                 | mm    | Percolation to shallow aquifer                         |
| `SURQ`                 | mm    | Surface runoff                                         |
| `GWQ`                  | mm    | Groundwater contribution to streamflow                 |
| `WYLD`                 | mm    | Total water yield                                      |
| `SYLD`                 | t/ha  | Sediment yield                                         |
| _(+ nutrient columns)_ |       | ORGN, ORGP, NO3, LATNO3, GWNO3, TNO3, …                |

> **Note on MON encoding:** SWAT encodes the subbasin `MON` column as
> `timestep × 1000 + area_ha` (e.g. `1121.99` = month 1, area 121.99 ha).
> `read_subbasin` decodes this automatically into separate `MON` and `AREA_ha` columns.

---

#### `to_netcdf(df, shapefile, variables, output_path, ...)` — NetCDF export

Exports a SWAT output DataFrame to a CF-1.8 compliant spatiotemporal NetCDF file
by rasterizing subbasin or reach polygons.

```python
from swatpytools.outputs.netcdf import to_netcdf

to_netcdf(
    df=df,                                          # from read_subbasin() or read_reach()
    shapefile="Watershed/Shapes/subs1.shp",
    variables=["ET", "WYLD", "PRECIP", "SURQ"],
    output_path="./output/swat_monthly.nc",
    id_col="SUB",                                   # or "RCH" for reach output
    resolution=500.0,                               # metres; use ≥250 for subbasin-level
    crs="EPSG:32616",
    title="SWAT monthly water balance 1983-2019",
)
```

> **Resolution guidance:** Each subbasin has one value per timestep regardless of
> resolution. Use `resolution=250` or coarser for subbasin output — 30 m produces
> a ~9M pixel grid that is slow to write and unnecessarily large. The function
> warns automatically if the grid exceeds 1M pixels.

---

### `swatpytools.metrics` — Performance Metrics

Standard model evaluation metrics implemented in pure NumPy. NaN pairs are
automatically excluded from all calculations.

```python
from swatpytools.metrics import calc_scores, nse, kge, pbias

# All metrics at once
scores = calc_scores(simulated, observed)
# {'nse': 0.82, 'kge': 0.79, 'pbias': 4.5, 'rmse': 12.3,
#  'mae': 9.1, 'r_squared': 0.85, 'index_of_agreement': 0.94}

# Individual metrics
nse_val  = nse(sim, obs)
kge_val  = kge(sim, obs)
bias_pct = pbias(sim, obs)   # positive = underestimate, negative = overestimate
```

| Function             | Range    | Perfect | Acceptable (streamflow) |
| -------------------- | -------- | ------- | ----------------------- |
| `nse`                | (−∞, 1]  | 1       | > 0.5                   |
| `kge`                | (−∞, 1]  | 1       | > 0.5                   |
| `pbias`              | (−∞, +∞) | 0       | \|PBIAS\| < 25%         |
| `rmse`               | [0, +∞)  | 0       | —                       |
| `mae`                | [0, +∞)  | 0       | —                       |
| `r_squared`          | [0, 1]   | 1       | > 0.6                   |
| `index_of_agreement` | [0, 1]   | 1       | > 0.65                  |

---

### `swatpytools.viz` — Visualization

Interactive Plotly figures for model evaluation.

---

#### `plot_hydrograph(simulated, observed=None, ...)`

Simulated vs observed time series with optional performance metric annotation.

```python
from swatpytools.outputs import read_reach
from swatpytools.viz import plot_hydrograph

df = read_reach("TxtInOut/output.rch", reach=25,
                start_date="1980-01-01", nyskip=3)

fig = plot_hydrograph(
    simulated=df,
    observed="path/to/observed_flow.csv",  # CSV with Date and flow column
    variable="FLOW_OUT",
    units="m³/s",
    title="Reach 25 — Outlet Streamflow",
    show_metrics=True,   # annotates NSE, KGE, PBIAS, R² on plot
)
fig.show()                        # display in browser
fig.write_html("hydrograph.html") # save to file
```

The `observed` CSV must have a date column and a numeric flow column. Column
names are auto-detected.

---

#### `plot_fdc(simulated, observed=None, ...)`

Flow Duration Curve — shows what fraction of time a given flow is equalled
or exceeded. Standard diagnostic for high-flow and low-flow calibration.

```python
from swatpytools.viz import plot_fdc

fig = plot_fdc(
    simulated=df,
    observed="path/to/observed_flow.csv",
    variable="FLOW_OUT",
    units="m³/s",
    log_scale=True,             # recommended for flow
    exceedance_pct="weibull",   # plotting position: "weibull" or "standard"
)
fig.show()
```

---

### `swatpytools.inputs` — Input Preparation

---

#### `prepare_soil_raster(soil_dir, ...)` — SSURGO → GeoTIFF

Converts SSURGO soil shapefiles (downloaded from
[Web Soil Survey](https://websoilsurvey.sc.egov.usda.gov/)) into an integer
MUKEY GeoTIFF raster for use as the SWAT soil input layer.

```python
from swatpytools.inputs import prepare_soil_raster

# Mosaic all .shp files in the directory into one raster
out_path = prepare_soil_raster(
    soil_dir="./Soils/SSURGO",
    output_path="./Soils/soil_mukey.tif",
    crs="EPSG:32616",      # must match your DEM/land use CRS
    resolution=30.0,       # metres
    mosaic=True,           # merge all counties into one raster
)

# Or write one raster per shapefile
prepare_soil_raster("./Soils/SSURGO", mosaic=False)
```

The output raster has `int32` values equal to the MUKEY field (converted from
string to integer). Fill/NoData = 0 to avoid ArcSWAT integer conversion errors.

---

#### `read_param_file(path)` — Read SWAT parameter file

Reads any SWAT file using the `value | PARAM_NAME : description` format
(`.hru`, `.mgt`, `.gw`, `.rte`, `.sub`, `.pnd`, `.sep`) into a plain dict.

```python
from swatpytools.inputs import read_param_file

params = read_param_file("TxtInOut/000010001.hru")
# {'HRU_FR': 0.1222306, 'SLSUBBSN': 91.463, 'HRU_SLP': 0.043,
#  'OV_N': 0.1, 'ESCO': 0.95, 'EPCO': 1.0, ...}
```

Values are returned as `int` if the original has no decimal point, else `float`.

---

#### `write_param_file(path, updates, ...)` — Write updated parameters

Updates parameter values in a SWAT file while **preserving exact column widths,
decimal precision, and all comment/header lines**.

```python
from swatpytools.inputs import write_param_file

write_param_file(
    "TxtInOut/000010001.hru",
    updates={"ESCO": 0.85, "EPCO": 0.90},
    inplace=True,           # overwrite in place (default)
)

# Or write to a new file
write_param_file("TxtInOut/000010001.hru", {"ESCO": 0.85},
                 inplace=False, output_path="/tmp/modified.hru")
```

---

#### `update_param(path, param_name, new_value)` — Single parameter update

Convenience wrapper for updating one parameter at a time.

```python
from swatpytools.inputs import update_param

update_param("TxtInOut/000010001.hru", "ESCO", 0.75)
```

---

#### `batch_update(txtinout_dir, extension, updates, ...)` — Batch parameter update

Updates a parameter across **all files** with the given extension in `TxtInOut/`.
Essential for calibration workflows where a parameter applies watershed-wide.

```python
from swatpytools.inputs.params import batch_update

# Update CN2 in all 164 .mgt files
batch_update(
    txtinout_dir="TxtInOut/",
    extension="mgt",
    updates={"CN2": 72.0},
)

# Update ESCO in all .hru files, but only for HRUs 1–10
batch_update("TxtInOut/", "hru", {"ESCO": 0.80}, hru_filter=list(range(1, 11)))

# Preview changes without writing (dry run)
batch_update("TxtInOut/", "mgt", {"CN2": 72.0}, dry_run=True)
```

> **Supported file types:** `.hru`, `.mgt`, `.gw`, `.rte`, `.pnd`, `.sep`, `.sub`  
> **Not supported:** `.sol` (uses a different array-based format)

---

## Quick-Start Examples

### Example 1 — Full LUC workflow

```python
from swatpytools.luc import LUCConfig, run_luc
from swatpytools.luc.validate import validate_hru_fr_sums, generate_summary_report
from swatpytools.luc.writers import write_lup_dat

config = LUCConfig.from_project_dir(
    project_dir="./ArcSWAT_Project",
    lookup_table_path="./Tables/luc.txt",
    update_rasters=[
        (2016, 1, 1, "./Landuse_Rasters/NLCD_2016_PRJ.tif"),
        (2019, 1, 1, "./Landuse_Rasters/NLCD_2019_NEW.tif"),
    ],
    output_dir="./output",
)

results = run_luc(config)
hrus = results["hrus"]
raster_results = results["raster_results"]

warnings = validate_hru_fr_sums(raster_results, hrus)
print(generate_summary_report(raster_results, hrus, "raster"))
write_lup_dat(config.output_dir, raster_results, hrus)
```

---

### Example 2 — Post-processing and calibration evaluation

```python
from swatpytools.outputs import read_reach, read_subbasin
from swatpytools.metrics import calc_scores
from swatpytools.viz import plot_hydrograph, plot_fdc

# Load outputs
rch = read_reach("TxtInOut/output.rch", reach=25,
                 start_date="1980-01-01", nyskip=3)

# Compare against observed (CSV with columns: Date, Flow_cms)
scores = calc_scores(
    simulated=rch["FLOW_OUT"].values,
    observed=observed_df["Flow_cms"].values,
)
print(f"NSE={scores['nse']}  KGE={scores['kge']}  PBIAS={scores['pbias']}%")

# Visualize
fig = plot_hydrograph(rch, observed="observed_flow.csv",
                      variable="FLOW_OUT", units="m³/s")
fig.show()

fig_fdc = plot_fdc(rch, observed="observed_flow.csv", variable="FLOW_OUT")
fig.write_html("fdc.html")
```

---

### Example 3 — Water balance summary

```python
from swatpytools.outputs import read_subbasin

sub = read_subbasin("TxtInOut/output.sub", start_date="1980-01-01", nyskip=3)

# Annual watershed-average water balance
annual = sub.groupby(sub["DATE"].dt.year)[
    ["PRECIP", "ET", "WYLD", "PERC", "SURQ", "GWQ"]
].sum()
print(annual.mean().round(1))
# PRECIP   1474.7
# ET        774.1
# WYLD      676.4   ← SURQ + GWQ + LATQ
# PERC      386.4
```

---

### Example 4 — Multi-simulation ensemble run

```python
from swatpytools.simulation import (
    ParameterSpec, SimulationConfig, generate_samples, run_simulations,
)

# Define parameter space using SWAT-CUP final calibrated ranges
params = [
    ParameterSpec.from_swatcup_id("CN2.mgt",       "r", -0.09, 0.20, 35, 98),
    ParameterSpec.from_swatcup_id("GW_REVAP.gw",   "v",  0.01, 0.20, 0.02, 0.20),
    ParameterSpec.from_swatcup_id("ESCO.hru",       "v",  0.00, 1.00, 0.00, 1.00),
    ParameterSpec.from_swatcup_id("CH_N2.rte",      "v",  0.01, 0.10, -0.01, 0.30),
    ParameterSpec.from_swatcup_id("SOL_AWC.sol",    "r", -0.20, 0.10, 0.00, 1.00),
]

config = SimulationConfig(
    source_txtinout="./ArcSWAT_Project/Scenarios/Default/TxtInOut",
    parameters=params,
    n_simulations=500,
    seed=42,
    max_workers=8,            # parallel SWAT processes
    delete_run_dirs=True,     # remove TxtInOut copies after each run
    results_dir="./simulation_results",
)

# Generate samples, then run
samples = generate_samples(config.parameters, config.n_simulations, config.seed)
log = run_simulations(config, samples)

# Check results
print(log["status"].value_counts())
# completed    497
# partial        3
```

---

### Example 5 — Batch parameter update (single run)

```python
from swatpytools.inputs.params import batch_update

# Increase CN2 by multiplying: read all values, apply factor, write back
from swatpytools.inputs.params import read_param_file, update_param
from pathlib import Path

for mgt in Path("TxtInOut/").glob("*.mgt"):
    params = read_param_file(mgt)
    new_cn2 = round(params["CN2"] * 1.05, 2)   # +5%
    update_param(mgt, "CN2", new_cn2)
```

---

### Example 6 — Export monthly ET to NetCDF for GIS/visualization

```python
from swatpytools.outputs import read_subbasin
from swatpytools.outputs.netcdf import to_netcdf

sub = read_subbasin("TxtInOut/output.sub", start_date="1980-01-01", nyskip=3)

to_netcdf(
    df=sub,
    shapefile="Watershed/Shapes/subs1.shp",
    variables=["ET", "WYLD", "PRECIP", "SURQ"],
    output_path="./output/swat_water_balance.nc",
    id_col="SUB",
    resolution=500.0,        # 500 m recommended for subbasin-level output
    title="SWAT monthly water balance 1983-2019",
)
```

---

## Notes on SWAT Output Formats

### `output.rch` column alignment

The header row has some columns concatenated without spaces (e.g.
`SOLPST_INmgSOLPST_OUTmg`) and others with spaces in the name (`TOT Nkg`).
`read_reach` uses hardcoded column names rather than parsing the header — do not
rely on the raw header for column indexing.

### `output.sub` MON encoding

SWAT stores the MON column in `output.sub` as a single float combining the
timestep index and subbasin area:

```
MON_value = timestep * 1000 + area_ha
1121.99   → month = 1,  area = 121.99 ha
13121.99  → month = 13 (annual summary), area = 121.99 ha
```

`read_subbasin` decodes this automatically. Do not use the raw `MON_AREA`
column directly — use the decoded `MON` and `AREA_ha` columns instead.

### LUC module accuracy

The raster method (pixel-level overlay) is more accurate than the shapefile
method because it captures soil and slope variation within each HRU polygon,
consistent with how SWAT originally delineated HRUs. Cross-method R² ≥ 0.999
has been observed for this watershed, but the two methods can diverge by up to
~2.7 percentage points for individual HRUs in a given update year.
