"""Hydrograph and flow duration curve visualization for SWAT outputs.

Produces interactive Plotly figures comparing simulated vs observed data
and standard calibration diagnostics.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def plot_hydrograph(
    simulated: pd.Series | pd.DataFrame,
    observed: pd.Series | pd.DataFrame | str | Path | None = None,
    variable: str = "FLOW_OUT",
    title: str | None = None,
    units: str | None = None,
    sim_label: str = "Simulated",
    obs_label: str = "Observed",
    show_metrics: bool = True,
    date_col: str = "DATE",
) -> "plotly.graph_objs.Figure":
    """Plot a simulated vs observed hydrograph with performance metrics.

    Args:
        simulated: DataFrame with a DATE column and the target variable, or a
            pre-indexed Series (index = dates, values = simulated flow).
        observed: DataFrame/Series of observed data, or a path to a CSV with
            columns [date_col, variable]. None = simulated-only plot.
        variable: Column name for the variable to plot (default 'FLOW_OUT').
        title: Plot title. Auto-generated if None.
        units: Units string for the y-axis label (e.g. 'm³/s').
        sim_label: Legend label for simulated line.
        obs_label: Legend label for observed line.
        show_metrics: Compute and annotate NSE, KGE, PBIAS on the figure.
        date_col: Name of the date column when simulated/observed are DataFrames.

    Returns:
        Plotly Figure. Call .show() to display or .write_html() to export.
    """
    try:
        import plotly.graph_objs as go
    except ImportError:
        raise ImportError("plotly is required for visualization: pip install plotly")

    from swatpytools.metrics.stats import calc_scores

    # --- Normalize simulated input ---
    if isinstance(simulated, pd.DataFrame):
        sim_series = simulated.set_index(date_col)[variable]
    else:
        sim_series = simulated.copy()
        sim_series.name = variable
    sim_series.index = pd.to_datetime(sim_series.index)

    # --- Normalize observed input ---
    obs_series = None
    if observed is not None:
        if isinstance(observed, (str, Path)):
            obs_df = pd.read_csv(observed)
            obs_df[date_col] = pd.to_datetime(obs_df[date_col])
            obs_series = obs_df.set_index(date_col).iloc[:, 0]
        elif isinstance(observed, pd.DataFrame):
            obs_series = observed.set_index(date_col).iloc[:, 0]
        else:
            obs_series = observed.copy()
        obs_series.index = pd.to_datetime(obs_series.index)

    # --- Build figure ---
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=sim_series.index,
        y=sim_series.values,
        name=sim_label,
        mode="lines",
        line=dict(color="red", width=1.5),
    ))

    scores = None
    if obs_series is not None:
        # Align on common dates
        common_idx = sim_series.index.intersection(obs_series.index)
        if len(common_idx) == 0:
            logger.warning("No overlapping dates between simulated and observed data")
        else:
            sim_aligned = sim_series.loc[common_idx].values
            obs_aligned = obs_series.loc[common_idx].values

            fig.add_trace(go.Scatter(
                x=obs_series.index,
                y=obs_series.values,
                name=obs_label,
                mode="lines",
                line=dict(color="blue", width=1.5),
            ))

            if show_metrics:
                scores = calc_scores(sim_aligned, obs_aligned)

    # --- Layout ---
    y_label = f"<b>{variable}"
    if units:
        y_label += f" ({units})"
    y_label += "</b>"

    plot_title = title or f"{variable} Hydrograph"
    if scores:
        metric_str = "  |  ".join(
            f"{k.upper()}={v}" for k, v in scores.items()
            if k in ("nse", "kge", "pbias", "r_squared")
        )
        plot_title += f"<br><sup>{metric_str}</sup>"

    fig.update_layout(
        title=dict(text=f"<b>{plot_title}</b>", x=0.5, font=dict(family="Arial", size=16)),
        template="plotly_white",
        xaxis=dict(title="<b>Date</b>", linecolor="black"),
        yaxis=dict(title=y_label, linecolor="black"),
        font_family="Arial",
        legend=dict(traceorder="reversed"),
    )

    if scores:
        logger.info("Performance metrics: %s", scores)

    return fig


def plot_fdc(
    simulated: pd.Series | pd.DataFrame,
    observed: pd.Series | pd.DataFrame | str | Path | None = None,
    variable: str = "FLOW_OUT",
    title: str | None = None,
    units: str | None = None,
    log_scale: bool = True,
    exceedance_pct: Literal["standard", "weibull"] = "weibull",
    date_col: str = "DATE",
) -> "plotly.graph_objs.Figure":
    """Plot a Flow Duration Curve (FDC) for simulated and optionally observed flow.

    The FDC shows the percentage of time a given flow value is equalled or exceeded.
    It is a standard calibration diagnostic for SWAT and other rainfall-runoff models.

    Args:
        simulated: DataFrame with DATE and variable columns, or a pre-indexed Series.
        observed: Observed flow data (same formats as simulated). Optional.
        variable: Column to use (default 'FLOW_OUT').
        title: Plot title. Auto-generated if None.
        units: Y-axis units string.
        log_scale: Use log scale on y-axis (recommended for flow, default True).
        exceedance_pct: Plotting position formula:
            'weibull' (default): P = rank / (n + 1)
            'standard': P = rank / n
        date_col: Date column name when inputs are DataFrames.

    Returns:
        Plotly Figure.
    """
    try:
        import plotly.graph_objs as go
    except ImportError:
        raise ImportError("plotly is required for visualization: pip install plotly")

    def _to_series(data, var, dcol):
        if isinstance(data, pd.DataFrame):
            return data.set_index(dcol)[var]
        return data.copy()

    def _exceedance(series, method):
        vals = np.sort(series.dropna().values)[::-1]  # descending
        n = len(vals)
        if method == "weibull":
            probs = np.arange(1, n + 1) / (n + 1) * 100
        else:
            probs = np.arange(1, n + 1) / n * 100
        return probs, vals

    sim_series = _to_series(simulated, variable, date_col)
    sim_probs, sim_vals = _exceedance(sim_series, exceedance_pct)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sim_probs, y=sim_vals,
        name="Simulated",
        mode="lines",
        line=dict(color="red", width=1.5),
    ))

    if observed is not None:
        if isinstance(observed, (str, Path)):
            obs_df = pd.read_csv(observed)
            obs_df[date_col] = pd.to_datetime(obs_df[date_col])
            obs_series = obs_df.set_index(date_col).iloc[:, 0]
        else:
            obs_series = _to_series(observed, variable, date_col)

        obs_probs, obs_vals = _exceedance(obs_series, exceedance_pct)
        fig.add_trace(go.Scatter(
            x=obs_probs, y=obs_vals,
            name="Observed",
            mode="lines",
            line=dict(color="blue", width=1.5),
        ))

    y_label = f"<b>{variable}"
    if units:
        y_label += f" ({units})"
    y_label += "</b>"

    fig.update_layout(
        title=dict(
            text=f"<b>{title or 'Flow Duration Curve'}</b>",
            x=0.5,
            font=dict(family="Arial", size=16),
        ),
        template="plotly_white",
        xaxis=dict(
            title="<b>Exceedance Probability (%)</b>",
            linecolor="black",
            range=[0, 100],
        ),
        yaxis=dict(
            title=y_label,
            linecolor="black",
            type="log" if log_scale else "linear",
        ),
        font_family="Arial",
    )

    return fig
