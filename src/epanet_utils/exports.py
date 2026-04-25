"""
Producer-side helpers used by NEER Console / WRM API to derive consumer-shaped
artifacts from EPANET .inp / .rpt / .out files:

- decode_to_data_json    — split an .inp into spatial vs editable non-spatial state.
                           The non-spatial dict (data.json) is the authoring surface
                           the UI/agent edits.
- encode_with_overlay    — render a complete .inp from an immutable source.inp +
                           the edited non-spatial overlay. Used at simulation
                           submit time and for "Download .inp".
- emit_report_json       — parse .rpt (and optionally cross-reference .out) into
                           a structured report.json: balances, warnings, errors,
                           hydraulic status log, summary stats.
- emit_results_zarr      — write the full time-series (feature × period × metric)
                           cube to a Zarr v3 store. Features are pre-sorted by a
                           Z-order space-filling curve on coordinates so the
                           feature-axis chunks are spatially coherent (cheap
                           viewport-aware loads on the client).

Spatial vs non-spatial split: the spatial sections (junctions, reservoirs,
tanks, pipes, pumps, valves, coordinates, vertices) describe network geometry
and are owned by the spatial pipeline (PMTiles + LayerFeature DB rows). The
remaining sections describe behavior/configuration and are owned by data.json.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

from .inp_decoder import EpanetInputDecoder
from .inp_encoder import EpanetInputEncoder
from .out import EpanetOutput
from .rpt import EpanetReport


PathLike = Union[str, Path]


# Sections describing geometry / position. These live in PMTiles + LayerFeature
# rows in Console, NOT in data.json. They are read from source.inp at encode
# time and never modified through the data.json overlay.
SPATIAL_SECTIONS: frozenset = frozenset({
    "junctions",
    "reservoirs",
    "tanks",
    "pipes",
    "pumps",
    "valves",
    "coordinates",
    "vertices",
})

# Sections that describe behavior / configuration. These are the editable
# surface in data.json. The keys here mirror EpanetInputDecoder's output dict
# keys (lowercase, snake-section style).
NON_SPATIAL_SECTIONS: frozenset = frozenset({
    "title",
    "tags",
    "demands",
    "status",
    "patterns",
    "curves",
    "controls",
    "rules",
    "energy",
    "emitters",
    "quality",
    "sources",
    "reactions",
    "mixing",
    "times",
    "report",
    "options",
    "labels",
    "backdrop",
})


# ---------------------------------------------------------------------------
# .inp ↔ data.json
# ---------------------------------------------------------------------------

def decode_to_data_json(inp_path: PathLike) -> Dict[str, Any]:
    """
    Extract the non-spatial sections of an EPANET .inp file as a serializable
    dict suitable for writing as `data.json` on S3.

    Spatial sections (junctions, pipes, coordinates, etc.) are excluded and
    must be carried by the spatial pipeline (PMTiles + LayerFeature).

    Args:
        inp_path: Path to a source .inp file.

    Returns:
        A dict whose keys are a subset of NON_SPATIAL_SECTIONS. Sections that
        the source .inp does not contain are omitted (not present as None).
        Suitable for `json.dumps(...)` directly.
    """
    decoder = EpanetInputDecoder()
    full = decoder.decode_inp(Path(inp_path))

    data: Dict[str, Any] = {}
    for section in NON_SPATIAL_SECTIONS:
        if section in full and full[section] not in (None, "", [], {}):
            data[section] = full[section]
    return data


def encode_with_overlay(
    source_inp_path: PathLike,
    data_overlay: Dict[str, Any],
) -> str:
    """
    Render a complete .inp file by overlaying the editable non-spatial
    sections (`data_overlay`) onto an immutable source `.inp`.

    The spatial sections of the source .inp are preserved verbatim; only the
    non-spatial sections present in `data_overlay` are replaced. Sections in
    the overlay that are NOT in NON_SPATIAL_SECTIONS are ignored (callers
    cannot smuggle spatial edits through this path).

    Args:
        source_inp_path: Path to the immutable source .inp.
        data_overlay:    Editable non-spatial sections, shape matching
                         `decode_to_data_json`'s output.

    Returns:
        The rendered .inp file content as a single string.
    """
    decoder = EpanetInputDecoder()
    encoder = EpanetInputEncoder()

    model = decoder.decode_inp(Path(source_inp_path))

    for section, value in data_overlay.items():
        if section not in NON_SPATIAL_SECTIONS:
            # Silently ignore — spatial edits don't go through this path.
            continue
        model[section] = value

    return encoder.encode_to_inp_string(model)


# ---------------------------------------------------------------------------
# .rpt → report.json
# ---------------------------------------------------------------------------

def emit_report_json(
    rpt_path: PathLike,
    out_path: Optional[PathLike] = None,
) -> Dict[str, Any]:
    """
    Produce the consumer-shaped `report.json` from an EPANET .rpt (and
    optionally cross-referenced with the binary .out for richer summaries).

    The shape is designed for direct consumption by Console's Results viewer
    (status strip, warnings panel, summary tables) without any client-side
    parsing of EPANET-specific text.

    Args:
        rpt_path: Path to the EPANET .rpt file.
        out_path: Optional path to the binary .out for per-feature summaries.

    Returns:
        Dict with keys: version, timestamps, balances, energy, warnings[],
        errors[], status_log[], summary[], metrics descriptor.
    """
    with EpanetReport(rpt_path) as report:
        out: Dict[str, Any] = {
            "version": report.version,
            "timestamps": {
                "analysis_begun": report.analysis_begun,
                "analysis_ended": report.analysis_ended,
            },
            "flow_balance": report.flow_balance,
            "quality_balance": report.quality_balance,
            "energy": report.energy_usage,
            "warnings": report.warnings,
            "errors": report.errors,
            "status_log": report.hydraulic_status,
            "has_warnings": report.has_warnings(),
            "has_errors": report.has_errors(),
            "summary": report.summary(),
        }

    if out_path is not None:
        out["per_feature_summary"] = _per_feature_summary(out_path)

    out["metrics"] = {
        "node": ["pressure", "head", "demand", "quality"],
        "link": ["flow", "velocity", "headloss", "status", "setting"],
    }
    return out


def _per_feature_summary(out_path: PathLike) -> Dict[str, Any]:
    """
    Compute min / max / mean / argmin / argmax per metric per feature from
    the binary .out time series. Tiny — proportional to (features × metrics).
    """
    with EpanetOutput(out_path) as ep:
        nodes_df = ep.nodes_to_dataframe()
        links_df = ep.links_to_dataframe()

    summary: Dict[str, Any] = {"nodes": {}, "links": {}}

    if nodes_df is not None and not nodes_df.empty:
        summary["nodes"] = _summarize_per_feature(nodes_df, id_col="id")
    if links_df is not None and not links_df.empty:
        summary["links"] = _summarize_per_feature(links_df, id_col="id")

    return summary


# Columns from EpanetOutput dataframes that are scaffolding, not metrics.
_NON_METRIC_COLS = frozenset({"id", "period", "time", "node_index", "link_index"})


def _summarize_per_feature(df, id_col: str) -> Dict[str, Any]:
    """min/max/mean per metric per feature; expects long-by-period DataFrame."""
    metric_cols = [c for c in df.columns if c not in _NON_METRIC_COLS]
    out: Dict[str, Any] = {}
    grouped = df.groupby(id_col)
    for fid, sub in grouped:
        per_metric: Dict[str, Any] = {}
        for m in metric_cols:
            series = sub[m]
            try:
                per_metric[m] = {
                    "min": float(series.min()),
                    "max": float(series.max()),
                    "mean": float(series.mean()),
                    "argmin": int(series.idxmin()),
                    "argmax": int(series.idxmax()),
                }
            except (TypeError, ValueError):
                # Non-numeric column (e.g. status flags); skip gracefully.
                continue
        out[str(fid)] = per_metric
    return out


# ---------------------------------------------------------------------------
# .out → results.zarr
# ---------------------------------------------------------------------------

def emit_results_zarr(
    out_path: PathLike,
    inp_path: PathLike,
    zarr_store: Any,
    *,
    chunk_features: int = 10_000,
    sort_spatial: bool = True,
) -> Dict[str, Any]:
    """
    Write simulation time-series to a Zarr v3 store, chunked for cheap
    cloud reads of (a) one period across all features (map scrubbing) and
    (b) one feature across all periods (charts via service).

    Layout (one xarray Dataset; per-role arrays):
        nodes        shape (N, P, M_node)   chunk (chunk_features, P, M_node)
        links        shape (L, P, M_link)   chunk (chunk_features, P, M_link)
    Coordinates:
        node_feature_id  (length N)  — EPANET ID strings ("J-101")
        link_feature_id  (length L)  — EPANET ID strings ("P-12")
        period_seconds   (length P)
        node_metric, link_metric

    Features on the feature axis are pre-sorted by a Z-order (Morton)
    space-filling curve on (x, y) when `sort_spatial` is True. This keeps
    spatially-near features in the same chunk → viewport-aware partial
    loads on the client are efficient. Falls back to file order if any
    coordinates are missing.

    Args:
        out_path:        Path to EPANET binary .out.
        inp_path:        Path to source .inp (for coordinates / spatial sort).
        zarr_store:      A zarr-store-compatible target — string path,
                         `zarr.storage.Store`, or any object xarray's
                         `.to_zarr()` accepts.
        chunk_features:  Feature-axis chunk size. Default 10_000 keeps each
                         chunk in the low-MB range for typical period counts.
        sort_spatial:    If True, sort features by Z-order on coordinates.

    Returns:
        A small descriptor dict: shapes, metric lists, period count. Useful
        for surfacing in `report.json` so the client knows what to expect
        without opening the Zarr.
    """
    # Lazy imports — these are heavy and only required for this function.
    try:
        import numpy as np
        import xarray as xr
    except ImportError as e:
        raise ImportError(
            "emit_results_zarr requires `xarray` and `zarr`. "
            "Install with: pip install 'epanet-utils[console]'"
        ) from e

    decoder = EpanetInputDecoder()
    inp = decoder.decode_inp(Path(inp_path))
    # The decoder uses keys "node", "x_coord", "y_coord" for coordinates.
    coords_by_id: Dict[str, tuple] = {
        c.get("node") or c.get("id"): (c.get("x_coord", c.get("x")), c.get("y_coord", c.get("y")))
        for c in inp.get("coordinates", []) or []
        if c.get("node") or c.get("id")
    }

    with EpanetOutput(out_path) as ep:
        node_ids = list(ep.node_ids)
        link_ids = list(ep.link_ids)
        n_periods = ep.num_periods
        step = ep.report_time_step or 1
        nodes_df = ep.nodes_to_dataframe()
        links_df = ep.links_to_dataframe()

    node_metrics = ("pressure", "head", "demand", "quality")
    link_metrics = ("flow", "velocity", "headloss", "status", "setting")

    node_arr = _df_to_cube(nodes_df, "id", node_ids, n_periods, node_metrics)
    link_arr = _df_to_cube(links_df, "id", link_ids, n_periods, link_metrics)

    if sort_spatial:
        node_order = _zorder(node_ids, coords_by_id)
        link_order = _zorder(link_ids, coords_by_id)
        node_ids = [node_ids[i] for i in node_order]
        node_arr = node_arr[node_order]
        link_ids = [link_ids[i] for i in link_order]
        link_arr = link_arr[link_order]

    period_seconds = np.arange(n_periods, dtype="int32") * int(step)

    ds = xr.Dataset(
        data_vars={
            "nodes": (("node_idx", "period_idx", "node_metric"), node_arr.astype("float32")),
            "links": (("link_idx", "period_idx", "link_metric"), link_arr.astype("float32")),
        },
        coords={
            "node_feature_id": ("node_idx", np.array(node_ids, dtype=object)),
            "link_feature_id": ("link_idx", np.array(link_ids, dtype=object)),
            "period_seconds": ("period_idx", period_seconds),
            "node_metric": list(node_metrics),
            "link_metric": list(link_metrics),
        },
        attrs={
            "producer": "epanet-utils",
            "sim_engine": "epanet",
            "report_time_step_seconds": int(step),
        },
    )

    n_node_chunk = min(chunk_features, max(1, len(node_ids)))
    n_link_chunk = min(chunk_features, max(1, len(link_ids)))

    encoding = {
        "nodes": {"chunks": (n_node_chunk, n_periods, len(node_metrics))},
        "links": {"chunks": (n_link_chunk, n_periods, len(link_metrics))},
    }

    # zarr_format defaults to whatever the installed zarr lib produces (v2 or
    # v3). Both are readable by zarrita.js on the client. Pass an explicit
    # `zarr_format=3` kwarg from the caller if you specifically need v3.
    # We use encoding chunks (not Dataset.chunk) to avoid a dask dep — the
    # producer runs in environments where dask isn't installed.
    ds.to_zarr(zarr_store, mode="w", consolidated=True, encoding=encoding)

    return {
        "nodes_shape": list(node_arr.shape),
        "links_shape": list(link_arr.shape),
        "node_metrics": list(node_metrics),
        "link_metrics": list(link_metrics),
        "n_periods": n_periods,
        "report_time_step_seconds": int(step),
        "chunk_features": chunk_features,
    }


def _df_to_cube(df, id_col: str, ordered_ids: list, n_periods: int, metrics: Iterable[str]):
    """Reshape a long-by-period DataFrame to (n_features, n_periods, n_metrics)."""
    import numpy as np

    metrics = list(metrics)
    n = len(ordered_ids)
    m = len(metrics)
    cube = np.full((n, n_periods, m), np.nan, dtype="float64")

    if df is None or df.empty:
        return cube

    id_to_idx = {fid: i for i, fid in enumerate(ordered_ids)}
    period_col = "period" if "period" in df.columns else "time"

    # Vectorize: pull arrays once; iterate row tuples.
    available_metrics = [(j, m) for j, m in enumerate(metrics) if m in df.columns]
    for _, row in df.iterrows():
        i = id_to_idx.get(row[id_col])
        if i is None:
            continue
        p = int(row[period_col])
        if p < 0 or p >= n_periods:
            continue
        for j, name in available_metrics:
            v = row[name]
            try:
                cube[i, p, j] = float(v)
            except (TypeError, ValueError):
                continue
    return cube


def _zorder(ids: list, coords_by_id: Dict[str, tuple]) -> list:
    """
    Return indices that sort `ids` by Z-order (Morton) curve on (x, y).
    Features without coordinates fall to the tail in original order.
    Pure numpy; no extra dependency.
    """
    import numpy as np

    n = len(ids)
    if n == 0:
        return []

    xs = np.full(n, np.nan)
    ys = np.full(n, np.nan)
    has_coord = np.zeros(n, dtype=bool)
    for i, fid in enumerate(ids):
        c = coords_by_id.get(fid)
        if c is None:
            continue
        x, y = c
        if x is None or y is None:
            continue
        xs[i] = float(x)
        ys[i] = float(y)
        has_coord[i] = True

    if not has_coord.any():
        return list(range(n))

    # Quantize to 16-bit grid within bbox of coordinated features.
    x_valid = xs[has_coord]
    y_valid = ys[has_coord]
    x_lo, x_hi = float(x_valid.min()), float(x_valid.max())
    y_lo, y_hi = float(y_valid.min()), float(y_valid.max())
    x_range = max(x_hi - x_lo, 1e-12)
    y_range = max(y_hi - y_lo, 1e-12)

    qx = np.zeros(n, dtype=np.uint64)
    qy = np.zeros(n, dtype=np.uint64)
    qx[has_coord] = np.clip(((xs[has_coord] - x_lo) / x_range * 0xFFFF), 0, 0xFFFF).astype(np.uint64)
    qy[has_coord] = np.clip(((ys[has_coord] - y_lo) / y_range * 0xFFFF), 0, 0xFFFF).astype(np.uint64)

    # Bit-interleave qx and qy into a Morton code.
    keys = _morton(qx) | (_morton(qy) << 1)

    # Sort by (has_coord descending, key ascending) so coordinated features
    # cluster spatially, uncoordinated tail sits at the end.
    order = np.lexsort((keys, ~has_coord))
    return order.tolist()


def _morton(v):
    """Spread bits of 16-bit `v` into even positions of a 32-bit value."""
    v = v & 0xFFFF
    v = (v | (v << 8)) & 0x00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F
    v = (v | (v << 2)) & 0x33333333
    v = (v | (v << 1)) & 0x55555555
    return v
