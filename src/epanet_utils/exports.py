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


# ---------------------------------------------------------------------------
# .inp → per-role GeoJSON layers
# ---------------------------------------------------------------------------
#
# Canonical EPANET .inp → spatial-layer parser. Lifted from NEER's
# lambda-importer (`app/inp_parser.py::_parse_epanet_inp`) so the lambda and
# any other consumer (console seed, ad-hoc tooling) share one source of
# truth instead of reimplementing per-element cross-references.
#
# Output is GeoJSON FeatureCollection dicts (no shapely / geopandas dep).
# Callers who want a GeoDataFrame can do:
#     gpd.GeoDataFrame.from_features(spec["feature_collection"]["features"],
#                                    crs=spec["crs"])

LAYER_ROLE_MAP: Dict[str, str] = {
    "Junctions":  "junction",
    "Reservoirs": "reservoir",
    "Tanks":      "tank",
    "Pipes":      "pipe",
    "Pumps":      "pump",
    "Valves":     "valve",
}


def _sf(val: Any) -> Optional[float]:
    """Safe float conversion. Returns None if the value can't be coerced."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _link_coords(
    n1: str,
    n2: str,
    link_id: str,
    coord_map: Dict[str, tuple],
    vertex_map: Dict[str, list],
) -> Optional[list]:
    """LineString coordinates: start node → vertices in order → end node.

    Returns None if either endpoint isn't in the coord map.
    """
    start = coord_map.get(n1)
    end = coord_map.get(n2)
    if not start or not end:
        return None
    coords: list = [[start[0], start[1]]]
    for vx, vy in vertex_map.get(link_id, []):
        coords.append([vx, vy])
    coords.append([end[0], end[1]])
    return coords


def emit_geojson_layers(
    inp_path: PathLike,
    crs: Optional[str] = None,
) -> list:
    """Build one GeoJSON FeatureCollection per HydraulicModelRole.

    Cross-references the per-element sibling sections that don't get
    standalone editors in the console (tags, demands, status, quality,
    sources, mixing, emitters) onto each feature's properties so the
    attribute table reflects the full authored .inp.

    EPANET pumps' opaque ``parameters`` blob is parsed into structured
    columns (``param_head``, ``param_power``, ``param_speed``,
    ``param_pattern``, ``param_price``, ``param_effic``) plus a
    ``parameters_kind`` summary so consumers can branch on pump type
    without reparsing the string.

    Args:
        inp_path: Path to a source .inp file.
        crs: Optional CRS string stored in each layer spec for
             downstream reprojection. Coordinate values themselves are
             never transformed here.

    Returns:
        List of layer specs:
            [
              {
                "name": "Junctions",
                "role": "junction",
                "geometry_type": "Point",
                "crs": crs,
                "feature_collection": {"type": "FeatureCollection", "features": [...]},
              },
              ...
            ]

        Empty roles are omitted.
    """
    from .inp import EpanetInput

    inp = EpanetInput(inp_path)

    coord_map: Dict[str, tuple] = {}
    for c in inp.coordinates or []:
        nid = str(c.get("node", c.get("id", "")))
        if not nid:
            continue
        try:
            coord_map[nid] = (float(c["x_coord"]), float(c["y_coord"]))
        except (KeyError, TypeError, ValueError):
            continue

    vertex_map: Dict[str, list] = {}
    for v in inp.vertices or []:
        lid = str(v.get("link", v.get("id", "")))
        if not lid:
            continue
        try:
            vertex_map.setdefault(lid, []).append(
                (float(v["x_coord"]), float(v["y_coord"]))
            )
        except (KeyError, TypeError, ValueError):
            continue

    # --- Cross-reference lookups (sibling sections keyed by element id) ---
    tag_by_id = {
        str(t.get("name", t.get("id", ""))): str(t.get("tag", t.get("value", "")))
        for t in (inp.tags or [])
        if t.get("name") or t.get("id")
    }
    quality_by_id = {
        str(q.get("node", q.get("id", ""))): _sf(
            q.get("initial_quality", q.get("quality", q.get("init_qual")))
        )
        for q in (inp.quality or [])
        if q.get("node") or q.get("id")
    }
    status_by_link = {
        str(s.get("link", s.get("id", ""))): str(s.get("status", "")).upper() or None
        for s in (inp.status or [])
        if s.get("link") or s.get("id")
    }
    emitter_by_id = {
        str(e.get("junction", e.get("id", ""))): _sf(e.get("coefficient"))
        for e in (inp.emitters or [])
        if e.get("junction") or e.get("id")
    }
    sources_by_id: Dict[str, Dict[str, Any]] = {}
    for s in (inp.sources or []):
        nid = str(s.get("node", s.get("id", "")))
        if not nid:
            continue
        sources_by_id[nid] = {
            "source_type": s.get("type"),
            "source_quality": _sf(s.get("source_quality", s.get("quality"))),
            "source_pattern": s.get("pattern"),
        }
    mixing_by_id: Dict[str, Dict[str, Any]] = {}
    for m in (inp.mixing or []):
        tid = str(m.get("tank", m.get("id", "")))
        if not tid:
            continue
        mixing_by_id[tid] = {
            "mixing_model": m.get("model"),
            "mixing_fraction": _sf(m.get("fraction")),
        }

    demands_summary: Dict[str, Dict[str, Any]] = {}
    for d in (inp.demands or []):
        nid = str(d.get("junction", d.get("id", "")))
        if not nid:
            continue
        bucket = demands_summary.setdefault(
            nid, {"demand_count": 0, "total_base_demand": 0.0}
        )
        bucket["demand_count"] += 1
        bd = _sf(d.get("base_demand", d.get("demand")))
        if bd is not None:
            bucket["total_base_demand"] += bd

    def _enrich_node(row: Dict[str, Any], nid: str) -> Dict[str, Any]:
        if nid in tag_by_id:
            row["tag"] = tag_by_id[nid]
        if nid in quality_by_id:
            row["initial_quality"] = quality_by_id[nid]
        if nid in demands_summary:
            row.update(demands_summary[nid])
        if nid in sources_by_id:
            row.update(sources_by_id[nid])
        if nid in emitter_by_id:
            row["emitter_coefficient"] = emitter_by_id[nid]
        return row

    def _enrich_link(row: Dict[str, Any], lid: str) -> Dict[str, Any]:
        if lid in tag_by_id:
            row["tag"] = tag_by_id[lid]
        if lid in status_by_link:
            row["initial_status"] = status_by_link[lid]
        return row

    def _enrich_tank(row: Dict[str, Any], tid: str) -> Dict[str, Any]:
        _enrich_node(row, tid)
        if tid in mixing_by_id:
            row.update(mixing_by_id[tid])
        return row

    layers: list = []

    def _layer_spec(name: str, geom_type: str, features: list) -> Dict[str, Any]:
        return {
            "name": name,
            "role": LAYER_ROLE_MAP[name],
            "geometry_type": geom_type,
            "crs": crs,
            "feature_collection": {
                "type": "FeatureCollection",
                "features": features,
            },
        }

    def _node_features(rows: Iterable[Dict[str, Any]], enrich) -> list:
        out: list = []
        for r in rows:
            nid = str(r.get("id", ""))
            xy = coord_map.get(nid)
            if not xy:
                continue
            # Store the canonical id under ``name`` (matches swmm-utils'
            # convention) so the role-aware panels and the layer
            # attribute table can read ``properties.name`` regardless
            # of engine.
            row = enrich({**r, "name": nid}, nid)
            out.append(
                {
                    "type": "Feature",
                    "id": nid,
                    "properties": row,
                    "geometry": {"type": "Point", "coordinates": [xy[0], xy[1]]},
                }
            )
        return out

    # --- Nodes ---
    junctions = _node_features(inp.junctions, _enrich_node)
    if junctions:
        layers.append(_layer_spec("Junctions", "Point", junctions))

    reservoirs = _node_features(inp.reservoirs, _enrich_node)
    if reservoirs:
        layers.append(_layer_spec("Reservoirs", "Point", reservoirs))

    tanks = _node_features(inp.tanks, _enrich_tank)
    if tanks:
        layers.append(_layer_spec("Tanks", "Point", tanks))

    # --- Pipes ---
    pipe_feats: list = []
    for p in inp.pipes:
        pid = str(p.get("id", ""))
        coords = _link_coords(
            str(p.get("node1", "")), str(p.get("node2", "")),
            pid, coord_map, vertex_map,
        )
        if not coords or len(coords) < 2:
            continue
        row = _enrich_link({**p, "name": pid}, pid)
        pipe_feats.append(
            {
                "type": "Feature",
                "id": pid,
                "properties": row,
                "geometry": {"type": "LineString", "coordinates": coords},
            }
        )
    if pipe_feats:
        layers.append(_layer_spec("Pipes", "LineString", pipe_feats))

    # --- Pumps (with parameters-blob expansion) ---
    pump_feats: list = []
    recognized_pump_kw = {"HEAD", "POWER", "SPEED", "PATTERN", "PRICE", "EFFIC"}
    for p in inp.pumps:
        pid = str(p.get("id", ""))
        coords = _link_coords(
            str(p.get("node1", "")), str(p.get("node2", "")),
            pid, coord_map, vertex_map,
        )
        if not coords or len(coords) < 2:
            continue
        row = _enrich_link({**p, "name": pid}, pid)
        params = str(p.get("parameters", "") or "").strip()
        if params:
            tokens = params.split()
            i = 0
            kinds = []
            while i < len(tokens):
                kw = tokens[i].upper()
                if kw in recognized_pump_kw and i + 1 < len(tokens):
                    row[f"param_{kw.lower()}"] = tokens[i + 1]
                    kinds.append(kw)
                    i += 2
                else:
                    i += 1
            if kinds:
                row["parameters_kind"] = ",".join(kinds)
        pump_feats.append(
            {
                "type": "Feature",
                "id": pid,
                "properties": row,
                "geometry": {"type": "LineString", "coordinates": coords},
            }
        )
    if pump_feats:
        layers.append(_layer_spec("Pumps", "LineString", pump_feats))

    # --- Valves ---
    valve_feats: list = []
    for v in inp.valves:
        vid = str(v.get("id", ""))
        coords = _link_coords(
            str(v.get("node1", "")), str(v.get("node2", "")),
            vid, coord_map, vertex_map,
        )
        if not coords or len(coords) < 2:
            continue
        row = _enrich_link({**v, "name": vid}, vid)
        valve_feats.append(
            {
                "type": "Feature",
                "id": vid,
                "properties": row,
                "geometry": {"type": "LineString", "coordinates": coords},
            }
        )
    if valve_feats:
        layers.append(_layer_spec("Valves", "LineString", valve_feats))

    return layers


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
# .out → results.parquet   (sidecar for file-ingestion-engine query service)
# ---------------------------------------------------------------------------

# Compression/writer settings mirrored from
# `neeraip/file-ingestion-engine/ingest/csv_ingest.py`. Keeping them aligned
# ensures the query service sees the exact shape it does for CSV-origin
# datasets: zstd-3 blocks, dictionary-encoded strings, 1M-row groups, Spark
# flavor, statistics for pruning.
_PARQUET_WRITER_KWARGS: Dict[str, Any] = {
    "compression": "zstd",
    "compression_level": 3,
    "use_dictionary": True,
    "write_statistics": True,
    "data_page_size": 1 << 20,     # 1 MB
    "row_group_size": 1_000_000,
    "flavor": "spark",
}

_NODE_METRICS_PARQUET = ("pressure", "head", "demand", "quality")
_LINK_METRICS_PARQUET = ("flow", "velocity", "headloss", "status", "setting")

# Synthetic timestamp base: period_seconds are added to this to produce
# period_ts. Synthetic because EPANET simulations are typically elapsed-time
# (no wall-clock). The query service parses the column as a datetime; the
# user sees ordinal steps labeled with an arbitrary fixed anchor, which is
# fine for charts.
_PERIOD_TS_BASE = "2000-01-01"


def emit_results_parquet(
    out_path: PathLike,
    inp_path: PathLike,
    parquet_path: PathLike,
) -> Dict[str, Any]:
    """
    Write simulation time-series to a single Parquet file as a sidecar for
    the file-ingestion-engine query service. Long-by-period wide-by-metric
    format: one row per (feature, period). Per-role metric columns are
    nulled across the other role(s).

    Schema:
        fid             string (dict)   — EPANET canonical id ("J-101")
        role            string (dict)   — "node" | "link"
        element_type    string (dict)   — junction|tank|reservoir|pipe|pump|valve
        period_idx      int32
        period_ts       timestamp[us]   — synthetic, base 2000-01-01
        period_seconds  int32
        pressure / head / demand / quality         float32  (null for links)
        flow / velocity / headloss / status / setting  float32  (null for nodes)

    Writer settings match file-ingestion-engine/ingest/csv_ingest.py so the
    query service reads it indistinguishably from any other Parquet there.

    Args:
        out_path:     Path to EPANET binary .out.
        inp_path:     Path to source .inp (for element_type classification).
        parquet_path: Destination path for the `.parquet` file.

    Returns:
        Small descriptor dict: row count, column list, n_periods.
    """
    try:
        import numpy as np
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise ImportError(
            "emit_results_parquet requires `pandas` and `pyarrow` (already "
            "in install_requires — reinstall the package)."
        ) from e

    # Classify element types from the .inp.
    element_type_by_id = _classify_element_types(inp_path)

    with EpanetOutput(out_path) as ep:
        n_periods = ep.num_periods
        step = ep.report_time_step or 1
        node_ids_all = list(ep.node_ids)
        link_ids_all = list(ep.link_ids)
        nodes_df = ep.nodes_to_dataframe()
        links_df = ep.links_to_dataframe()

    frames = []
    if nodes_df is not None and not nodes_df.empty:
        frames.append(_prepare_role_frame(
            nodes_df, role="node", metrics=_NODE_METRICS_PARQUET,
            element_type_by_id=element_type_by_id,
            step_seconds=int(step),
        ))
    if links_df is not None and not links_df.empty:
        frames.append(_prepare_role_frame(
            links_df, role="link", metrics=_LINK_METRICS_PARQUET,
            element_type_by_id=element_type_by_id,
            step_seconds=int(step),
        ))

    if not frames:
        # Produce an empty-but-schema-correct Parquet so downstream code can
        # always open it.
        df = pd.DataFrame(columns=_parquet_schema_columns())
    else:
        df = pd.concat(frames, ignore_index=True, sort=False)

    # Materialize timestamps from period_seconds.
    base = pd.Timestamp(_PERIOD_TS_BASE)
    df["period_ts"] = base + pd.to_timedelta(df["period_seconds"].astype("int64"), unit="s")

    # Enforce types & column order before writing.
    df = _coerce_parquet_types(df)

    table = pa.Table.from_pandas(df, preserve_index=False)

    out_path_local = Path(parquet_path)
    out_path_local.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(out_path_local), **_PARQUET_WRITER_KWARGS)

    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "n_periods": int(n_periods),
        "report_time_step_seconds": int(step),
        "node_metrics": list(_NODE_METRICS_PARQUET),
        "link_metrics": list(_LINK_METRICS_PARQUET),
    }


def _classify_element_types(inp_path: PathLike) -> Dict[str, str]:
    """Map every node/link id → canonical element_type string."""
    decoder = EpanetInputDecoder()
    model = decoder.decode_inp(Path(inp_path))
    out: Dict[str, str] = {}

    for section, element_type in (
        ("junctions", "junction"),
        ("reservoirs", "reservoir"),
        ("tanks", "tank"),
        ("pipes", "pipe"),
        ("pumps", "pump"),
        ("valves", "valve"),
    ):
        for row in model.get(section, []) or []:
            fid = row.get("id") or row.get("name") or row.get("node")
            if fid:
                out[fid] = element_type
    return out


def _parquet_schema_columns() -> list:
    return [
        "fid", "role", "element_type",
        "period_idx", "period_ts", "period_seconds",
        *_NODE_METRICS_PARQUET,
        *_LINK_METRICS_PARQUET,
    ]


def _prepare_role_frame(
    df,
    *,
    role: str,
    metrics: Iterable[str],
    element_type_by_id: Dict[str, str],
    step_seconds: int,
):
    """Reshape a role-specific dataframe to the common Parquet schema."""
    import pandas as pd

    metrics = tuple(metrics)
    # EpanetOutput dataframes have columns: id, period, plus metrics.
    base_cols = ["id", "period"]
    keep = [c for c in (*base_cols, *metrics) if c in df.columns]
    out = df[keep].rename(columns={"id": "fid", "period": "period_idx"}).copy()
    out["role"] = role
    out["element_type"] = out["fid"].map(element_type_by_id).fillna(role)
    out["period_seconds"] = out["period_idx"].astype("int64") * int(step_seconds)

    # Ensure every expected metric column exists (fill missing with NaN so
    # the concatenated frame has a consistent column set across roles).
    for m in (*_NODE_METRICS_PARQUET, *_LINK_METRICS_PARQUET):
        if m not in out.columns:
            out[m] = pd.NA
    return out


def _coerce_parquet_types(df):
    """Cast columns to the types expected by the file-ingestion-engine."""
    import pandas as pd

    df["fid"] = df["fid"].astype("string")
    df["role"] = df["role"].astype("string")
    df["element_type"] = df["element_type"].astype("string")
    df["period_idx"] = df["period_idx"].astype("int32")
    df["period_seconds"] = df["period_seconds"].astype("int32")
    for col in (*_NODE_METRICS_PARQUET, *_LINK_METRICS_PARQUET):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    # Final column order — matches `_parquet_schema_columns()`.
    cols = _parquet_schema_columns()
    return df.reindex(columns=cols)


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
    """
    Reshape a long-by-period DataFrame to (n_features, n_periods, n_metrics).

    Vectorized via numpy fancy-indexing: build a (feature_idx, period_idx)
    pair per row, mask invalid rows, then scatter each metric column into
    the cube in a single assignment. Replaces a per-row iterrows loop that
    was O(rows * metrics) and dominated post-step time on real models
    (e.g. ~50s per role for a 200-feature × 600-period network).
    """
    import numpy as np

    metrics = list(metrics)
    n = len(ordered_ids)
    m = len(metrics)
    cube = np.full((n, n_periods, m), np.nan, dtype="float64")

    if df is None or df.empty:
        return cube

    id_to_idx = {fid: i for i, fid in enumerate(ordered_ids)}
    period_col = "period" if "period" in df.columns else "time"

    feat_idx = df[id_col].map(id_to_idx).to_numpy()
    period_idx = df[period_col].to_numpy()

    valid = (
        ~np.isnan(feat_idx.astype("float64", copy=False))
        & (period_idx >= 0)
        & (period_idx < n_periods)
    )
    if not valid.any():
        return cube
    fi = feat_idx[valid].astype("int64", copy=False)
    pi = period_idx[valid].astype("int64", copy=False)

    for j, name in enumerate(metrics):
        if name not in df.columns:
            continue
        col_vals = df[name].to_numpy(dtype="float64", na_value=np.nan)[valid]
        cube[fi, pi, j] = col_vals
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
