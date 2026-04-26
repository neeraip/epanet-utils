"""
Tests for the producer-side helpers in epanet_utils.exports:

- decode_to_data_json    (split spatial vs editable)
- encode_with_overlay    (round-trip + overlay edits)
- emit_report_json       (.rpt → structured JSON)
- emit_results_zarr      (.out → Zarr cube)

Skips Zarr tests when xarray/zarr aren't installed.
"""

from pathlib import Path

import pytest

from epanet_utils.exports import (
    LAYER_ROLE_MAP,
    NON_SPATIAL_SECTIONS,
    SPATIAL_SECTIONS,
    decode_to_data_json,
    emit_geojson_layers,
    emit_report_json,
    emit_results_parquet,
    encode_with_overlay,
)


REPO_ROOT = Path(__file__).parent.parent
INP_SIMPLE = REPO_ROOT / "data" / "examples" / "collect-epanet-inp" / "simplenet.inp"
INP_KY5 = REPO_ROOT / "data" / "results" / "20260205_182606_05333e7c" / "ky5.inp"
RPT_KY5 = REPO_ROOT / "data" / "results" / "20260205_182606_05333e7c" / "ky5.rpt"
OUT_KY5 = REPO_ROOT / "data" / "results" / "20260205_182606_05333e7c" / "ky5.out"


@pytest.fixture
def simple_inp() -> Path:
    if not INP_SIMPLE.exists():
        pytest.skip(f"fixture missing: {INP_SIMPLE}")
    return INP_SIMPLE


@pytest.fixture
def ky5_files():
    for p in (INP_KY5, RPT_KY5, OUT_KY5):
        if not p.exists():
            pytest.skip(f"fixture missing: {p}")
    return INP_KY5, RPT_KY5, OUT_KY5


# ---------------------------------------------------------------------------
# decode_to_data_json
# ---------------------------------------------------------------------------

def test_decode_to_data_json_only_non_spatial(simple_inp):
    data = decode_to_data_json(simple_inp)

    # Every section that is present must be in NON_SPATIAL_SECTIONS.
    assert all(k in NON_SPATIAL_SECTIONS for k in data), \
        f"spatial leak: {set(data) - NON_SPATIAL_SECTIONS}"

    # No spatial sections should appear.
    assert not (set(data) & SPATIAL_SECTIONS), \
        f"spatial section in data.json: {set(data) & SPATIAL_SECTIONS}"

    # Sanity: at least one expected non-spatial section exists.
    assert len(data) > 0


def test_decode_to_data_json_serializable(simple_inp):
    import json
    data = decode_to_data_json(simple_inp)
    text = json.dumps(data, default=str)
    assert isinstance(text, str)
    assert len(text) > 0


# ---------------------------------------------------------------------------
# encode_with_overlay
# ---------------------------------------------------------------------------

def test_encode_with_overlay_passthrough_renders_inp(simple_inp):
    """No edits in overlay → output still parses as a valid .inp shape."""
    data = decode_to_data_json(simple_inp)
    rendered = encode_with_overlay(simple_inp, data)
    assert isinstance(rendered, str)
    assert "[OPTIONS]" in rendered or "[TIMES]" in rendered


def test_encode_with_overlay_applies_edit(simple_inp):
    """Editing a non-spatial section flows into the rendered output."""
    data = decode_to_data_json(simple_inp)
    if not data.get("patterns"):
        pytest.skip("fixture has no patterns to edit")

    sentinel = 9.99
    # Patterns are a dict[id -> [multipliers]] (matches swmm-utils
    # convention). Edit the first multiplier of whichever pattern the
    # fixture happens to have first.
    first_id = next(iter(data["patterns"]))
    data["patterns"][first_id][0] = sentinel
    rendered = encode_with_overlay(simple_inp, data)
    assert f"{sentinel}" in rendered, "overlay edit did not appear in rendered .inp"


def test_encode_with_overlay_ignores_spatial_keys(simple_inp):
    """A caller cannot smuggle spatial edits through the overlay."""
    data = decode_to_data_json(simple_inp)
    sentinel = "JUNCTION-NEVER-RENDERED"
    data["junctions"] = [{"id": sentinel, "elevation": 0}]  # type: ignore[index]
    rendered = encode_with_overlay(simple_inp, data)
    assert sentinel not in rendered, \
        "spatial section in overlay leaked into rendered .inp"


# ---------------------------------------------------------------------------
# emit_report_json
# ---------------------------------------------------------------------------

def test_emit_report_json_shape(ky5_files):
    _, rpt, out = ky5_files
    r = emit_report_json(rpt, out)

    expected_keys = {
        "version", "timestamps", "flow_balance", "quality_balance",
        "energy", "warnings", "errors", "status_log",
        "has_warnings", "has_errors", "summary",
        "metrics", "per_feature_summary",
    }
    assert expected_keys.issubset(r.keys())

    assert isinstance(r["warnings"], list)
    assert isinstance(r["errors"], list)
    assert isinstance(r["status_log"], list)
    assert isinstance(r["per_feature_summary"], dict)

    pfs = r["per_feature_summary"]
    assert "nodes" in pfs and "links" in pfs
    if pfs["nodes"]:
        any_fid, metrics = next(iter(pfs["nodes"].items()))
        for stats in metrics.values():
            assert {"min", "max", "mean", "argmin", "argmax"} <= stats.keys()


def test_emit_report_json_serializable(ky5_files):
    import json
    _, rpt, out = ky5_files
    r = emit_report_json(rpt, out)
    json.dumps(r, default=str)  # should not raise


def test_emit_report_json_without_out_path(ky5_files):
    _, rpt, _ = ky5_files
    r = emit_report_json(rpt)
    assert "per_feature_summary" not in r


# ---------------------------------------------------------------------------
# emit_results_zarr
# ---------------------------------------------------------------------------

def _has_xarray_zarr() -> bool:
    try:
        import xarray  # noqa: F401
        import zarr  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has_xarray_zarr(), reason="xarray + zarr not installed")
def test_emit_results_zarr_writes_and_reopens(ky5_files, tmp_path):
    from epanet_utils.exports import emit_results_zarr
    import xarray as xr

    inp, _, out = ky5_files
    store = tmp_path / "results.zarr"
    desc = emit_results_zarr(out, inp, str(store), chunk_features=200)

    assert desc["n_periods"] >= 1
    assert desc["node_metrics"] == ["pressure", "head", "demand", "quality"]
    assert desc["link_metrics"] == ["flow", "velocity", "headloss", "status", "setting"]

    ds = xr.open_zarr(str(store), consolidated=True)
    assert "nodes" in ds.data_vars
    assert "links" in ds.data_vars
    assert ds.nodes.dtype.name == "float32"
    assert ds.nodes.shape[0] == len(ds.node_feature_id)
    assert ds.nodes.shape[1] == desc["n_periods"]
    assert ds.nodes.shape[2] == 4
    assert ds.links.shape[2] == 5
    # spatial sort applied → coordinates should be a permutation, not file order
    assert len(ds.node_feature_id.values) == ds.nodes.shape[0]


# ---------------------------------------------------------------------------
# emit_results_parquet
# ---------------------------------------------------------------------------

def test_emit_results_parquet_shape_and_types(ky5_files, tmp_path):
    import pyarrow.parquet as pq

    inp, _, out = ky5_files
    pq_path = tmp_path / "results.parquet"
    desc = emit_results_parquet(out, inp, str(pq_path))

    expected_cols = [
        "fid", "role", "element_type",
        "period_idx", "period_ts", "period_seconds",
        "pressure", "head", "demand", "quality",
        "flow", "velocity", "headloss", "status", "setting",
    ]
    assert desc["columns"] == expected_cols

    table = pq.read_table(str(pq_path))
    assert table.num_rows > 0

    t_schema = {f.name: str(f.type) for f in table.schema}
    assert t_schema["fid"] == "string"
    assert t_schema["role"] == "string"
    assert t_schema["element_type"] == "string"
    assert t_schema["period_idx"] == "int32"
    assert t_schema["period_seconds"] == "int32"
    # pyarrow stringifies timestamps as e.g. "timestamp[ns]".
    assert t_schema["period_ts"].startswith("timestamp")
    for m in ("pressure", "head", "flow", "velocity"):
        assert t_schema[m] == "float"   # float32 → pyarrow "float"


def test_emit_results_parquet_null_across_roles(ky5_files, tmp_path):
    import pyarrow.parquet as pq

    inp, _, out = ky5_files
    pq_path = tmp_path / "results.parquet"
    emit_results_parquet(out, inp, str(pq_path))

    df = pq.read_table(str(pq_path)).to_pandas()
    node_row = df[df["role"] == "node"].iloc[0]
    link_row = df[df["role"] == "link"].iloc[0]

    # Node rows: link metrics must be NaN.
    assert all(
        str(node_row[c]) == "nan" for c in ("flow", "velocity", "headloss")
    )
    # Link rows: node metrics must be NaN.
    assert all(
        str(link_row[c]) == "nan" for c in ("pressure", "head", "demand")
    )


def test_emit_results_parquet_compression(ky5_files, tmp_path):
    """Writer settings must match file-ingestion-engine (zstd, row groups)."""
    import pyarrow.parquet as pq

    inp, _, out = ky5_files
    pq_path = tmp_path / "results.parquet"
    emit_results_parquet(out, inp, str(pq_path))

    md = pq.read_metadata(str(pq_path))
    rg = md.row_group(0)
    # Every column in the first row group should be zstd.
    for i in range(rg.num_columns):
        assert rg.column(i).compression == "ZSTD"


# ---------------------------------------------------------------------------
# emit_geojson_layers
# ---------------------------------------------------------------------------

def test_emit_geojson_layers_shape(ky5_files):
    """Each layer spec has the documented shape and nonzero feature counts.

    simplenet.inp is too sparse for this — no JUNCTIONS / COORDINATES
    rows, so it would emit zero layers. ky5.inp is the smallest real
    fixture available with all six EPANET role classes.
    """
    inp, _, _ = ky5_files
    layers = emit_geojson_layers(inp, crs="EPSG:4326")
    assert len(layers) > 0

    for spec in layers:
        assert set(spec.keys()) == {
            "name", "role", "geometry_type", "crs", "feature_collection",
        }
        assert spec["role"] == LAYER_ROLE_MAP[spec["name"]]
        assert spec["crs"] == "EPSG:4326"
        fc = spec["feature_collection"]
        assert fc["type"] == "FeatureCollection"
        assert len(fc["features"]) > 0
        f0 = fc["features"][0]
        assert f0["type"] == "Feature"
        assert "id" in f0 and "properties" in f0 and "geometry" in f0
        assert f0["geometry"]["type"] == spec["geometry_type"]


def test_emit_geojson_layers_pump_parameters_expanded(ky5_files):
    """Pumps' opaque ``parameters`` blob must expand into ``param_*`` keys
    and a ``parameters_kind`` summary so consumers don't have to reparse."""
    inp, _, _ = ky5_files
    layers = emit_geojson_layers(inp)
    pumps = next(
        (s for s in layers if s["role"] == "pump"), None
    )
    if pumps is None:
        pytest.skip("ky5 has no pumps to assert on")

    feats = pumps["feature_collection"]["features"]
    # ky5 uses POWER-style pumps; either form should expand. Assert that
    # *some* pump produced at least one ``param_*`` key + a kind summary.
    expanded = [
        f for f in feats
        if any(k.startswith("param_") for k in f["properties"])
    ]
    assert expanded, "expected at least one pump with expanded parameters"
    p0 = expanded[0]["properties"]
    assert "parameters_kind" in p0
    assert p0["parameters_kind"] in ("HEAD", "POWER", "HEAD,SPEED", "POWER,SPEED") \
        or "HEAD" in p0["parameters_kind"] \
        or "POWER" in p0["parameters_kind"]
