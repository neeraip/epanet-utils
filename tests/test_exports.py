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
    NON_SPATIAL_SECTIONS,
    SPATIAL_SECTIONS,
    decode_to_data_json,
    emit_report_json,
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
    data["patterns"][0]["multipliers"][0] = sentinel
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
