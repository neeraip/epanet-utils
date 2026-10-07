"""
Checks the .out decoder against the EPANET engine itself.

The fixtures in tests/fixtures/out were produced by OWA-EPANET 2.3.6
(`runepanet net.inp net.rpt net.out`):

- net1:       EPANET's Net1 (1 pump, 1 tank, 1 reservoir, chlorine) with
              [REPORT] Nodes/Links All, an energy price and a demand charge.
- net3_short: EPANET's Net3 (2 pumps, 3 tanks, 2 reservoirs, trace) cut to
              8 h with Report Start 2:00, so the period count (7) differs
              from duration / step.

Each check compares the decoder with what the engine printed in the .rpt,
with the source .inp, or with a minimal independent reader written here
from the documented layout (EPANET src/output.c).
"""

import math
import re
import shutil
import struct
from pathlib import Path

import numpy as np
import pytest

from epanet_utils import EpanetOutput, EpanetOutputDecoder
from epanet_utils.exports import _per_feature_summary

FIXTURES = Path(__file__).parent / "fixtures" / "out"
NETS = ["net1", "net3_short"]

# The .rpt prints two decimals.
RPT_TOL = 0.0051


# ---------------------------------------------------------------------------
# Independent readers
# ---------------------------------------------------------------------------

def _rpt_tables(rpt_path: Path, kind: str):
    """{seconds: {id: [floats]}} for every 'Node Results' / 'Link Results' table."""
    lines = rpt_path.read_text().splitlines()
    tables = {}
    i = 0
    header = re.compile(rf"^\s*{kind} Results at (\d+):(\d\d):(\d\d) hrs:")
    while i < len(lines):
        m = header.match(lines[i])
        if not m:
            i += 1
            continue
        secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        i += 5  # dashes, 2 header lines, dashes
        rows = {}
        while i < len(lines) and lines[i].strip():
            tok = lines[i].split()
            vals = []
            for t in tok[1:]:
                try:
                    vals.append(float(t))
                except ValueError:
                    break
            rows[tok[0]] = vals
            i += 1
        tables[secs] = rows
    return tables


def _rpt_energy(rpt_path: Path):
    text = rpt_path.read_text()
    block = text.split("Energy Usage:")[1].split("Demand Charge:")[0]
    rows = {}
    for line in block.splitlines():
        tok = line.split()
        if len(tok) == 7:
            try:
                rows[tok[0]] = [float(t) for t in tok[1:]]
            except ValueError:
                pass
    demand = float(text.split("Demand Charge:")[1].split()[0])
    return rows, demand


def _inp_sections(inp_path: Path):
    sections, cur = {}, None
    for raw in inp_path.read_text().splitlines():
        line = raw.split(";")[0].strip()
        if not line:
            continue
        if line.startswith("["):
            cur = line.strip("[]").upper()
            sections[cur] = []
        elif cur:
            sections[cur].append(line.split())
    return sections


def _raw_cube(out_path: Path):
    """Read the dynamic results with numpy, offsets walked from the prolog start."""
    buf = out_path.read_bytes()
    ints = struct.unpack_from("<15i", buf, 0)
    n, t, l, pumps = ints[2], ints[3], ints[4], ints[5]
    off = 15 * 4 + 3 * 80 + 2 * 260 + 2 * 32  # counts, title, files, chem
    off += 32 * n + 32 * l                    # IDs
    off += 3 * 4 * l                          # start, end, type
    off += 2 * 4 * t                          # tank index, area
    off += 4 * n + 2 * 4 * l                  # elevation, length, diameter
    off += pumps * 28 + 4                     # energy + demand charge
    periods = struct.unpack_from("<i", buf, len(buf) - 12)[0]
    per = 4 * n + 8 * l
    arr = np.frombuffer(buf, dtype="<f4", count=periods * per, offset=off).reshape(periods, per)
    assert off + periods * per * 4 + 28 == len(buf)
    nodes = arr[:, : 4 * n].reshape(periods, 4, n)
    links = arr[:, 4 * n:].reshape(periods, 8, l)
    return nodes, links


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(params=NETS)
def net(request):
    name = request.param
    out = FIXTURES / f"{name}.out"
    if not out.exists():
        pytest.skip(f"fixture missing: {out}")
    return {
        "name": name,
        "out": out,
        "rpt": FIXTURES / f"{name}.rpt",
        "inp": FIXTURES / f"{name}.inp",
        "decoded": EpanetOutputDecoder().decode_file(out),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_period_count_and_offset(net):
    d = net["decoded"]
    p, e = d["prolog"], d["epilog"]
    assert e["magic_number"] == EpanetOutputDecoder.EPANET_MAGIC_NUMBER
    assert p["num_periods"] == e["num_periods"]
    assert len(d["time_series"]["nodes"]) == e["num_periods"]
    assert len(d["time_series"]["links"]) == e["num_periods"]
    # The results begin right after the energy section.
    assert p["results_offset"] == p["prolog_bytes"] + 28 * p["num_pumps"] + 4
    expected = {"net1": 25, "net3_short": 7}[net["name"]]
    assert p["num_periods"] == expected


def test_node_results_match_rpt(net):
    d = net["decoded"]
    p = d["prolog"]
    ids = p["node_ids"]
    tables = _rpt_tables(net["rpt"], "Node")
    assert tables, "rpt has no node tables"
    checked = 0
    for secs, rows in tables.items():
        period = (secs - p["report_start_time"]) // p["report_time_step"]
        decoded = d["time_series"]["nodes"][period]
        for nid, vals in rows.items():
            r = decoded[ids.index(nid)]
            got = [r["demand"], r["head"], r["pressure"], r["quality"]][: len(vals)]
            for g, v in zip(got, vals):
                assert abs(g - v) <= RPT_TOL, (net["name"], secs, nid, got, vals)
            checked += 1
    assert checked > 50


def test_link_results_match_rpt(net):
    d = net["decoded"]
    p = d["prolog"]
    ids = p["link_ids"]
    tables = _rpt_tables(net["rpt"], "Link")
    assert tables, "rpt has no link tables"
    checked = 0
    for secs, rows in tables.items():
        period = (secs - p["report_start_time"]) // p["report_time_step"]
        decoded = d["time_series"]["links"][period]
        for lid, vals in rows.items():
            r = decoded[ids.index(lid)]
            got = [r["flow"], r["velocity"], r["headloss"]][: len(vals)]
            for g, v in zip(got, vals):
                assert abs(g - v) <= RPT_TOL, (net["name"], secs, lid, got, vals)
            checked += 1
    assert checked > 50


def test_time_series_match_independent_reader(net):
    d = net["decoded"]
    nodes, links = _raw_cube(net["out"])
    node_keys = ["demand", "head", "pressure", "quality"]
    link_keys = ["flow", "velocity", "headloss", "avg_quality",
                 "status", "setting", "reaction_rate", "friction_factor"]
    for period in range(nodes.shape[0]):
        dn = d["time_series"]["nodes"][period]
        for k, key in enumerate(node_keys):
            np.testing.assert_array_equal(
                np.array([r[key] for r in dn], dtype="<f4"), nodes[period, k])
        dl = d["time_series"]["links"][period]
        for k, key in enumerate(link_keys):
            np.testing.assert_array_equal(
                np.array([r[key] for r in dl], dtype="<f4"), links[period, k])


def test_energy_matches_rpt(net):
    d = net["decoded"]
    rows, demand_charge = _rpt_energy(net["rpt"])
    energy = d["energy_usage"]
    assert len(energy) == d["prolog"]["num_pumps"] == len(rows) >= 1
    keys = ["percent_utilization", "avg_efficiency", "kwh_per_flow",
            "avg_kw", "peak_kw", "cost_per_day"]
    for i, rec in enumerate(energy):
        assert rec["pump_index"] == i
        assert d["prolog"]["link_ids"][rec["link_index"]] == rec["link_id"]
        assert d["prolog"]["link_type_names"][rec["link_index"]] == "PUMP"
        for key, v in zip(keys, rows[rec["link_id"]]):
            assert abs(rec[key] - v) <= RPT_TOL, (rec["link_id"], key, rec[key], v)
    # The .out holds peak kW x demand charge ($/kW). saveenergy() stores that
    # product back into Emax, and the .rpt multiplies by the charge again
    # (report.c), so the printed "Demand Charge" is the .out value x charge.
    dcost = float(next(r for r in _inp_sections(net["inp"])["ENERGY"]
                       if r[0].upper() == "DEMAND")[-1])
    if dcost:
        assert math.isclose(d["peak_demand_cost"] * dcost, demand_charge, rel_tol=1e-5)
    else:
        assert d["peak_demand_cost"] == 0 and demand_charge == 0
    if net["name"] == "net1":
        assert demand_charge > 0
        assert energy[0]["cost_per_day"] > 0
        # One pump: its peak kW is the network peak.
        assert math.isclose(d["peak_demand_cost"], energy[0]["peak_kw"] * dcost, rel_tol=1e-5)


def test_prolog_network_data_matches_inp(net):
    p = net["decoded"]["prolog"]
    sec = _inp_sections(net["inp"])
    node_ids, link_ids = p["node_ids"], p["link_ids"]

    for row in sec["JUNCTIONS"]:
        i = node_ids.index(row[0])
        assert p["node_types"][i] == "JUNCTION"
        assert math.isclose(p["node_elevations"][i], float(row[1]), rel_tol=1e-6)
    for row in sec["RESERVOIRS"]:
        i = node_ids.index(row[0])
        assert p["node_types"][i] == "RESERVOIR"
        assert p["tank_areas"][p["tank_node_indices"].index(i)] == 0
    for row in sec["TANKS"]:
        i = node_ids.index(row[0])
        assert p["node_types"][i] == "TANK"
        assert math.isclose(p["node_elevations"][i], float(row[1]), rel_tol=1e-6)
        area = math.pi * float(row[5]) ** 2 / 4
        assert math.isclose(p["tank_areas"][p["tank_node_indices"].index(i)], area, rel_tol=1e-5)
    assert sorted(p["tank_ids"]) == sorted(r[0] for r in sec["RESERVOIRS"] + sec["TANKS"])

    for row in sec["PIPES"]:
        j = link_ids.index(row[0])
        assert p["link_start_node_ids"][j] == row[1]
        assert p["link_end_node_ids"][j] == row[2]
        assert node_ids[p["link_start_node_indices"][j]] == row[1]
        assert node_ids[p["link_end_node_indices"][j]] == row[2]
        assert math.isclose(p["link_lengths"][j], float(row[3]), rel_tol=1e-6)
        assert math.isclose(p["link_diameters"][j], float(row[4]), rel_tol=1e-6)
        assert p["link_type_names"][j] in ("PIPE", "CVPIPE")
    for row in sec["PUMPS"]:
        j = link_ids.index(row[0])
        assert p["link_type_names"][j] == "PUMP"
        assert p["link_types"][j] == 2
        assert p["link_diameters"][j] == 0
        assert (p["link_start_node_ids"][j], p["link_end_node_ids"][j]) == (row[1], row[2])


def test_without_time_series_still_reads_counts_and_energy(net):
    d = EpanetOutputDecoder().decode_file(net["out"], load_time_series=False)
    assert d["prolog"]["num_periods"] == net["decoded"]["prolog"]["num_periods"]
    assert d["energy_usage"] == net["decoded"]["energy_usage"]
    assert d["time_series"] == {"nodes": [], "links": []}


def test_missing_epilog_falls_back_to_complete_periods(net, tmp_path):
    """An aborted run has no epilog; read the complete periods that are there."""
    buf = net["out"].read_bytes()
    p = net["decoded"]["prolog"]
    per = 16 * p["num_nodes"] + 32 * p["num_links"]
    cut = tmp_path / "cut.out"
    # Drop the epilog and half of the last period.
    cut.write_bytes(buf[: len(buf) - 28 - per // 2])
    d = EpanetOutputDecoder().decode_file(cut)
    assert d["prolog"]["num_periods"] == p["num_periods"] - 1
    assert d["time_series"]["nodes"][0] == net["decoded"]["time_series"]["nodes"][0]


def test_high_level_properties(net):
    with EpanetOutput(net["out"]) as ep:
        p = net["decoded"]["prolog"]
        assert ep.num_periods == p["num_periods"]
        assert ep.node_elevations == p["node_elevations"]
        assert ep.link_types == p["link_type_names"]
        assert ep.link_lengths == p["link_lengths"]
        assert ep.link_diameters == p["link_diameters"]
        assert ep.node_types == p["node_types"]
        assert ep.link_start_node_ids == p["link_start_node_ids"]
        assert ep.peak_demand_cost == net["decoded"]["peak_demand_cost"]
        df = ep.nodes_to_dataframe()
        assert len(df) == p["num_nodes"] * p["num_periods"]


def test_per_feature_summary_is_per_feature(net):
    d = net["decoded"]
    nodes, links = _raw_cube(net["out"])
    summary = _per_feature_summary(net["out"])
    link_ids, node_ids = d["prolog"]["link_ids"], d["prolog"]["node_ids"]

    flow_max = {lid: summary["links"][lid]["flow"]["max"] for lid in link_ids}
    assert len(set(flow_max.values())) > 1, "every link has the same max flow"
    for j, lid in enumerate(link_ids):
        s = summary["links"][lid]["flow"]
        assert math.isclose(s["max"], float(links[:, 0, j].max()), rel_tol=1e-6, abs_tol=1e-6)
        assert math.isclose(s["min"], float(links[:, 0, j].min()), rel_tol=1e-6, abs_tol=1e-6)
        assert links[s["argmax"], 0, j] == links[:, 0, j].max()
    for i, nid in enumerate(node_ids):
        s = summary["nodes"][nid]["pressure"]
        assert math.isclose(s["max"], float(nodes[:, 2, i].max()), rel_tol=1e-6, abs_tol=1e-6)
        assert 0 <= s["argmin"] < nodes.shape[0]
        assert nodes[s["argmin"], 2, i] == nodes[:, 2, i].min()


def test_results_zarr_and_parquet_values(net, tmp_path):
    pytest.importorskip("xarray")
    pytest.importorskip("zarr")
    import pandas as pd
    import xarray as xr
    from epanet_utils.exports import emit_results_parquet, emit_results_zarr

    d = net["decoded"]
    nodes, links = _raw_cube(net["out"])
    node_ids, link_ids = d["prolog"]["node_ids"], d["prolog"]["link_ids"]

    store = tmp_path / "r.zarr"
    desc = emit_results_zarr(net["out"], net["inp"], str(store))
    assert desc["n_periods"] == d["prolog"]["num_periods"]
    ds = xr.open_zarr(str(store))
    zn = ds["nodes"].sel(node_metric="pressure").values
    zids = list(ds["node_feature_id"].values)
    for i, nid in enumerate(node_ids):
        np.testing.assert_array_equal(zn[zids.index(nid)], nodes[:, 2, i])
    zl = ds["links"].sel(link_metric="flow").values
    zlids = list(ds["link_feature_id"].values)
    for j, lid in enumerate(link_ids):
        np.testing.assert_array_equal(zl[zlids.index(lid)], links[:, 0, j])

    pq = tmp_path / "r.parquet"
    emit_results_parquet(net["out"], net["inp"], str(pq))
    df = pd.read_parquet(pq)
    heads = df[df.role == "node"].pivot(index="fid", columns="period_idx", values="head")
    for i, nid in enumerate(node_ids):
        np.testing.assert_array_equal(heads.loc[nid].to_numpy(dtype="<f4"), nodes[:, 1, i])


def test_net1_known_values():
    """Spot values from the Net1 .rpt, so a regression reads plainly."""
    out = FIXTURES / "net1.out"
    if not out.exists():
        pytest.skip("fixture missing")
    with EpanetOutput(out) as ep:
        r = ep.get_node_results("10", period=0)
        assert abs(r["pressure"] - 127.54) <= RPT_TOL
        assert abs(r["head"] - 1004.35) <= RPT_TOL
        assert abs(ep.get_node_results("9", period=0)["head"] - 800.0) <= RPT_TOL  # reservoir
        assert abs(ep.get_link_results("10", period=0)["flow"] - 1866.18) <= RPT_TOL
        assert ep.energy_usage[0]["link_id"] == "9"
