"""
EPANET Binary Output File Decoder

Decodes EPANET binary output files (.out) into Python dictionaries.

Layout (EPANET 2.2 / 2.3, written by ``src/output.c`` in OWA-EPANET; every
integer is int32 and every real is float32, little-endian):

1. Prolog (``savenetdata``)
   - 15 int32: magic, version, Nnodes, Ntanks (reservoirs + tanks), Nlinks,
     Npumps, Nvalves, quality option, trace node, flow units, pressure
     units, report statistic, report start, report step, duration
   - title: 3 x 80 chars; input file: 260; report file: 260;
     chemical name: 32; chemical units: 32
   - node IDs: Nnodes x 32 chars; link IDs: Nlinks x 32 chars
   - link start node indices, link end node indices, link types:
     3 x Nlinks int32 (1-based node indices)
   - tank node indices: Ntanks int32; tank cross-section areas: Ntanks float
   - node elevations: Nnodes float; link lengths: Nlinks float;
     link diameters: Nlinks float
2. Energy usage (``saveenergy``)
   - per pump: pump link index (int32, 1-based) + 6 floats (utilization %,
     avg efficiency %, kWh per flow unit, avg kW, peak kW, avg cost/day)
   - 1 float: peak demand cost
3. Dynamic results (``saveoutput``), per reporting period:
   - 4 x Nnodes floats (demand, head, pressure, quality)
   - 8 x Nlinks floats (flow, velocity, headloss, avg quality, status,
     setting, reaction rate, friction factor)
4. Epilog (last 28 bytes): 4 floats (avg bulk, wall, tank reaction rates,
   avg source inflow rate), int32 Nperiods, int32 warning flag, int32 magic.

The period count comes from the epilog, and the dynamic results are located
from the end of the file: ``filesize - 28 - Nperiods * (16*Nnodes + 32*Nlinks)``.

Indices exposed by this decoder are 0-based (``node_index``/``link_index``
into ``node_ids``/``link_ids``), unlike the 1-based values in the file.
"""

import os
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


# EPANET LinkType enum (types.h). PCV is EPANET 2.3+.
LINK_TYPE_NAMES = {
    0: "CVPIPE",
    1: "PIPE",
    2: "PUMP",
    3: "PRV",
    4: "PSV",
    5: "PBV",
    6: "FCV",
    7: "TCV",
    8: "GPV",
    9: "PCV",
}

_EPILOG_BYTES = 28
_PROLOG_INT_COUNT = 15
_ID_LEN = 32
_TITLE_LEN = 80
_FNAME_LEN = 260
_NODE_VARS = 4
_LINK_VARS = 8


class EpanetOutputDecoder:
    """
    Decoder for EPANET binary output files (.out).

    Parses simulation output including:
    - Network metadata (counts, IDs, link end nodes and types, tank areas,
      node elevations, link lengths and diameters)
    - Time series data for nodes (demand, head, pressure, quality)
    - Time series data for links (flow, velocity, headloss, status, ...)
    - Pump energy usage statistics

    Example:
        >>> decoder = EpanetOutputDecoder()
        >>> output = decoder.decode_file("simulation.out")
        >>> print(output['prolog'])
        >>> print(output['node_results'])
    """

    # EPANET output file constants
    EPANET_MAGIC_NUMBER = 516114521

    # Result types for nodes
    NODE_DEMAND = 0
    NODE_HEAD = 1
    NODE_PRESSURE = 2
    NODE_QUALITY = 3

    # Result types for links
    LINK_FLOW = 0
    LINK_VELOCITY = 1
    LINK_HEADLOSS = 2
    LINK_AVG_QUALITY = 3
    LINK_STATUS = 4
    LINK_SETTING = 5
    LINK_REACTION_RATE = 6
    LINK_FRICTION_FACTOR = 7

    def __init__(self):
        """Initialize the decoder."""
        pass

    def decode_file(self, filepath: Union[str, Path], load_time_series: bool = True) -> Dict[str, Any]:
        """
        Decode an EPANET binary output file.

        Args:
            filepath: Path to .out file
            load_time_series: Whether to load full time series data

        Returns:
            Dictionary containing parsed output data
        """
        filepath = Path(filepath)

        with open(filepath, 'rb') as f:
            return self._decode_binary(f, load_time_series)

    def _decode_binary(self, f, load_time_series: bool) -> Dict[str, Any]:
        """Decode binary output file content."""
        output = {
            "prolog": {},
            "energy_usage": [],
            "peak_demand_cost": None,
            "node_results": [],
            "link_results": [],
            "epilog": {},
            "time_series": {
                "nodes": [],
                "links": []
            }
        }

        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        f.seek(0)

        # Read prolog
        prolog = self._read_prolog(f)
        output["prolog"] = prolog

        if not prolog.get("valid", False):
            return output

        num_nodes = prolog["num_nodes"]
        num_links = prolog["num_links"]
        num_pumps = prolog["num_pumps"]

        # Read energy usage (one record per pump, then the peak demand cost)
        energy, peak_demand_cost = self._read_energy_usage(f, num_pumps, prolog.get("link_ids", []))
        output["energy_usage"] = energy
        output["peak_demand_cost"] = peak_demand_cost
        energy_end = f.tell()

        # Read epilog from the end of the file
        epilog = self._read_epilog(f, file_size)
        output["epilog"] = epilog

        period_bytes = 4 * (_NODE_VARS * num_nodes + _LINK_VARS * num_links)
        num_periods_computed = prolog.get("num_periods_computed", 0)

        if epilog.get("magic_number") == self.EPANET_MAGIC_NUMBER and epilog.get("num_periods", -1) >= 0:
            num_periods = epilog["num_periods"]
            results_offset = file_size - _EPILOG_BYTES - num_periods * period_bytes
            if results_offset < energy_end:
                # Epilog disagrees with the file size; fall back to sequential.
                results_offset = energy_end
                num_periods = self._periods_that_fit(file_size - _EPILOG_BYTES - energy_end, period_bytes)
        else:
            # No valid epilog (e.g. the run aborted): read the complete
            # periods that follow the energy section.
            results_offset = energy_end
            num_periods = self._periods_that_fit(file_size - energy_end, period_bytes)
            if num_periods_computed:
                num_periods = min(num_periods, num_periods_computed)

        prolog["num_periods"] = num_periods
        prolog["results_offset"] = results_offset

        # Read dynamic results (time series)
        if load_time_series and num_periods > 0:
            f.seek(results_offset)
            time_series = self._read_time_series(f, num_nodes, num_links, num_periods)
            output["time_series"] = time_series

            # Also populate summary results from final period
            if time_series["nodes"]:
                output["node_results"] = time_series["nodes"][-1]
            if time_series["links"]:
                output["link_results"] = time_series["links"][-1]

        return output

    @staticmethod
    def _periods_that_fit(nbytes: int, period_bytes: int) -> int:
        if period_bytes <= 0 or nbytes <= 0:
            return 0
        return nbytes // period_bytes

    @staticmethod
    def _read_ints(f, n: int) -> List[int]:
        if n <= 0:
            return []
        data = f.read(4 * n)
        return list(struct.unpack(f'<{n}i', data))

    @staticmethod
    def _read_floats(f, n: int) -> List[float]:
        if n <= 0:
            return []
        data = f.read(4 * n)
        return list(struct.unpack(f'<{n}f', data))

    @staticmethod
    def _read_str(f, n: int) -> str:
        raw = f.read(n)
        if len(raw) < n:
            raise IOError("unexpected end of file in prolog")
        return raw.split(b'\x00', 1)[0].decode('ascii', errors='replace').strip()

    def _read_prolog(self, f) -> Dict[str, Any]:
        """Read the prolog section of the output file."""
        prolog = {"valid": False}

        try:
            header = struct.unpack(f'<{_PROLOG_INT_COUNT}i', f.read(4 * _PROLOG_INT_COUNT))
            (magic, version, num_nodes, num_tanks, num_links, num_pumps,
             num_valves, quality_option, trace_node, flow_units,
             pressure_units, report_statistic, report_start, report_step,
             duration) = header

            prolog["magic_number"] = magic
            prolog["version"] = version

            # Validate magic number
            if magic != self.EPANET_MAGIC_NUMBER:
                return prolog

            # Network counts
            prolog["num_nodes"] = num_nodes
            prolog["num_reservoirs_tanks"] = num_tanks
            prolog["num_links"] = num_links
            prolog["num_pumps"] = num_pumps
            prolog["num_valves"] = num_valves

            # Options
            prolog["water_quality_option"] = quality_option
            prolog["trace_node_index"] = trace_node
            prolog["flow_units"] = flow_units
            prolog["pressure_units"] = pressure_units

            # Time parameters
            prolog["report_statistic_type"] = report_statistic
            prolog["report_start_time"] = report_start
            prolog["report_time_step"] = report_step
            prolog["simulation_duration"] = duration

            # Period count implied by the times. The decoder replaces
            # num_periods with the epilog's count, which is authoritative.
            if report_step > 0 and duration >= report_start:
                computed = (duration - report_start) // report_step + 1
            else:
                computed = 0
            prolog["num_periods_computed"] = computed
            prolog["num_periods"] = computed

            # Title (3 lines), file names, chemical name and units
            title_lines = [self._read_str(f, _TITLE_LEN) for _ in range(3)]
            prolog["title"] = '\n'.join(line for line in title_lines if line)
            prolog["input_file"] = self._read_str(f, _FNAME_LEN)
            prolog["report_file"] = self._read_str(f, _FNAME_LEN)
            prolog["chemical_name"] = self._read_str(f, _ID_LEN)
            prolog["chemical_units"] = self._read_str(f, _ID_LEN)

            # Node and link IDs
            node_ids = [self._read_str(f, _ID_LEN) for _ in range(num_nodes)]
            link_ids = [self._read_str(f, _ID_LEN) for _ in range(num_links)]
            prolog["node_ids"] = node_ids
            prolog["link_ids"] = link_ids

            # Link end nodes and types (file holds 1-based node indices)
            start_nodes = [i - 1 for i in self._read_ints(f, num_links)]
            end_nodes = [i - 1 for i in self._read_ints(f, num_links)]
            link_types = self._read_ints(f, num_links)

            # Tanks and reservoirs: node index and cross-section area
            # (area is 0 for a reservoir)
            tank_nodes = [i - 1 for i in self._read_ints(f, num_tanks)]
            tank_areas = self._read_floats(f, num_tanks)

            # Node elevations, link lengths and link diameters (pumps: 0)
            elevations = self._read_floats(f, num_nodes)
            lengths = self._read_floats(f, num_links)
            diameters = self._read_floats(f, num_links)

            def _id(ids: List[str], i: int) -> Optional[str]:
                return ids[i] if 0 <= i < len(ids) else None

            prolog["link_start_node_indices"] = start_nodes
            prolog["link_end_node_indices"] = end_nodes
            prolog["link_start_node_ids"] = [_id(node_ids, i) for i in start_nodes]
            prolog["link_end_node_ids"] = [_id(node_ids, i) for i in end_nodes]
            prolog["link_types"] = link_types
            prolog["link_type_names"] = [LINK_TYPE_NAMES.get(t, str(t)) for t in link_types]
            prolog["tank_node_indices"] = tank_nodes
            prolog["tank_ids"] = [_id(node_ids, i) for i in tank_nodes]
            prolog["tank_areas"] = tank_areas
            prolog["node_elevations"] = elevations
            prolog["link_lengths"] = lengths
            prolog["link_diameters"] = diameters

            node_types = ["JUNCTION"] * num_nodes
            for i, area in zip(tank_nodes, tank_areas):
                if 0 <= i < num_nodes:
                    node_types[i] = "TANK" if area > 0 else "RESERVOIR"
            prolog["node_types"] = node_types

            prolog["prolog_bytes"] = f.tell()
            prolog["valid"] = True

        except (struct.error, IOError, ValueError) as e:
            prolog["error"] = str(e)

        return prolog

    def _read_energy_usage(self, f, num_pumps: int, link_ids: List[str]):
        """
        Read the energy usage section.

        Returns:
            (records, peak_demand_cost). One record per pump: pump_index
            (0-based ordinal among pumps), link_index (0-based), link_id,
            percent_utilization, avg_efficiency (%), kwh_per_flow (kWh per
            MG or per m3), avg_kw, peak_kw, cost_per_day.
        """
        energy: List[Dict[str, Any]] = []
        peak_demand_cost: Optional[float] = None

        try:
            for p in range(num_pumps):
                link_index = struct.unpack('<i', f.read(4))[0] - 1
                (utilization, efficiency, kwh_per_flow, avg_kw, peak_kw,
                 cost_per_day) = struct.unpack('<6f', f.read(24))
                energy.append({
                    "pump_index": p,
                    "link_index": link_index,
                    "link_id": link_ids[link_index] if 0 <= link_index < len(link_ids) else None,
                    "percent_utilization": utilization,
                    "avg_efficiency": efficiency,
                    "kwh_per_flow": kwh_per_flow,
                    "avg_kw": avg_kw,
                    "peak_kw": peak_kw,
                    "cost_per_day": cost_per_day,
                })

            # Peak demand cost is written even when there are no pumps.
            peak_demand_cost = struct.unpack('<f', f.read(4))[0]

        except (struct.error, IOError):
            pass

        return energy, peak_demand_cost

    def _read_time_series(self, f, num_nodes: int, num_links: int, num_periods: int) -> Dict[str, List]:
        """Read dynamic results (time series data) from the current offset."""
        time_series = {"nodes": [], "links": []}
        node_fmt = f'<{_NODE_VARS * num_nodes}f'
        link_fmt = f'<{_LINK_VARS * num_links}f'
        node_bytes = 4 * _NODE_VARS * num_nodes
        link_bytes = 4 * _LINK_VARS * num_links
        n, l = num_nodes, num_links

        try:
            for _ in range(num_periods):
                v = struct.unpack(node_fmt, f.read(node_bytes))
                demands, heads = v[0:n], v[n:2 * n]
                pressures, qualities = v[2 * n:3 * n], v[3 * n:4 * n]
                time_series["nodes"].append([
                    {
                        "node_index": i,
                        "demand": demands[i],
                        "head": heads[i],
                        "pressure": pressures[i],
                        "quality": qualities[i],
                    }
                    for i in range(n)
                ])

                v = struct.unpack(link_fmt, f.read(link_bytes))
                cols = [v[k * l:(k + 1) * l] for k in range(_LINK_VARS)]
                (flows, velocities, headlosses, avg_qualities, statuses,
                 settings, reaction_rates, friction_factors) = cols
                time_series["links"].append([
                    {
                        "link_index": i,
                        "flow": flows[i],
                        "velocity": velocities[i],
                        "headloss": headlosses[i],
                        "avg_quality": avg_qualities[i],
                        "status": statuses[i],
                        "setting": settings[i],
                        "reaction_rate": reaction_rates[i],
                        "friction_factor": friction_factors[i],
                    }
                    for i in range(l)
                ])

        except (struct.error, IOError):
            pass

        return time_series

    def _read_epilog(self, f, file_size: int) -> Dict[str, Any]:
        """Read the epilog (the last 28 bytes of the file)."""
        epilog: Dict[str, Any] = {}

        if file_size < _EPILOG_BYTES:
            return epilog
        try:
            f.seek(file_size - _EPILOG_BYTES)
            (bulk, wall, tank, source, num_periods, warning_flag,
             magic) = struct.unpack('<4f3i', f.read(_EPILOG_BYTES))
            epilog["avg_bulk_reaction_rate"] = bulk
            epilog["avg_wall_reaction_rate"] = wall
            epilog["avg_tank_reaction_rate"] = tank
            epilog["avg_source_inflow_rate"] = source
            epilog["num_periods"] = num_periods
            epilog["warning_flag"] = warning_flag
            epilog["magic_number"] = magic
        except (struct.error, IOError):
            pass

        return epilog

    def to_dataframe(self, output: Dict[str, Any], result_type: str,
                     period: Optional[int] = None) -> 'pd.DataFrame':
        """
        Convert output results to pandas DataFrame.

        Args:
            output: Decoded output dictionary
            result_type: 'nodes' or 'links'
            period: Specific period index (None for all)

        Returns:
            DataFrame with results
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame support")

        time_series = output.get("time_series", {})
        results = time_series.get(result_type, [])

        if not results:
            return pd.DataFrame()

        prolog = output.get("prolog", {})
        ids = prolog.get("node_ids" if result_type == "nodes" else "link_ids", [])

        if period is not None:
            if period < len(results):
                data = results[period]
                df = pd.DataFrame(data)
                if ids and len(ids) == len(data):
                    df['id'] = ids
                df['period'] = period
                return df
            return pd.DataFrame()

        # All periods
        all_data = []
        for p, period_results in enumerate(results):
            for result in period_results:
                row = result.copy()
                row['period'] = p
                idx = result.get('node_index' if result_type == 'nodes' else 'link_index', 0)
                if ids and idx < len(ids):
                    row['id'] = ids[idx]
                all_data.append(row)

        return pd.DataFrame(all_data)
