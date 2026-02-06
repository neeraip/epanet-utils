"""
EPANET Binary Output File Decoder

Decodes EPANET binary output files (.out) into Python dictionaries.

The EPANET output file is a binary file with the following structure:
1. Prolog - Header information and network counts
2. Energy Usage - Pump energy consumption data
3. Dynamic Results - Time series data for nodes and links
4. Epilog - Summary statistics

Binary Format Reference:
- All integers are 4-byte (int32)
- All floats are 4-byte (float32)
- Strings are fixed-length character arrays
"""

import struct
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class EpanetOutputDecoder:
    """
    Decoder for EPANET binary output files (.out).
    
    Parses simulation output including:
    - Network metadata (counts of nodes, links, etc.)
    - Time series data for nodes (demand, head, pressure, quality)
    - Time series data for links (flow, velocity, headloss, status)
    - Energy usage statistics
    
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
            "node_results": [],
            "link_results": [],
            "epilog": {},
            "time_series": {
                "nodes": [],
                "links": []
            }
        }
        
        # Read prolog
        prolog = self._read_prolog(f)
        output["prolog"] = prolog
        
        if not prolog.get("valid", False):
            return output
        
        # Calculate offsets
        num_nodes = prolog.get("num_nodes", 0)
        num_links = prolog.get("num_links", 0)
        num_pumps = prolog.get("num_pumps", 0)
        num_periods = prolog.get("num_periods", 0)
        
        # Read energy usage (one record per pump)
        output["energy_usage"] = self._read_energy_usage(f, num_pumps)
        
        # Read dynamic results (time series)
        if load_time_series and num_periods > 0:
            time_series = self._read_time_series(f, num_nodes, num_links, num_periods)
            output["time_series"] = time_series
            
            # Also populate summary results from final period
            if time_series["nodes"]:
                output["node_results"] = time_series["nodes"][-1] if time_series["nodes"] else []
            if time_series["links"]:
                output["link_results"] = time_series["links"][-1] if time_series["links"] else []
        
        # Read epilog
        output["epilog"] = self._read_epilog(f)
        
        return output
    
    def _read_prolog(self, f) -> Dict[str, Any]:
        """Read the prolog section of the output file."""
        prolog = {"valid": False}
        
        try:
            # Read magic number and version
            magic = struct.unpack('i', f.read(4))[0]
            version = struct.unpack('i', f.read(4))[0]
            
            prolog["magic_number"] = magic
            prolog["version"] = version
            
            # Validate magic number
            if magic != self.EPANET_MAGIC_NUMBER:
                return prolog
            
            # Read network counts
            prolog["num_nodes"] = struct.unpack('i', f.read(4))[0]
            prolog["num_reservoirs_tanks"] = struct.unpack('i', f.read(4))[0]
            prolog["num_links"] = struct.unpack('i', f.read(4))[0]
            prolog["num_pumps"] = struct.unpack('i', f.read(4))[0]
            prolog["num_valves"] = struct.unpack('i', f.read(4))[0]
            
            # Read options
            prolog["water_quality_option"] = struct.unpack('i', f.read(4))[0]
            prolog["trace_node_index"] = struct.unpack('i', f.read(4))[0]
            prolog["flow_units"] = struct.unpack('i', f.read(4))[0]
            prolog["pressure_units"] = struct.unpack('i', f.read(4))[0]
            
            # Read time parameters
            prolog["report_statistic_type"] = struct.unpack('i', f.read(4))[0]
            prolog["report_start_time"] = struct.unpack('i', f.read(4))[0]
            prolog["report_time_step"] = struct.unpack('i', f.read(4))[0]
            prolog["simulation_duration"] = struct.unpack('i', f.read(4))[0]
            
            # Calculate number of reporting periods
            if prolog["report_time_step"] > 0:
                prolog["num_periods"] = (prolog["simulation_duration"] - prolog["report_start_time"]) // prolog["report_time_step"] + 1
            else:
                prolog["num_periods"] = 0
            
            # Read problem title (3 lines of 80 chars each)
            title_lines = []
            for _ in range(3):
                title_data = f.read(80)
                title_lines.append(title_data.decode('ascii', errors='replace').strip('\x00').strip())
            prolog["title"] = '\n'.join(line for line in title_lines if line)
            
            # Read input file name
            input_name = f.read(260).decode('ascii', errors='replace').strip('\x00').strip()
            prolog["input_file"] = input_name
            
            # Read report file name
            report_name = f.read(260).decode('ascii', errors='replace').strip('\x00').strip()
            prolog["report_file"] = report_name
            
            # Read chemical name and concentration units
            chem_name = f.read(32).decode('ascii', errors='replace').strip('\x00').strip()
            prolog["chemical_name"] = chem_name
            
            chem_units = f.read(32).decode('ascii', errors='replace').strip('\x00').strip()
            prolog["chemical_units"] = chem_units
            
            # Read node IDs
            node_ids = []
            for _ in range(prolog["num_nodes"]):
                node_id = f.read(32).decode('ascii', errors='replace').strip('\x00').strip()
                node_ids.append(node_id)
            prolog["node_ids"] = node_ids
            
            # Read link IDs
            link_ids = []
            for _ in range(prolog["num_links"]):
                link_id = f.read(32).decode('ascii', errors='replace').strip('\x00').strip()
                link_ids.append(link_id)
            prolog["link_ids"] = link_ids
            
            prolog["valid"] = True
            
        except (struct.error, IOError) as e:
            prolog["error"] = str(e)
        
        return prolog
    
    def _read_energy_usage(self, f, num_pumps: int) -> List[Dict[str, Any]]:
        """Read energy usage section."""
        energy = []
        
        try:
            for i in range(num_pumps):
                pump_energy = {
                    "pump_index": struct.unpack('i', f.read(4))[0],
                    "link_index": struct.unpack('i', f.read(4))[0],
                    "percent_utilization": struct.unpack('f', f.read(4))[0],
                    "avg_efficiency": struct.unpack('f', f.read(4))[0],
                    "kwh_per_flow": struct.unpack('f', f.read(4))[0],
                    "avg_kw": struct.unpack('f', f.read(4))[0],
                    "peak_kw": struct.unpack('f', f.read(4))[0],
                    "cost_per_day": struct.unpack('f', f.read(4))[0]
                }
                energy.append(pump_energy)
            
            # Read peak demand cost
            if num_pumps > 0:
                peak_demand_cost = struct.unpack('f', f.read(4))[0]
                # Store with first pump or separately
        
        except (struct.error, IOError):
            pass
        
        return energy
    
    def _read_time_series(self, f, num_nodes: int, num_links: int, num_periods: int) -> Dict[str, List]:
        """Read dynamic results (time series data)."""
        time_series = {"nodes": [], "links": []}
        
        try:
            for period in range(num_periods):
                # Read node results for this period
                node_results = []
                
                # Demand for all nodes
                demands = struct.unpack(f'{num_nodes}f', f.read(4 * num_nodes))
                
                # Head for all nodes
                heads = struct.unpack(f'{num_nodes}f', f.read(4 * num_nodes))
                
                # Pressure for all nodes
                pressures = struct.unpack(f'{num_nodes}f', f.read(4 * num_nodes))
                
                # Quality for all nodes
                qualities = struct.unpack(f'{num_nodes}f', f.read(4 * num_nodes))
                
                for i in range(num_nodes):
                    node_results.append({
                        "node_index": i,
                        "demand": demands[i],
                        "head": heads[i],
                        "pressure": pressures[i],
                        "quality": qualities[i]
                    })
                
                time_series["nodes"].append(node_results)
                
                # Read link results for this period
                link_results = []
                
                # Flow for all links
                flows = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Velocity for all links
                velocities = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Headloss for all links
                headlosses = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Average quality for all links
                avg_qualities = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Status for all links
                statuses = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Setting for all links
                settings = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Reaction rate for all links
                reaction_rates = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                # Friction factor for all links
                friction_factors = struct.unpack(f'{num_links}f', f.read(4 * num_links))
                
                for i in range(num_links):
                    link_results.append({
                        "link_index": i,
                        "flow": flows[i],
                        "velocity": velocities[i],
                        "headloss": headlosses[i],
                        "avg_quality": avg_qualities[i],
                        "status": statuses[i],
                        "setting": settings[i],
                        "reaction_rate": reaction_rates[i],
                        "friction_factor": friction_factors[i]
                    })
                
                time_series["links"].append(link_results)
        
        except (struct.error, IOError) as e:
            pass
        
        return time_series
    
    def _read_epilog(self, f) -> Dict[str, Any]:
        """Read epilog section."""
        epilog = {}
        
        try:
            # Read average bulk reaction rate
            epilog["avg_bulk_reaction_rate"] = struct.unpack('f', f.read(4))[0]
            
            # Read average wall reaction rate
            epilog["avg_wall_reaction_rate"] = struct.unpack('f', f.read(4))[0]
            
            # Read average tank reaction rate
            epilog["avg_tank_reaction_rate"] = struct.unpack('f', f.read(4))[0]
            
            # Read average source inflow rate
            epilog["avg_source_inflow_rate"] = struct.unpack('f', f.read(4))[0]
            
            # Read number of reporting periods
            epilog["num_periods"] = struct.unpack('i', f.read(4))[0]
            
            # Read warning flag
            epilog["warning_flag"] = struct.unpack('i', f.read(4))[0]
            
            # Read magic number (should match)
            epilog["magic_number"] = struct.unpack('i', f.read(4))[0]
        
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
