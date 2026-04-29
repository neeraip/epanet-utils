"""
EPANET High-Level Output File Interface

Provides an easy-to-use interface for reading EPANET binary output files.

Example:
    >>> from epanet_utils import EpanetOutput
    >>>
    >>> with EpanetOutput("simulation.out") as output:
    ...     print(f"Nodes: {output.num_nodes}")
    ...     print(f"Periods: {output.num_periods}")
    ...
    ...     # Get node results as DataFrame
    ...     nodes_df = output.nodes_to_dataframe()
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from .out_decoder import EpanetOutputDecoder


class EpanetOutput:
    """
    High-level interface for EPANET binary output files (.out).

    Supports context manager protocol for convenient file handling.
    Provides property-based access to output data.

    Example:
        >>> with EpanetOutput("simulation.out") as output:
        ...     print(output.prolog)
        ...     for node in output.node_results:
        ...         print(node)
    """

    def __init__(self, filepath: Union[str, Path], load_time_series: bool = True):
        """
        Initialize EPANET output handler.

        Args:
            filepath: Path to output file (.out)
            load_time_series: Whether to load full time series data
        """
        self._filepath = Path(filepath)
        self._decoder = EpanetOutputDecoder()
        self._output = None
        self._loaded = False
        self._load_time_series = load_time_series

    def __enter__(self):
        """Context manager entry - load output."""
        self._load()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        return False

    def _load(self) -> None:
        """Load and parse the output file."""
        if not self._loaded:
            self._output = self._decoder.decode_file(self._filepath, self._load_time_series)
            self._loaded = True

    def _ensure_loaded(self) -> None:
        """Ensure output is loaded."""
        if not self._loaded:
            self._load()

    # === Properties ===

    @property
    def prolog(self) -> Dict[str, Any]:
        """Get prolog (header) information."""
        self._ensure_loaded()
        return self._output.get("prolog", {})

    @property
    def epilog(self) -> Dict[str, Any]:
        """Get epilog (summary) information."""
        self._ensure_loaded()
        return self._output.get("epilog", {})

    @property
    def energy_usage(self) -> List[Dict[str, Any]]:
        """Get pump energy usage data."""
        self._ensure_loaded()
        return self._output.get("energy_usage", [])

    @property
    def node_results(self) -> List[Dict[str, Any]]:
        """Get final period node results."""
        self._ensure_loaded()
        return self._output.get("node_results", [])

    @property
    def link_results(self) -> List[Dict[str, Any]]:
        """Get final period link results."""
        self._ensure_loaded()
        return self._output.get("link_results", [])

    @property
    def time_series(self) -> Dict[str, List]:
        """Get full time series data."""
        self._ensure_loaded()
        return self._output.get("time_series", {"nodes": [], "links": []})

    @property
    def num_nodes(self) -> int:
        """Get number of nodes."""
        return self.prolog.get("num_nodes", 0)

    @property
    def num_links(self) -> int:
        """Get number of links."""
        return self.prolog.get("num_links", 0)

    @property
    def num_periods(self) -> int:
        """Get number of reporting periods."""
        return self.prolog.get("num_periods", 0)

    @property
    def num_pumps(self) -> int:
        """Get number of pumps."""
        return self.prolog.get("num_pumps", 0)

    @property
    def node_ids(self) -> List[str]:
        """Get list of node IDs."""
        return self.prolog.get("node_ids", [])

    @property
    def link_ids(self) -> List[str]:
        """Get list of link IDs."""
        return self.prolog.get("link_ids", [])

    @property
    def title(self) -> str:
        """Get simulation title."""
        return self.prolog.get("title", "")

    @property
    def version(self) -> int:
        """Get EPANET version."""
        return self.prolog.get("version", 0)

    @property
    def report_time_step(self) -> int:
        """Get report time step in seconds."""
        return self.prolog.get("report_time_step", 0)

    @property
    def simulation_duration(self) -> int:
        """Get simulation duration in seconds."""
        return self.prolog.get("simulation_duration", 0)

    # === DataFrame Methods ===

    def to_dataframe(self, result_type: str, period: Optional[int] = None) -> 'pd.DataFrame':
        """
        Convert results to pandas DataFrame.

        Args:
            result_type: 'nodes' or 'links'
            period: Specific period index (None for all periods)

        Returns:
            DataFrame containing results
        """
        self._ensure_loaded()
        return self._decoder.to_dataframe(self._output, result_type, period)

    def nodes_to_dataframe(self, period: Optional[int] = None) -> 'pd.DataFrame':
        """Get node results as DataFrame."""
        return self.to_dataframe("nodes", period)

    def links_to_dataframe(self, period: Optional[int] = None) -> 'pd.DataFrame':
        """Get link results as DataFrame."""
        return self.to_dataframe("links", period)

    def energy_to_dataframe(self) -> 'pd.DataFrame':
        """Get energy usage as DataFrame."""
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame support")

        self._ensure_loaded()
        return pd.DataFrame(self.energy_usage)

    # === Helper Methods ===

    def get_node_results(self, node_id: str, period: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get results for a specific node.

        Args:
            node_id: Node ID
            period: Period index (None for final period)

        Returns:
            Node result dict or None
        """
        self._ensure_loaded()

        node_ids = self.node_ids
        if node_id not in node_ids:
            return None

        node_index = node_ids.index(node_id)

        if period is None:
            # Return final period results
            results = self.node_results
        else:
            ts = self.time_series.get("nodes", [])
            if period >= len(ts):
                return None
            results = ts[period]

        for result in results:
            if result.get("node_index") == node_index:
                result_copy = result.copy()
                result_copy["node_id"] = node_id
                return result_copy

        return None

    def get_link_results(self, link_id: str, period: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get results for a specific link.

        Args:
            link_id: Link ID
            period: Period index (None for final period)

        Returns:
            Link result dict or None
        """
        self._ensure_loaded()

        link_ids = self.link_ids
        if link_id not in link_ids:
            return None

        link_index = link_ids.index(link_id)

        if period is None:
            # Return final period results
            results = self.link_results
        else:
            ts = self.time_series.get("links", [])
            if period >= len(ts):
                return None
            results = ts[period]

        for result in results:
            if result.get("link_index") == link_index:
                result_copy = result.copy()
                result_copy["link_id"] = link_id
                return result_copy

        return None

    def get_node_time_series(self, node_id: str) -> List[Dict[str, Any]]:
        """
        Get time series data for a specific node.

        Args:
            node_id: Node ID

        Returns:
            List of result dicts for each period
        """
        self._ensure_loaded()

        results = []
        for period in range(self.num_periods):
            result = self.get_node_results(node_id, period)
            if result:
                result["period"] = period
                results.append(result)

        return results

    def get_link_time_series(self, link_id: str) -> List[Dict[str, Any]]:
        """
        Get time series data for a specific link.

        Args:
            link_id: Link ID

        Returns:
            List of result dicts for each period
        """
        self._ensure_loaded()

        results = []
        for period in range(self.num_periods):
            result = self.get_link_results(link_id, period)
            if result:
                result["period"] = period
                results.append(result)

        return results

    def to_dict(self) -> Dict[str, Any]:
        """
        Get full output as dictionary.

        Returns:
            Complete output dictionary
        """
        self._ensure_loaded()
        return self._output.copy()

    def is_valid(self) -> bool:
        """Check if output file was parsed successfully."""
        self._ensure_loaded()
        return self.prolog.get("valid", False)

    # === Statistics ===

    def summary(self) -> Dict[str, Any]:
        """
        Get output summary statistics.

        Returns:
            Dictionary with summary information
        """
        self._ensure_loaded()
        return {
            "valid": self.is_valid(),
            "version": self.version,
            "num_nodes": self.num_nodes,
            "num_links": self.num_links,
            "num_pumps": self.num_pumps,
            "num_periods": self.num_periods,
            "report_time_step": self.report_time_step,
            "simulation_duration": self.simulation_duration,
            "title": self.title
        }

    def __repr__(self) -> str:
        """String representation."""
        self._ensure_loaded()
        summary = self.summary()
        return (
            f"EpanetOutput('{self._filepath.name}'): "
            f"{summary['num_nodes']} nodes, "
            f"{summary['num_links']} links, "
            f"{summary['num_periods']} periods"
        )
