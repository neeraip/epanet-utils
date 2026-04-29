"""
EPANET High-Level Report File Interface

Provides an easy-to-use interface for reading EPANET report files.

Example:
    >>> from epanet_utils import EpanetReport
    >>>
    >>> with EpanetReport("simulation.rpt") as report:
    ...     print(f"Nodes: {len(report.node_results)}")
    ...     print(f"Links: {len(report.link_results)}")
    ...
    ...     # Get specific node data
    ...     for node in report.node_results:
    ...         print(node['node_id'], node.get('pressure', 'N/A'))
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from .rpt_decoder import EpanetReportDecoder


class EpanetReport:
    """
    High-level interface for EPANET report files.

    Supports context manager protocol for convenient file handling.
    Provides property-based access to report data.

    Example:
        >>> with EpanetReport("simulation.rpt") as report:
        ...     for node in report.node_results:
        ...         print(node['node_id'], node.get('pressure'))
    """

    def __init__(self, filepath: Union[str, Path]):
        """
        Initialize EPANET report handler.

        Args:
            filepath: Path to report file (.rpt)
        """
        self._filepath = Path(filepath)
        self._decoder = EpanetReportDecoder()
        self._report = None
        self._loaded = False

    def __enter__(self):
        """Context manager entry - load report."""
        self._load()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        return False

    def _load(self) -> None:
        """Load and parse the report file."""
        if not self._loaded:
            self._report = self._decoder.decode_file(self._filepath)
            self._loaded = True

    def _ensure_loaded(self) -> None:
        """Ensure report is loaded."""
        if not self._loaded:
            self._load()

    # === Properties ===

    @property
    def version(self) -> Optional[str]:
        """Get EPANET version."""
        self._ensure_loaded()
        return self._report.get("version")

    @property
    def analysis_begun(self) -> Optional[str]:
        """Get analysis begin timestamp."""
        self._ensure_loaded()
        return self._report.get("analysis_begun")

    @property
    def analysis_ended(self) -> Optional[str]:
        """Get analysis end timestamp."""
        self._ensure_loaded()
        return self._report.get("analysis_ended")

    @property
    def hydraulic_status(self) -> List[Dict[str, Any]]:
        """Get hydraulic status events."""
        self._ensure_loaded()
        return self._report.get("hydraulic_status", [])

    @property
    def flow_balance(self) -> Dict[str, Any]:
        """Get hydraulic flow balance."""
        self._ensure_loaded()
        return self._report.get("flow_balance", {})

    @property
    def quality_balance(self) -> Dict[str, Any]:
        """Get water quality mass balance."""
        self._ensure_loaded()
        return self._report.get("quality_balance", {})

    @property
    def energy_usage(self) -> Dict[str, Any]:
        """Get energy usage summary."""
        self._ensure_loaded()
        return self._report.get("energy_usage", {})

    @property
    def node_results(self) -> List[Dict[str, Any]]:
        """Get node results."""
        self._ensure_loaded()
        return self._report.get("node_results", [])

    @property
    def link_results(self) -> List[Dict[str, Any]]:
        """Get link results."""
        self._ensure_loaded()
        return self._report.get("link_results", [])

    @property
    def time_series(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get time series data."""
        self._ensure_loaded()
        return self._report.get("time_series", {"nodes": [], "links": []})

    @property
    def warnings(self) -> List[str]:
        """Get warning messages."""
        self._ensure_loaded()
        return self._report.get("warnings", [])

    @property
    def errors(self) -> List[str]:
        """Get error messages."""
        self._ensure_loaded()
        return self._report.get("errors", [])

    # === DataFrame Methods ===

    def to_dataframe(self, section: str, element_name: Optional[str] = None) -> 'pd.DataFrame':
        """
        Convert a section to pandas DataFrame.

        Args:
            section: Section name ('node_results', 'link_results', 'energy_usage')
            element_name: Optional filter by element ID

        Returns:
            DataFrame containing section data
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame support")

        self._ensure_loaded()

        if section == "node_results":
            data = self.node_results
            id_col = "node_id"
        elif section == "link_results":
            data = self.link_results
            id_col = "link_id"
        elif section == "energy_usage":
            pumps = self.energy_usage.get("pumps", [])
            data = pumps if pumps else [self.energy_usage]
            id_col = "pump_id"
        else:
            data = self._report.get(section, [])
            id_col = "id"

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Filter by element name if specified
        if element_name and id_col in df.columns:
            df = df[df[id_col] == element_name]

        return df

    def nodes_to_dataframe(self) -> 'pd.DataFrame':
        """Get node results as DataFrame."""
        return self.to_dataframe("node_results")

    def links_to_dataframe(self) -> 'pd.DataFrame':
        """Get link results as DataFrame."""
        return self.to_dataframe("link_results")

    # === Helper Methods ===

    def get_node_result(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Get result for a specific node.

        Args:
            node_id: Node ID

        Returns:
            Node result dict or None
        """
        self._ensure_loaded()
        for node in self.node_results:
            if str(node.get("node_id")) == str(node_id):
                return node
        return None

    def get_link_result(self, link_id: str) -> Optional[Dict[str, Any]]:
        """
        Get result for a specific link.

        Args:
            link_id: Link ID

        Returns:
            Link result dict or None
        """
        self._ensure_loaded()
        for link in self.link_results:
            if str(link.get("link_id")) == str(link_id):
                return link
        return None

    def get_pump_energy(self, pump_id: str) -> Optional[Dict[str, Any]]:
        """
        Get energy usage for a specific pump.

        Args:
            pump_id: Pump ID

        Returns:
            Pump energy dict or None
        """
        self._ensure_loaded()
        pumps = self.energy_usage.get("pumps", [])
        for pump in pumps:
            if str(pump.get("pump_id")) == str(pump_id):
                return pump
        return None

    def to_dict(self) -> Dict[str, Any]:
        """
        Get full report as dictionary.

        Returns:
            Complete report dictionary
        """
        self._ensure_loaded()
        return self._report.copy()

    def has_errors(self) -> bool:
        """Check if report contains errors."""
        self._ensure_loaded()
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """Check if report contains warnings."""
        self._ensure_loaded()
        return len(self.warnings) > 0

    # === Statistics ===

    def summary(self) -> Dict[str, Any]:
        """
        Get report summary statistics.

        Returns:
            Dictionary with summary information
        """
        self._ensure_loaded()
        return {
            "nodes_count": len(self.node_results),
            "links_count": len(self.link_results),
            "warnings_count": len(self.warnings),
            "errors_count": len(self.errors),
            "has_energy_data": bool(self.energy_usage),
            "has_time_series": bool(self.time_series.get("nodes") or self.time_series.get("links"))
        }

    def __repr__(self) -> str:
        """String representation."""
        self._ensure_loaded()
        summary = self.summary()
        return (
            f"EpanetReport('{self._filepath.name}'): "
            f"{summary['nodes_count']} nodes, "
            f"{summary['links_count']} links, "
            f"{summary['warnings_count']} warnings, "
            f"{summary['errors_count']} errors"
        )
