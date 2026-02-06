"""
EPANET High-Level Input File Interface

Provides an easy-to-use interface for reading and manipulating EPANET input files.

Example:
    >>> from epanet_utils import EpanetInput
    >>> 
    >>> # Read and modify a model
    >>> with EpanetInput("network.inp") as model:
    ...     print(f"Junctions: {len(model.junctions)}")
    ...     print(f"Pipes: {len(model.pipes)}")
    ...     
    ...     # Modify a junction
    ...     model.junctions[0]['elevation'] = 100
    ...     
    ...     # Save changes
    ...     model.save("modified.inp")
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from .inp_decoder import EpanetInputDecoder
from .inp_encoder import EpanetInputEncoder


class EpanetInput:
    """
    High-level interface for EPANET input files.
    
    Supports context manager protocol for convenient file handling.
    Provides property-based access to model components.
    
    Example:
        >>> with EpanetInput("network.inp") as model:
        ...     for junction in model.junctions:
        ...         print(junction['id'], junction['elevation'])
    """
    
    def __init__(self, filepath: Optional[Union[str, Path]] = None, model_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize EPANET input handler.
        
        Args:
            filepath: Path to input file (optional)
            model_dict: Pre-loaded model dictionary (optional)
        """
        self._filepath = Path(filepath) if filepath else None
        self._decoder = EpanetInputDecoder()
        self._encoder = EpanetInputEncoder()
        
        if model_dict:
            self._model = model_dict
        elif filepath:
            self._model = self._decoder.decode_file(filepath)
        else:
            # Create empty model
            self._model = {"metadata": {"format": "epanet_inp", "version": "2.2"}}
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        return False
    
    # === File Operations ===
    
    def save(self, filepath: Optional[Union[str, Path]] = None) -> None:
        """
        Save model to .inp file.
        
        Args:
            filepath: Output path (uses original path if not specified)
        """
        if filepath is None:
            if self._filepath is None:
                raise ValueError("No filepath specified")
            filepath = self._filepath
        
        self._encoder.encode_to_inp_file(self._model, filepath)
    
    def to_json(self, filepath: Union[str, Path], pretty: bool = True) -> None:
        """
        Export model to JSON format.
        
        Args:
            filepath: Output JSON file path
            pretty: Whether to format with indentation
        """
        self._encoder.encode_to_json(self._model, filepath, pretty=pretty)
    
    def to_parquet(self, filepath: Union[str, Path], single_file: bool = False) -> None:
        """
        Export model to Parquet format.
        
        Args:
            filepath: Output path (file or directory)
            single_file: If True, create single file; otherwise directory
        """
        self._encoder.encode_to_parquet(self._model, filepath, single_file=single_file)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get model as dictionary.
        
        Returns:
            Model dictionary
        """
        return self._model.copy()
    
    def to_dataframe(self, section: str) -> 'pd.DataFrame':
        """
        Get a section as a pandas DataFrame.
        
        Args:
            section: Section name (e.g., 'junctions', 'pipes')
            
        Returns:
            DataFrame containing section data
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame support")
        
        data = self._model.get(section.lower(), [])
        
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            return pd.DataFrame()
    
    # === Model Properties ===
    
    @property
    def title(self) -> str:
        """Get/set model title."""
        return self._model.get("title", "")
    
    @title.setter
    def title(self, value: str) -> None:
        self._model["title"] = value
    
    @property
    def junctions(self) -> List[Dict[str, Any]]:
        """Get junctions list."""
        return self._model.setdefault("junctions", [])
    
    @property
    def reservoirs(self) -> List[Dict[str, Any]]:
        """Get reservoirs list."""
        return self._model.setdefault("reservoirs", [])
    
    @property
    def tanks(self) -> List[Dict[str, Any]]:
        """Get tanks list."""
        return self._model.setdefault("tanks", [])
    
    @property
    def pipes(self) -> List[Dict[str, Any]]:
        """Get pipes list."""
        return self._model.setdefault("pipes", [])
    
    @property
    def pumps(self) -> List[Dict[str, Any]]:
        """Get pumps list."""
        return self._model.setdefault("pumps", [])
    
    @property
    def valves(self) -> List[Dict[str, Any]]:
        """Get valves list."""
        return self._model.setdefault("valves", [])
    
    @property
    def patterns(self) -> List[Dict[str, Any]]:
        """Get patterns list."""
        return self._model.setdefault("patterns", [])
    
    @property
    def curves(self) -> List[Dict[str, Any]]:
        """Get curves list."""
        return self._model.setdefault("curves", [])
    
    @property
    def controls(self) -> str:
        """Get/set control rules text."""
        return self._model.get("controls", "")
    
    @controls.setter
    def controls(self, value: str) -> None:
        self._model["controls"] = value
    
    @property
    def rules(self) -> str:
        """Get/set rule-based controls text."""
        return self._model.get("rules", "")
    
    @rules.setter
    def rules(self, value: str) -> None:
        self._model["rules"] = value
    
    @property
    def options(self) -> Dict[str, Any]:
        """Get options dictionary."""
        return self._model.setdefault("options", {})
    
    @property
    def times(self) -> Dict[str, Any]:
        """Get time settings dictionary."""
        return self._model.setdefault("times", {})
    
    @property
    def coordinates(self) -> List[Dict[str, Any]]:
        """Get node coordinates list."""
        return self._model.setdefault("coordinates", [])
    
    @property
    def quality(self) -> List[Dict[str, Any]]:
        """Get initial quality list."""
        return self._model.setdefault("quality", [])
    
    @property
    def reactions(self) -> Dict[str, Any]:
        """Get reaction settings dictionary."""
        return self._model.setdefault("reactions", {})
    
    @property
    def energy(self) -> Dict[str, Any]:
        """Get energy settings dictionary."""
        return self._model.setdefault("energy", {})
    
    @property
    def emitters(self) -> List[Dict[str, Any]]:
        """Get emitters list."""
        return self._model.setdefault("emitters", [])
    
    @property
    def sources(self) -> List[Dict[str, Any]]:
        """Get water quality sources list."""
        return self._model.setdefault("sources", [])
    
    @property
    def demands(self) -> List[Dict[str, Any]]:
        """Get demands list."""
        return self._model.setdefault("demands", [])
    
    @property
    def status(self) -> List[Dict[str, Any]]:
        """Get link status list."""
        return self._model.setdefault("status", [])
    
    @property
    def tags(self) -> List[Dict[str, Any]]:
        """Get tags list."""
        return self._model.setdefault("tags", [])
    
    @property
    def mixing(self) -> List[Dict[str, Any]]:
        """Get tank mixing models list."""
        return self._model.setdefault("mixing", [])
    
    @property
    def vertices(self) -> List[Dict[str, Any]]:
        """Get link vertices list."""
        return self._model.setdefault("vertices", [])
    
    @property
    def labels(self) -> List[Dict[str, Any]]:
        """Get map labels list."""
        return self._model.setdefault("labels", [])
    
    @property
    def backdrop(self) -> Dict[str, Any]:
        """Get backdrop settings dictionary."""
        return self._model.setdefault("backdrop", {})
    
    @property
    def report(self) -> Dict[str, Any]:
        """Get report settings dictionary."""
        return self._model.setdefault("report", {})
    
    # === Helper Methods ===
    
    def get_junction(self, junction_id: str) -> Optional[Dict[str, Any]]:
        """
        Get junction by ID.
        
        Args:
            junction_id: Junction ID
            
        Returns:
            Junction dict or None if not found
        """
        for junction in self.junctions:
            if str(junction.get("id")) == str(junction_id):
                return junction
        return None
    
    def get_pipe(self, pipe_id: str) -> Optional[Dict[str, Any]]:
        """
        Get pipe by ID.
        
        Args:
            pipe_id: Pipe ID
            
        Returns:
            Pipe dict or None if not found
        """
        for pipe in self.pipes:
            if str(pipe.get("id")) == str(pipe_id):
                return pipe
        return None
    
    def get_pump(self, pump_id: str) -> Optional[Dict[str, Any]]:
        """
        Get pump by ID.
        
        Args:
            pump_id: Pump ID
            
        Returns:
            Pump dict or None if not found
        """
        for pump in self.pumps:
            if str(pump.get("id")) == str(pump_id):
                return pump
        return None
    
    def get_tank(self, tank_id: str) -> Optional[Dict[str, Any]]:
        """
        Get tank by ID.
        
        Args:
            tank_id: Tank ID
            
        Returns:
            Tank dict or None if not found
        """
        for tank in self.tanks:
            if str(tank.get("id")) == str(tank_id):
                return tank
        return None
    
    def get_reservoir(self, reservoir_id: str) -> Optional[Dict[str, Any]]:
        """
        Get reservoir by ID.
        
        Args:
            reservoir_id: Reservoir ID
            
        Returns:
            Reservoir dict or None if not found
        """
        for reservoir in self.reservoirs:
            if str(reservoir.get("id")) == str(reservoir_id):
                return reservoir
        return None
    
    def get_valve(self, valve_id: str) -> Optional[Dict[str, Any]]:
        """
        Get valve by ID.
        
        Args:
            valve_id: Valve ID
            
        Returns:
            Valve dict or None if not found
        """
        for valve in self.valves:
            if str(valve.get("id")) == str(valve_id):
                return valve
        return None
    
    def get_pattern(self, pattern_id: str) -> Optional[Dict[str, Any]]:
        """
        Get pattern by ID.
        
        Args:
            pattern_id: Pattern ID
            
        Returns:
            Pattern dict or None if not found
        """
        for pattern in self.patterns:
            if str(pattern.get("id")) == str(pattern_id):
                return pattern
        return None
    
    def get_curve(self, curve_id: str) -> Optional[Dict[str, Any]]:
        """
        Get curve by ID.
        
        Args:
            curve_id: Curve ID
            
        Returns:
            Curve dict or None if not found
        """
        for curve in self.curves:
            if str(curve.get("id")) == str(curve_id):
                return curve
        return None
    
    def add_junction(self, junction_id: str, elevation: float, demand: float = 0, pattern: str = "") -> Dict[str, Any]:
        """
        Add a new junction.
        
        Args:
            junction_id: Junction ID
            elevation: Elevation
            demand: Base demand
            pattern: Demand pattern ID
            
        Returns:
            The added junction dict
        """
        junction = {
            "id": junction_id,
            "elevation": elevation,
            "demand": demand,
            "pattern": pattern
        }
        self.junctions.append(junction)
        return junction
    
    def add_pipe(self, pipe_id: str, node1: str, node2: str, length: float, 
                 diameter: float, roughness: float, minor_loss: float = 0, status: str = "Open") -> Dict[str, Any]:
        """
        Add a new pipe.
        
        Args:
            pipe_id: Pipe ID
            node1: Start node ID
            node2: End node ID
            length: Pipe length
            diameter: Pipe diameter
            roughness: Roughness coefficient
            minor_loss: Minor loss coefficient
            status: Initial status
            
        Returns:
            The added pipe dict
        """
        pipe = {
            "id": pipe_id,
            "node1": node1,
            "node2": node2,
            "length": length,
            "diameter": diameter,
            "roughness": roughness,
            "minor_loss": minor_loss,
            "status": status
        }
        self.pipes.append(pipe)
        return pipe
    
    def add_reservoir(self, reservoir_id: str, head: float, pattern: str = "") -> Dict[str, Any]:
        """
        Add a new reservoir.
        
        Args:
            reservoir_id: Reservoir ID
            head: Total head
            pattern: Head pattern ID
            
        Returns:
            The added reservoir dict
        """
        reservoir = {
            "id": reservoir_id,
            "head": head,
            "pattern": pattern
        }
        self.reservoirs.append(reservoir)
        return reservoir
    
    def add_tank(self, tank_id: str, elevation: float, init_level: float, 
                 min_level: float, max_level: float, diameter: float,
                 min_vol: float = 0, vol_curve: str = "") -> Dict[str, Any]:
        """
        Add a new tank.
        
        Args:
            tank_id: Tank ID
            elevation: Bottom elevation
            init_level: Initial water level
            min_level: Minimum water level
            max_level: Maximum water level
            diameter: Tank diameter
            min_vol: Minimum volume
            vol_curve: Volume curve ID
            
        Returns:
            The added tank dict
        """
        tank = {
            "id": tank_id,
            "elevation": elevation,
            "init_level": init_level,
            "min_level": min_level,
            "max_level": max_level,
            "diameter": diameter,
            "min_vol": min_vol,
            "vol_curve": vol_curve
        }
        self.tanks.append(tank)
        return tank
    
    def add_pump(self, pump_id: str, node1: str, node2: str, parameters: str) -> Dict[str, Any]:
        """
        Add a new pump.
        
        Args:
            pump_id: Pump ID
            node1: Suction node ID
            node2: Delivery node ID
            parameters: Pump parameters (e.g., "HEAD 1")
            
        Returns:
            The added pump dict
        """
        pump = {
            "id": pump_id,
            "node1": node1,
            "node2": node2,
            "parameters": parameters
        }
        self.pumps.append(pump)
        return pump
    
    def add_pattern(self, pattern_id: str, multipliers: List[float]) -> Dict[str, Any]:
        """
        Add a new time pattern.
        
        Args:
            pattern_id: Pattern ID
            multipliers: List of multiplier values
            
        Returns:
            The added pattern dict
        """
        pattern = {
            "id": pattern_id,
            "multipliers": multipliers
        }
        self.patterns.append(pattern)
        return pattern
    
    def add_curve(self, curve_id: str, points: List[tuple]) -> Dict[str, Any]:
        """
        Add a new data curve.
        
        Args:
            curve_id: Curve ID
            points: List of (x, y) tuples
            
        Returns:
            The added curve dict
        """
        curve = {
            "id": curve_id,
            "points": [{"x": x, "y": y} for x, y in points]
        }
        self.curves.append(curve)
        return curve
    
    # === Statistics ===
    
    def summary(self) -> Dict[str, int]:
        """
        Get model component counts.
        
        Returns:
            Dictionary with component counts
        """
        return {
            "junctions": len(self.junctions),
            "reservoirs": len(self.reservoirs),
            "tanks": len(self.tanks),
            "pipes": len(self.pipes),
            "pumps": len(self.pumps),
            "valves": len(self.valves),
            "patterns": len(self.patterns),
            "curves": len(self.curves),
        }
    
    def __repr__(self) -> str:
        """String representation."""
        summary = self.summary()
        filepath = self._filepath.name if self._filepath else "new model"
        return (
            f"EpanetInput('{filepath}'): "
            f"{summary['junctions']} junctions, "
            f"{summary['reservoirs']} reservoirs, "
            f"{summary['tanks']} tanks, "
            f"{summary['pipes']} pipes, "
            f"{summary['pumps']} pumps, "
            f"{summary['valves']} valves"
        )
