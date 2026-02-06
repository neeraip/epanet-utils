"""
EPANET Report File Decoder

Decodes EPANET report files (.rpt) into Python dictionaries.

EPANET Report File Sections:
- Title and Notes
- Hydraulic Status (timestep events)
- Hydraulic Flow Balance
- Water Quality Mass Balance
- Node Results (pressures, demands, quality)
- Link Results (flows, velocities, headloss)
- Energy Usage Summary
- Node/Link Time Series (if requested in [REPORT])
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


class EpanetReportDecoder:
    """
    Decoder for EPANET report files (.rpt).
    
    Parses simulation output reports including:
    - EPANET version and timestamps
    - Hydraulic status messages
    - Flow and mass balances
    - Node results (pressure, demand, quality)
    - Link results (flow, velocity, headloss)
    - Energy usage summary
    
    Example:
        >>> decoder = EpanetReportDecoder()
        >>> report = decoder.decode_file("simulation.rpt")
        >>> print(report['flow_balance'])
    """
    
    def __init__(self):
        """Initialize the decoder."""
        pass
    
    def decode_file(self, filepath: Union[str, Path]) -> Dict[str, Any]:
        """
        Decode an EPANET report file.
        
        Args:
            filepath: Path to .rpt file
            
        Returns:
            Dictionary containing parsed report data
        """
        filepath = Path(filepath)
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        return self.decode_string(content)
    
    def decode_string(self, content: str) -> Dict[str, Any]:
        """
        Decode EPANET report content from a string.
        
        Args:
            content: String content of .rpt file
            
        Returns:
            Dictionary containing parsed report data
        """
        report = {
            "version": None,
            "analysis_begun": None,
            "analysis_ended": None,
            "hydraulic_status": [],
            "flow_balance": {},
            "quality_balance": {},
            "node_results": [],
            "link_results": [],
            "energy_usage": {},
            "warnings": [],
            "errors": []
        }
        
        lines = content.split('\n')
        
        # Parse different sections
        report["version"] = self._parse_version(lines)
        report["analysis_begun"], report["analysis_ended"] = self._parse_timestamps(lines)
        report["hydraulic_status"] = self._parse_hydraulic_status(lines)
        report["flow_balance"] = self._parse_flow_balance(lines)
        report["quality_balance"] = self._parse_quality_balance(lines)
        report["node_results"] = self._parse_node_results(lines)
        report["link_results"] = self._parse_link_results(lines)
        report["energy_usage"] = self._parse_energy_usage(lines)
        report["warnings"] = self._parse_warnings(lines)
        report["errors"] = self._parse_errors(lines)
        
        return report
    
    def _parse_version(self, lines: List[str]) -> Optional[str]:
        """Parse EPANET version from header."""
        for line in lines:
            if 'Version' in line:
                match = re.search(r'Version\s+([\d.]+)', line)
                if match:
                    return match.group(1)
        return None
    
    def _parse_timestamps(self, lines: List[str]) -> tuple:
        """Parse analysis begin and end timestamps."""
        begun = None
        ended = None
        
        for line in lines:
            stripped = line.strip()
            if 'Analysis begun' in stripped:
                begun = stripped.replace('Analysis begun', '').strip()
            elif 'Analysis ended' in stripped:
                ended = stripped.replace('Analysis ended', '').strip()
        
        return begun, ended
    
    def _parse_hydraulic_status(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse hydraulic status messages."""
        status_events = []
        in_status = False
        
        for line in lines:
            stripped = line.strip()
            
            if 'Hydraulic Status:' in stripped:
                in_status = True
                continue
            
            if in_status:
                # End of status section
                if stripped.startswith('Hydraulic Flow Balance') or stripped.startswith('==='):
                    break
                
                if not stripped or stripped.startswith('-'):
                    continue
                
                # Parse status line: "0:00:00: Message"
                match = re.match(r'(\d+:\d+:\d+):\s*(.+)', stripped)
                if match:
                    time_str = match.group(1)
                    message = match.group(2)
                    
                    status_events.append({
                        "time": time_str,
                        "message": message
                    })
        
        return status_events
    
    def _parse_flow_balance(self, lines: List[str]) -> Dict[str, Any]:
        """Parse hydraulic flow balance section."""
        balance = {}
        in_balance = False
        has_data = False  # Track if we've parsed actual data fields
        
        for line in lines:
            stripped = line.strip()
            
            if 'Hydraulic Flow Balance' in stripped or 'Flow Balance' in stripped:
                in_balance = True
                # Extract units if present
                match = re.search(r'\((\w+)\)', stripped)
                if match:
                    balance["units"] = match.group(1)
                continue
            
            if in_balance:
                if stripped.startswith('===') or not stripped:
                    # Only break if we've parsed actual data (not just units)
                    if has_data:
                        break
                    continue
                
                # Parse key-value lines
                if ':' in stripped:
                    parts = stripped.split(':', 1)
                    key = parts[0].strip().lower().replace(' ', '_')
                    value = parts[1].strip()
                    balance[key] = self._convert_value(value)
                    has_data = True
        
        return balance
    
    def _parse_quality_balance(self, lines: List[str]) -> Dict[str, Any]:
        """Parse water quality mass balance section."""
        balance = {}
        in_balance = False
        has_data = False  # Track if we've parsed actual data fields
        
        for line in lines:
            stripped = line.strip()
            
            if 'Water Quality Mass Balance' in stripped or 'Quality Mass Balance' in stripped:
                in_balance = True
                # Extract units if present
                match = re.search(r'\((\w+)\)', stripped)
                if match:
                    balance["units"] = match.group(1)
                continue
            
            if in_balance:
                if stripped.startswith('===') or not stripped:
                    # Only break if we've parsed actual data (not just units)
                    if has_data:
                        break
                    continue
                
                # Parse key-value lines
                if ':' in stripped:
                    parts = stripped.split(':', 1)
                    key = parts[0].strip().lower().replace(' ', '_')
                    value = parts[1].strip()
                    balance[key] = self._convert_value(value)
                    has_data = True
        
        return balance
    
    def _parse_node_results(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse node results section."""
        results = []
        in_section = False
        headers = []
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # Look for node results header
            if 'Node Results' in stripped:
                in_section = True
                continue
            
            if in_section:
                # End of section
                if stripped.startswith('Link Results') or (stripped.startswith('===') and results):
                    break
                
                if not stripped or stripped.startswith('-') or stripped.startswith('==='):
                    continue
                
                # Parse header row (contains Node, Demand, Head, Pressure, Quality)
                if 'Node' in stripped and ('Demand' in stripped or 'Head' in stripped or 'Pressure' in stripped):
                    headers = [h.strip().lower().replace(' ', '_') for h in stripped.split()]
                    continue
                
                # Parse data row
                parts = stripped.split()
                if len(parts) >= 2 and headers:
                    row = {}
                    for j, val in enumerate(parts):
                        if j < len(headers):
                            row[headers[j]] = self._convert_value(val)
                    if row:
                        results.append(row)
        
        return results
    
    def _parse_link_results(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse link results section."""
        results = []
        in_section = False
        headers = []
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # Look for link results header
            if 'Link Results' in stripped:
                in_section = True
                continue
            
            if in_section:
                # End of section
                if stripped.startswith('===') and results:
                    break
                if stripped.startswith('Node Results'):
                    break
                if 'Energy' in stripped:
                    break
                
                if not stripped or stripped.startswith('-'):
                    continue
                
                # Parse header row
                if 'Link' in stripped and ('Flow' in stripped or 'Velocity' in stripped):
                    headers = [h.strip().lower().replace(' ', '_') for h in stripped.split()]
                    continue
                
                # Parse data row
                parts = stripped.split()
                if len(parts) >= 2 and headers:
                    row = {}
                    for j, val in enumerate(parts):
                        if j < len(headers):
                            row[headers[j]] = self._convert_value(val)
                    if row:
                        results.append(row)
        
        return results
    
    def _parse_energy_usage(self, lines: List[str]) -> Dict[str, Any]:
        """Parse energy usage section."""
        energy = {"pumps": []}
        in_section = False
        headers = []
        
        for line in lines:
            stripped = line.strip()
            
            if 'Energy Usage' in stripped or 'Energy Report' in stripped:
                in_section = True
                continue
            
            if in_section:
                if stripped.startswith('===') and energy["pumps"]:
                    break
                
                if not stripped or stripped.startswith('-'):
                    continue
                
                # Parse header
                if 'Pump' in stripped and 'Percent' in stripped:
                    headers = ['pump', 'percent_utilization', 'avg_efficiency', 
                              'kwh_per_flow', 'avg_kw', 'peak_kw', 'cost_per_day']
                    continue
                
                # Parse pump data
                parts = stripped.split()
                if len(parts) >= 2 and not stripped.startswith('Pump'):
                    pump_data = {"pump_id": parts[0]}
                    for j, val in enumerate(parts[1:], 1):
                        if j < len(headers):
                            pump_data[headers[j]] = self._convert_value(val)
                    energy["pumps"].append(pump_data)
                
                # Parse totals
                if ':' in stripped:
                    parts = stripped.split(':', 1)
                    key = parts[0].strip().lower().replace(' ', '_')
                    value = parts[1].strip().replace('$', '').replace(',', '')
                    energy[key] = self._convert_value(value)
        
        return energy
    
    def _parse_warnings(self, lines: List[str]) -> List[str]:
        """Parse warning messages."""
        warnings = []
        
        for line in lines:
            stripped = line.strip()
            if 'WARNING' in stripped.upper():
                warnings.append(stripped)
        
        return warnings
    
    def _parse_errors(self, lines: List[str]) -> List[str]:
        """Parse error messages."""
        errors = []
        
        for line in lines:
            stripped = line.strip()
            if 'ERROR' in stripped.upper():
                errors.append(stripped)
        
        return errors
    
    def _convert_value(self, value: str) -> Any:
        """Convert a string value to appropriate type."""
        if not value:
            return None
        
        # Remove common formatting
        value = value.strip().replace(',', '')
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float (including scientific notation)
        try:
            return float(value)
        except ValueError:
            pass
        
        return value
