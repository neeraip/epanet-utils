"""
EPANET Input File Encoder

Encodes Python dictionaries to EPANET input files (.inp), JSON, and Parquet formats.

Supported output formats:
- .inp (EPANET native format)
- .json (JSON representation)
- .parquet (single-file or multi-file Parquet)
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class EpanetInputEncoder:
    """
    Encoder for EPANET input files.
    
    Supports encoding to:
    - .inp files (native EPANET format)
    - .json files (JSON representation)
    - .parquet files (single or multi-file)
    
    Example:
        >>> encoder = EpanetInputEncoder()
        >>> encoder.encode_to_inp_file(model_dict, "network.inp")
        >>> encoder.encode_to_json(model_dict, "network.json")
    """
    
    # Sections in output order
    SECTION_ORDER = [
        "title",
        "junctions",
        "reservoirs",
        "tanks",
        "pipes",
        "pumps",
        "valves",
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
        "coordinates",
        "vertices",
        "labels",
        "backdrop",
    ]
    
    # Text sections
    TEXT_SECTIONS = {"title", "controls", "rules"}
    
    # Key-value sections
    KEYVALUE_SECTIONS = {"energy", "reactions", "times", "report", "options", "backdrop"}
    
    # Column order for tabular sections
    SECTION_COLUMNS = {
        "junctions": ["id", "elevation", "demand", "pattern"],
        "reservoirs": ["id", "head", "pattern"],
        "tanks": ["id", "elevation", "init_level", "min_level", "max_level", "diameter", "min_vol", "vol_curve"],
        "pipes": ["id", "node1", "node2", "length", "diameter", "roughness", "minor_loss", "status"],
        "pumps": ["id", "node1", "node2", "parameters"],
        "valves": ["id", "node1", "node2", "diameter", "type", "setting", "minor_loss"],
        "tags": ["object_type", "object_id", "tag"],
        "demands": ["junction", "demand", "pattern", "category"],
        "status": ["id", "status"],
        "emitters": ["junction", "coefficient"],
        "quality": ["node", "init_qual"],
        "sources": ["node", "type", "quality", "pattern"],
        "mixing": ["tank", "model", "fraction"],
        "coordinates": ["node", "x_coord", "y_coord"],
        "vertices": ["link", "x_coord", "y_coord"],
        "labels": ["x_coord", "y_coord", "label", "anchor"],
    }
    
    # Section headers with comments
    SECTION_HEADERS = {
        "junctions": ";ID              \tElev        \tDemand      \tPattern         ",
        "reservoirs": ";ID              \tHead        \tPattern         ",
        "tanks": ";ID              \tElevation   \tInitLevel   \tMinLevel    \tMaxLevel    \tDiameter    \tMinVol      \tVolCurve",
        "pipes": ";ID              \tNode1           \tNode2           \tLength      \tDiameter    \tRoughness   \tMinorLoss   \tStatus",
        "pumps": ";ID              \tNode1           \tNode2           \tParameters",
        "valves": ";ID              \tNode1           \tNode2           \tDiameter    \tType\tSetting     \tMinorLoss   ",
        "tags": ";Type            \tID              \tTag",
        "demands": ";Junction        \tDemand      \tPattern         \tCategory",
        "status": ";ID              \tStatus/Setting",
        "patterns": ";ID              \tMultipliers",
        "curves": ";ID              \tX-Value     \tY-Value",
        "emitters": ";Junction        \tCoefficient",
        "quality": ";Node            \tInitQual",
        "sources": ";Node            \tType        \tQuality     \tPattern",
        "mixing": ";Tank            \tModel",
        "coordinates": ";Node            \tX-Coord         \tY-Coord",
        "vertices": ";Link            \tX-Coord         \tY-Coord",
        "labels": ";X-Coord           Y-Coord          Label & Anchor Node",
    }
    
    def __init__(self):
        """Initialize the encoder."""
        pass
    
    def encode_to_inp_string(self, model: Dict[str, Any]) -> str:
        """
        Encode model to EPANET .inp format string.
        
        Args:
            model: Dictionary containing model data
            
        Returns:
            String in EPANET .inp format
        """
        lines = []
        
        for section in self.SECTION_ORDER:
            if section not in model:
                continue
            
            section_data = model[section]
            
            # Skip empty sections
            if section_data is None:
                continue
            if isinstance(section_data, (list, dict)) and not section_data:
                continue
            if isinstance(section_data, str) and not section_data.strip():
                continue
            
            # Add section header
            lines.append(f"[{section.upper()}]")
            
            # Encode section content
            if section in self.TEXT_SECTIONS:
                lines.append(self._encode_text_section(section_data))
            elif section == "patterns":
                lines.append(self._encode_patterns_section(section_data))
            elif section == "curves":
                lines.append(self._encode_curves_section(section_data))
            elif section in self.KEYVALUE_SECTIONS:
                lines.append(self._encode_keyvalue_section(section_data, section))
            elif section in self.SECTION_COLUMNS:
                lines.append(self._encode_table_section(section_data, section))
            else:
                # Unknown section - try as text
                if isinstance(section_data, str):
                    lines.append(section_data)
            
            lines.append("")  # Empty line between sections
        
        # Add END marker
        lines.append("[END]")
        lines.append("")
        
        return '\n'.join(lines)
    
    def encode_to_inp_file(self, model: Dict[str, Any], filepath: Union[str, Path]) -> None:
        """
        Encode model to EPANET .inp file.
        
        Args:
            model: Dictionary containing model data
            filepath: Output file path
        """
        filepath = Path(filepath)
        content = self.encode_to_inp_string(model)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def encode_to_json(self, model: Dict[str, Any], filepath: Union[str, Path], pretty: bool = False) -> None:
        """
        Encode model to JSON file.
        
        Args:
            model: Dictionary containing model data
            filepath: Output file path
            pretty: Whether to format JSON with indentation
        """
        filepath = Path(filepath)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(model, f, indent=2)
            else:
                json.dump(model, f)
    
    def encode_to_parquet(self, model: Dict[str, Any], filepath: Union[str, Path], single_file: bool = False) -> None:
        """
        Encode model to Parquet format.
        
        Args:
            model: Dictionary containing model data
            filepath: Output file path or directory
            single_file: If True, create single .parquet file; otherwise create directory with multiple files
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas and pyarrow are required for Parquet support")
        
        filepath = Path(filepath)
        
        if single_file:
            self._encode_single_parquet(model, filepath)
        else:
            self._encode_multi_parquet(model, filepath)
    
    def _encode_text_section(self, data: str) -> str:
        """Encode a text section."""
        return data
    
    def _encode_keyvalue_section(self, data: Dict[str, Any], section: str) -> str:
        """Encode a key-value section.

        Inverse of decoder. Two-token keys (``order_wall``,
        ``global_price``) emit as two whitespace-separated tokens. The
        accumulator lists ``pump_settings`` (ENERGY) and ``per_id``
        (REACTIONS) emit one row per entry.
        """
        lines = []

        for key, value in data.items():
            if value is None:
                continue

            if key == "pump_settings" and isinstance(value, list):
                for s in value:
                    if isinstance(s, dict):
                        lines.append(
                            f" Pump\t{s.get('id', '')}\t"
                            f"{str(s.get('param', '')).capitalize()}\t{s.get('value', '')}"
                        )
                    else:
                        # Legacy str payload from older decoders.
                        lines.append(f" Pump \t{s}")
                continue

            if key == "per_id" and isinstance(value, list):
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    lines.append(
                        f" {str(item.get('type', '')).capitalize()}\t"
                        f"{item.get('id', '')}\t{item.get('value', '')}"
                    )
                continue

            if key == "specific" and isinstance(value, list):
                # Legacy shape from older decoders — keep emitting it
                # so existing data.json blobs round-trip.
                for item in value:
                    lines.append(
                        f" {item['type'].capitalize()}\t{item['value']}"
                    )
                continue

            # Convert snake_case to Title Case (two-token keys split
            # on underscore; one-token keys remain one word).
            display_key = ' '.join(word.capitalize() for word in key.split('_'))
            lines.append(f" {display_key}\t{value}")

        return '\n'.join(lines)
    
    def _encode_table_section(self, data: List[Dict[str, Any]], section: str) -> str:
        """Encode a tabular section."""
        lines = []

        # Add header comment
        if section in self.SECTION_HEADERS:
            lines.append(self.SECTION_HEADERS[section])

        columns = self.SECTION_COLUMNS.get(section, [])

        for row in data:
            # Per-row description round-trips as a leading ``;<text>``
            # line (one per physical line so multi-line descriptions
            # stay readable). The EPANET engine ignores comments.
            desc = row.get("description")
            if desc:
                for d_line in str(desc).split("\n"):
                    lines.append(f";{d_line}")

            row_values = []
            for col in columns:
                value = row.get(col, "")
                if value is None:
                    value = ""

                # Format labels with quotes
                if section == "labels" and col == "label":
                    value = f'"{value}"'

                row_values.append(str(value))

            # Format row with tabs
            line = " " + "\t".join(f"{v:<16}" if i == 0 else f"{v:<12}" for i, v in enumerate(row_values))

            # Add semicolon for most table sections
            if section not in ("labels", "backdrop"):
                line = line.rstrip() + "\t;"

            lines.append(line)

        return '\n'.join(lines)
    
    def _encode_patterns_section(self, data: Any) -> str:
        """Encode the PATTERNS section.

        Accepts either the modern dict shape ``{id: [multipliers]}`` or
        the legacy list shape ``[{id, multipliers}]``. Existing
        data.json blobs in S3 may use either.
        """
        lines = []
        lines.append(self.SECTION_HEADERS.get("patterns", ";ID              \tMultipliers"))

        items = self._patterns_items(data)
        for pid, multipliers in items:
            for i in range(0, len(multipliers), 6):
                chunk = multipliers[i:i+6]
                values = "\t".join(f"{v:<12}" for v in chunk)
                lines.append(f" {pid:<16}\t{values}")

        return '\n'.join(lines)

    def _encode_curves_section(self, data: Any) -> str:
        """Encode the CURVES section.

        Accepts dict shape ``{id: {type?, points: [{x,y}]}}`` (modern)
        or list shape ``[{id, points}]`` (legacy). A leading
        ``;PUMP:`` / ``;EFFICIENCY:`` etc. comment is emitted so the
        type round-trips through EPANET — the engine itself ignores
        these comments but the .inp viewer convention depends on them.
        """
        lines = []
        lines.append(self.SECTION_HEADERS.get("curves", ";ID              \tX-Value     \tY-Value"))

        for cid, curve in self._curves_items(data):
            ctype = (curve.get("type") or "").upper()
            if ctype:
                lines.append(f";{ctype}: Curve {cid}")
            for point in curve.get("points", []):
                x = point.get("x", 0)
                y = point.get("y", 0)
                lines.append(f" {cid:<16}\t{x:<12}\t{y:<12}")

        return '\n'.join(lines)

    @staticmethod
    def _patterns_items(data: Any):
        """Yield (id, multipliers) regardless of dict or list shape."""
        if isinstance(data, dict):
            for pid, value in data.items():
                if isinstance(value, list):
                    yield pid, value
                elif isinstance(value, dict):
                    yield pid, value.get("multipliers", [])
        elif isinstance(data, list):
            for p in data:
                yield p.get("id", ""), p.get("multipliers", [])

    @staticmethod
    def _curves_items(data: Any):
        """Yield (id, curve_dict) regardless of dict or list shape."""
        if isinstance(data, dict):
            for cid, curve in data.items():
                if isinstance(curve, dict):
                    yield cid, curve
        elif isinstance(data, list):
            for c in data:
                yield c.get("id", ""), c
    
    def _encode_single_parquet(self, model: Dict[str, Any], filepath: Path) -> None:
        """Encode to a single Parquet file."""
        all_rows = []
        
        for section in self.SECTION_ORDER:
            if section not in model:
                continue
            
            section_data = model[section]
            
            if section in self.TEXT_SECTIONS:
                all_rows.append({"section": section, "content": section_data, "key": None, "value": None})
            elif section in self.KEYVALUE_SECTIONS:
                for key, value in section_data.items():
                    all_rows.append({"section": section, "content": None, "key": key, "value": str(value)})
            elif isinstance(section_data, list):
                for row in section_data:
                    row_data = {"section": section, "content": None, "key": None, "value": None}
                    row_data.update(row)
                    all_rows.append(row_data)
        
        df = pd.DataFrame(all_rows)
        df.to_parquet(filepath, index=False)
    
    def _encode_multi_parquet(self, model: Dict[str, Any], dirpath: Path) -> None:
        """Encode to multiple Parquet files in a directory."""
        dirpath.mkdir(parents=True, exist_ok=True)
        
        # Write metadata
        metadata = model.get("metadata", {})
        if metadata:
            df = pd.DataFrame([metadata])
            df.to_parquet(dirpath / "metadata.parquet", index=False)
        
        for section in self.SECTION_ORDER:
            if section not in model:
                continue
            
            section_data = model[section]
            
            if section in self.TEXT_SECTIONS:
                df = pd.DataFrame([{"content": section_data}])
            elif section in self.KEYVALUE_SECTIONS:
                df = pd.DataFrame([section_data])
            elif isinstance(section_data, list) and section_data:
                df = pd.DataFrame(section_data)
            else:
                continue
            
            df.to_parquet(dirpath / f"{section}.parquet", index=False)
