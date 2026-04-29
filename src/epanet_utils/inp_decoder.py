"""
EPANET Input File Decoder

Decodes EPANET input files (.inp), JSON, and Parquet formats into Python dictionaries.

Supported input formats:
- .inp (EPANET native format)
- .json (JSON representation)
- .parquet (single-file or multi-file Parquet)

EPANET Input File Sections:
- [TITLE] - Descriptive title for the network
- [JUNCTIONS] - Junction node information
- [RESERVOIRS] - Reservoir node information
- [TANKS] - Storage tank node information
- [PIPES] - Pipe link information
- [PUMPS] - Pump link information
- [VALVES] - Valve link information
- [TAGS] - Optional component tags
- [DEMANDS] - Nodal demand data
- [STATUS] - Initial status of links
- [PATTERNS] - Time patterns
- [CURVES] - Data curves
- [CONTROLS] - Simple control statements
- [RULES] - Rule-based controls
- [ENERGY] - Energy analysis parameters
- [EMITTERS] - Emitter coefficients
- [QUALITY] - Initial water quality
- [SOURCES] - Water quality sources
- [REACTIONS] - Reaction coefficients
- [MIXING] - Tank mixing models
- [TIMES] - Time parameters
- [REPORT] - Report options
- [OPTIONS] - Analysis options
- [COORDINATES] - Node coordinates
- [VERTICES] - Link vertex points
- [LABELS] - Map labels
- [BACKDROP] - Map backdrop settings
"""

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class EpanetInputDecoder:
    """
    Decoder for EPANET input files.
    
    Supports decoding from:
    - .inp files (native EPANET format)
    - .json files (JSON representation)
    - .parquet files (single or multi-file)
    
    Example:
        >>> decoder = EpanetInputDecoder()
        >>> model = decoder.decode_file("network.inp")
        >>> print(model['junctions'])
    """
    
    # All EPANET sections in standard order
    SECTIONS = [
        "TITLE",
        "JUNCTIONS",
        "RESERVOIRS",
        "TANKS",
        "PIPES",
        "PUMPS",
        "VALVES",
        "TAGS",
        "DEMANDS",
        "STATUS",
        "PATTERNS",
        "CURVES",
        "CONTROLS",
        "RULES",
        "ENERGY",
        "EMITTERS",
        "QUALITY",
        "SOURCES",
        "REACTIONS",
        "MIXING",
        "TIMES",
        "REPORT",
        "OPTIONS",
        "COORDINATES",
        "VERTICES",
        "LABELS",
        "BACKDROP",
    ]
    
    # Sections that are stored as text (not tabular)
    TEXT_SECTIONS = {"TITLE", "CONTROLS", "RULES"}
    
    # Sections with key-value pairs
    KEYVALUE_SECTIONS = {"ENERGY", "REACTIONS", "TIMES", "REPORT", "OPTIONS", "BACKDROP"}
    
    # Column definitions for tabular sections
    SECTION_COLUMNS = {
        "JUNCTIONS": ["id", "elevation", "demand", "pattern"],
        "RESERVOIRS": ["id", "head", "pattern"],
        "TANKS": ["id", "elevation", "init_level", "min_level", "max_level", "diameter", "min_vol", "vol_curve", "overflow"],
        "PIPES": ["id", "node1", "node2", "length", "diameter", "roughness", "minor_loss", "status"],
        "PUMPS": ["id", "node1", "node2", "parameters"],
        "VALVES": ["id", "node1", "node2", "diameter", "type", "setting", "minor_loss"],
        "TAGS": ["object_type", "object_id", "tag"],
        "DEMANDS": ["junction", "demand", "pattern", "category"],
        "STATUS": ["id", "status"],
        "PATTERNS": ["id", "multipliers"],
        "CURVES": ["id", "x_value", "y_value"],
        "EMITTERS": ["junction", "coefficient"],
        "QUALITY": ["node", "init_qual"],
        "SOURCES": ["node", "type", "quality", "pattern"],
        "MIXING": ["tank", "model", "fraction"],
        "COORDINATES": ["node", "x_coord", "y_coord"],
        "VERTICES": ["link", "x_coord", "y_coord"],
        "LABELS": ["x_coord", "y_coord", "label", "anchor"],
    }
    
    def __init__(self):
        """Initialize the decoder."""
        pass
    
    def decode_file(self, filepath: str) -> Dict[str, Any]:
        """
        Decode an EPANET input file.
        
        Automatically detects format based on file extension.
        
        Args:
            filepath: Path to input file (.inp, .json, or .parquet)
            
        Returns:
            Dictionary containing parsed model data
            
        Raises:
            ValueError: If file format is not supported
            FileNotFoundError: If file does not exist
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            # Check if it's a parquet directory
            if not filepath.with_suffix('').exists():
                raise FileNotFoundError(f"File not found: {filepath}")
        
        ext = filepath.suffix.lower()
        
        if ext == ".inp":
            return self.decode_inp(filepath)
        elif ext == ".json":
            return self.decode_json(filepath)
        elif ext == ".parquet":
            return self.decode_parquet(filepath)
        else:
            # Try as parquet directory
            if filepath.is_dir() or (filepath.with_suffix('').exists() and filepath.with_suffix('').is_dir()):
                return self.decode_parquet(filepath)
            raise ValueError(f"Unsupported file format: {ext}")
    
    def decode_inp(self, filepath: Union[str, Path]) -> Dict[str, Any]:
        """
        Decode an EPANET .inp file.
        
        Args:
            filepath: Path to .inp file
            
        Returns:
            Dictionary containing parsed model data
        """
        filepath = Path(filepath)
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        return self.decode_inp_string(content)
    
    def decode_inp_string(self, content: str) -> Dict[str, Any]:
        """
        Decode EPANET input file content from a string.

        Args:
            content: String content of .inp file

        Returns:
            Dictionary containing parsed model data
        """
        model = {
            "metadata": {
                "format": "epanet_inp",
                "engine": "epanet",
                "version": "2.2",
            }
        }

        # Split content into sections
        sections = self._split_sections(content)

        # Parse each section
        for section_name, section_content in sections.items():
            section_upper = section_name.upper()

            # [END] is just the trailing marker — no payload to capture.
            if section_upper == "END":
                continue

            if section_upper in self.TEXT_SECTIONS:
                model[section_name.lower()] = self._parse_text_section(section_content)
            elif section_upper in self.KEYVALUE_SECTIONS:
                model[section_name.lower()] = self._parse_keyvalue_section(section_content, section_upper)
            elif section_upper in self.SECTION_COLUMNS:
                model[section_name.lower()] = self._parse_table_section(
                    section_content,
                    self.SECTION_COLUMNS[section_upper],
                    section_upper
                )
            else:
                # Unknown section — store under metadata.unknown_sections so
                # it round-trips even though we don't understand its shape.
                # (Matters for forward-compat when EPANET adds new sections.)
                model.setdefault("metadata", {}).setdefault(
                    "unknown_sections", {}
                )[section_name] = section_content

        return model
    
    def _split_sections(self, content: str) -> Dict[str, str]:
        """Split input file content into sections."""
        sections = {}
        current_section = None
        current_content = []
        
        for line in content.split('\n'):
            # Check for section header
            match = re.match(r'\s*\[(\w+)\]\s*', line)
            if match:
                # Save previous section
                if current_section:
                    sections[current_section] = '\n'.join(current_content)
                current_section = match.group(1)
                current_content = []
            elif current_section:
                current_content.append(line)
        
        # Save last section
        if current_section:
            sections[current_section] = '\n'.join(current_content)
        
        return sections
    
    def _parse_text_section(self, content: str) -> str:
        """Parse a text section (TITLE, CONTROLS, RULES)."""
        lines = []
        for line in content.split('\n'):
            # Keep non-empty lines, strip trailing whitespace
            stripped = line.rstrip()
            if stripped:
                lines.append(stripped)
        return '\n'.join(lines)
    
    def _parse_keyvalue_section(self, content: str, section_name: str) -> Dict[str, Any]:
        """Parse a key-value section (OPTIONS, TIMES, ENERGY, REACTIONS, ...).

        Most EPANET keyvalue sections use a one-token key + one value.
        ENERGY and REACTIONS, however, use two-token keys:

          [ENERGY]      GLOBAL PRICE 0.0     |  PUMP EFFIC <id> <curve>
          [REACTIONS]   ORDER BULK 1         |  GLOBAL WALL 0.0
                        ROUGHNESS CORRELATION 0.0

        Naively splitting on the first whitespace collapses every
        ``ORDER X 1`` line into a single ``order`` key, losing
        BULK/WALL/TANK distinctions. We special-case those sections so
        the second token is folded into the key (``order_wall``,
        ``global_price``, …), giving the editor stable, lookupable keys.

        Per-id pump rows in ENERGY (``PUMP <id> EFFIC <curve>``) and
        per-id reactions in REACTIONS (``BULK <pipe-id> 0.5``) are
        accumulated under a generic ``per_id`` list so they're not lost.
        """
        # Words that are themselves two-token *modifiers* — when we see
        # them as the first token, the *next* token is a sub-keyword.
        # OPTIONS in EPANET 2.2 has several two-token keys: DEMAND
        # MULTIPLIER, DEMAND MODEL, EMITTER EXPONENT, SPECIFIC GRAVITY,
        # MINIMUM/REQUIRED/PRESSURE EXPONENT (PDA), plus QUALITY TRACE
        # <node>. Without the merge here, "DEMAND MULTIPLIER 1.0" gets
        # split into key=demand value="Multiplier 1.0", which lands in
        # data.options as a broken pair that confuses the editor form.
        TWO_TOKEN_PREFIXES = {
            "ENERGY": {"global", "demand", "pump"},
            "REACTIONS": {"order", "global", "bulk", "wall", "tank",
                          "limiting", "roughness"},
            "OPTIONS": {"demand", "emitter", "specific",
                        "minimum", "required", "pressure"},
            # Almost every TIMES key is two-token: HYDRAULIC TIMESTEP,
            # QUALITY TIMESTEP, RULE TIMESTEP, PATTERN TIMESTEP/START,
            # REPORT TIMESTEP/START, START CLOCKTIME. Without merging
            # we'd drop the second token onto the value side and break
            # both the editor schema and round-trip.
            "TIMES": {"hydraulic", "quality", "rule", "pattern",
                      "report", "start"},
        }

        result: Dict[str, Any] = {}

        for line in content.split('\n'):
            line = line.strip()

            # Skip empty lines and full-line comments
            if not line or line.startswith(';'):
                continue

            # Strip inline comments
            if ';' in line:
                line = line[:line.index(';')].strip()

            parts = line.split()
            if not parts:
                continue

            head = parts[0].lower()
            two_token_set = TWO_TOKEN_PREFIXES.get(section_name, set())

            # Special-case: PUMP <id> EFFIC <curve> / PUMP <id> PRICE <v>
            # / PUMP <id> PATTERN <pat>. The pump id varies per row, so
            # we collect them as a list of {id, param, value}.
            if section_name == "ENERGY" and head == "pump" and len(parts) >= 4:
                result.setdefault("pump_settings", []).append({
                    "id": parts[1],
                    "param": parts[2].lower(),
                    "value": self._convert_value(" ".join(parts[3:])),
                })
                continue

            # Special-case: REACTIONS per-pipe/tank — BULK <id> <coef> /
            # WALL <id> <coef> / TANK <id> <coef> when the second token
            # looks like an id rather than a known sub-keyword.
            REACTIONS_GLOBAL_SUB = {"bulk", "wall", "tank"}
            if (
                section_name == "REACTIONS"
                and head in REACTIONS_GLOBAL_SUB
                and len(parts) >= 3
                and parts[1].lower() not in {"bulk", "wall", "tank"}
            ):
                # Heuristic: numeric second token = per-id row
                # (`Bulk Pipe1 0.5`); known sub-keyword = global row
                # already handled below.
                result.setdefault("per_id", []).append({
                    "type": head,
                    "id": parts[1],
                    "value": self._convert_value(parts[2]),
                })
                continue

            # Special-case: OPTIONS QUALITY — multi-token directive.
            # Format:
            #   QUALITY NONE
            #   QUALITY AGE
            #   QUALITY CHEMICAL <name> [mass-units]
            #   QUALITY TRACE <node-id>
            # EPANET also writes a stray ``QUALITY None mg/L`` (default
            # mass units even when type is None). Split the value into
            # the canonical schema keys so the editor's QUALITY_FIELDS
            # form picks them up.
            if section_name == "OPTIONS" and head == "quality" and len(parts) >= 2:
                qtype = parts[1]
                qtype_upper = qtype.upper()
                result["quality"] = qtype
                if qtype_upper == "CHEMICAL":
                    if len(parts) >= 3:
                        result["quality_chemical_name"] = parts[2]
                    if len(parts) >= 4:
                        result["mass_units"] = " ".join(parts[3:])
                elif qtype_upper == "TRACE":
                    if len(parts) >= 3:
                        result["quality_trace_node"] = parts[2]
                else:
                    # NONE / AGE: any trailing tokens are mass-units
                    # the EPANET writer leaves behind (harmless, but
                    # we keep them so round-trip stays exact).
                    if len(parts) >= 3:
                        result["mass_units"] = " ".join(parts[2:])
                continue

            # Two-token key handling (`order_wall`, `global_price`, …).
            if head in two_token_set and len(parts) >= 3:
                key = f"{head}_{parts[1].lower()}"
                value = self._convert_value(" ".join(parts[2:]))
            else:
                key = head.replace(' ', '_').replace('-', '_')
                value = self._convert_value(parts[1]) if len(parts) > 1 else ""
                if len(parts) > 2:
                    # Preserve any trailing tokens as part of the value
                    # (e.g. OPTIONS "MAP somefile.map" with whitespace).
                    value = self._convert_value(" ".join(parts[1:]))

            result[key] = value

        return result
    
    def _parse_table_section(self, content: str, columns: List[str], section_name: str) -> Any:
        """Parse a tabular section.

        For most sections this returns ``list[dict]``. PATTERNS and
        CURVES are aggregated into ``dict[id -> ...]`` so the editor
        can look them up by name (matches swmm-utils convention; also
        avoids reconciling a list-of-dicts with a name-keyed editor).

        CURVES additionally captures the type from the preceding
        ``;PUMP:`` / ``;EFFICIENCY:`` / ``;VOLUME:`` / ``;HEADLOSS:``
        comment line — EPANET stores the type only in those comments,
        and losing them on import would force the user to re-classify
        every curve.
        """
        result: List[Dict[str, Any]] = []
        # Track the most recently seen curve type comment so it can
        # attach to the next non-comment row in [CURVES]. ``pending_*``
        # lifecycle: set when a ``;PUMP:`` / ``;EFFICIENCY:`` etc. line
        # is parsed, attached to the next data row, then cleared.
        pending_curve_type: Optional[str] = None
        pending_curve_desc: Optional[str] = None
        # Maps of curve_id → type / description (filled while iterating).
        curve_types: Dict[str, str] = {}
        curve_descs: Dict[str, str] = {}
        # Standalone ``;<text>`` lines preceding a data row become that
        # row's ``description``. Multiple consecutive ``;`` lines are
        # joined with newlines. A blank line clears any pending text.
        pending_desc: List[str] = []
        # Parallel descriptions list for post-aggregation attach on
        # PATTERNS / CURVES (where the result shape is a dict, not a
        # list) — there we drop them since dict-keyed sections don't
        # have per-row semantics matching positional descriptions.
        descriptions: List[str] = []

        for line in content.split('\n'):
            stripped = line.strip()

            if not stripped:
                pending_desc = []
                continue

            # Curve type + description live in comments. EPANET writes
            # rows like ``;PUMP: Pump curve for Pump VANTAGE_P1`` — the
            # token before the ``:`` is the curve type, anything after
            # is a free-form description for that curve. Sniff before
            # the generic-comment-strip pass so the description doesn't
            # get attached to a downstream data row instead.
            if section_name == "CURVES" and stripped.startswith(';'):
                m = re.match(r';\s*(PUMP|EFFICIENCY|VOLUME|HEADLOSS)\s*:\s*(.*)',
                             stripped, re.IGNORECASE)
                if m:
                    pending_curve_type = m.group(1).upper()
                    pending_curve_desc = (m.group(2) or "").strip()
                    continue

            # Standalone ``;<text>`` description line (excluding the
            # double-semicolon ``;;`` column-header divider rows).
            #
            # Some legacy EPANET files use a single ``;`` for column
            # headers (e.g. ``;ID\tElevation\tInitLevel``) instead of
            # the modern ``;;`` convention. Without a guard here that
            # header line gets stored as the first row's description
            # ("ID\tElevation\t..." appears as the description for the
            # first tank). Sniff: a ``;`` line with ≥3 TABs whose
            # tab-separated tokens are short, single-word identifiers
            # is almost certainly a column header — skip it.
            if stripped.startswith(';'):
                if stripped.startswith(';;'):
                    continue
                body = stripped[1:].strip()
                tabs = body.count('\t')
                if tabs >= 3:
                    tokens = [t.strip() for t in body.split('\t') if t.strip()]
                    if tokens and all(
                        ' ' not in t and len(t) <= 24 and t.replace('_', '').replace('-', '').isalnum()
                        for t in tokens
                    ):
                        # Column-header masquerading as a description — drop.
                        continue
                pending_desc.append(body)
                continue

            line = stripped
            inline_desc = ""

            # Remove trailing semicolon and capture inline comment as
            # the row's inline description.
            if line.endswith(';'):
                line = line[:-1].strip()
            if ';' in line:
                # Be careful with labels that might have semicolons in quoted strings
                if '"' in line:
                    in_quote = False
                    for i, char in enumerate(line):
                        if char == '"':
                            in_quote = not in_quote
                        elif char == ';' and not in_quote:
                            inline_desc = line[i + 1:].strip()
                            line = line[:i].strip()
                            break
                else:
                    idx = line.index(';')
                    inline_desc = line[idx + 1:].strip()
                    line = line[:idx].strip()

            # Preceding ``;`` lines win over inline comments (the more
            # deliberate of the two when both are present).
            row_desc = "\n".join(pending_desc) if pending_desc else inline_desc
            pending_desc = []

            # Parse based on section type
            if section_name == "PATTERNS":
                row = self._parse_pattern_line(line, columns)
            elif section_name == "CURVES":
                row = self._parse_curve_line(line, columns)
                if row:
                    if pending_curve_type and row["id"] not in curve_types:
                        curve_types[row["id"]] = pending_curve_type
                    if pending_curve_desc and row["id"] not in curve_descs:
                        curve_descs[row["id"]] = pending_curve_desc
            elif section_name == "LABELS":
                row = self._parse_label_line(line, columns)
            elif section_name == "PUMPS":
                row = self._parse_pump_line(line, columns)
            else:
                row = self._parse_table_line(line, columns)

            if row:
                if row_desc and isinstance(row, dict):
                    row["description"] = row_desc
                result.append(row)
                descriptions.append(row_desc)

        # Aggregate patterns and curves into dict[id -> ...] shape.
        if section_name == "PATTERNS":
            return self._aggregate_patterns(result)
        if section_name == "CURVES":
            return self._aggregate_curves(result, curve_types, curve_descs)

        return result
    
    def _parse_table_line(self, line: str, columns: List[str]) -> Optional[Dict[str, Any]]:
        """Parse a single line of a table section."""
        parts = line.split()
        if not parts:
            return None
        
        row = {}
        for i, col in enumerate(columns):
            if i < len(parts):
                row[col] = self._convert_value(parts[i])
            else:
                row[col] = None
        
        return row
    
    def _parse_pattern_line(self, line: str, columns: List[str]) -> Optional[Dict[str, Any]]:
        """Parse a PATTERNS section line."""
        parts = line.split()
        if not parts:
            return None
        
        pattern_id = parts[0]
        multipliers = [self._convert_value(p) for p in parts[1:]]
        
        return {"id": pattern_id, "multipliers": multipliers}
    
    def _parse_curve_line(self, line: str, columns: List[str]) -> Optional[Dict[str, Any]]:
        """Parse a CURVES section line."""
        parts = line.split()
        if len(parts) < 3:
            return None
        
        return {
            "id": parts[0],
            "x_value": self._convert_value(parts[1]),
            "y_value": self._convert_value(parts[2])
        }
    
    def _parse_label_line(self, line: str, columns: List[str]) -> Optional[Dict[str, Any]]:
        """Parse a LABELS section line."""
        # Labels can have quoted strings
        parts = []
        current = ""
        in_quote = False
        
        for char in line:
            if char == '"':
                in_quote = not in_quote
            elif char.isspace() and not in_quote:
                if current:
                    parts.append(current)
                    current = ""
            else:
                current += char
        
        if current:
            parts.append(current)
        
        if len(parts) < 3:
            return None
        
        row = {
            "x_coord": self._convert_value(parts[0]),
            "y_coord": self._convert_value(parts[1]),
            "label": parts[2].strip('"'),
            "anchor": parts[3] if len(parts) > 3 else None
        }
        
        return row
    
    def _parse_pump_line(self, line: str, columns: List[str]) -> Optional[Dict[str, Any]]:
        """Parse a PUMPS section line."""
        parts = line.split()
        if len(parts) < 4:
            return None
        
        return {
            "id": parts[0],
            "node1": parts[1],
            "node2": parts[2],
            "parameters": ' '.join(parts[3:])
        }
    
    def _aggregate_patterns(self, rows: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """Aggregate pattern rows into ``dict[id -> multipliers]``.

        Multiple rows with the same id are concatenated (EPANET wraps
        long patterns across many lines). The dict shape mirrors
        swmm-utils' ``patterns: dict[name -> tokens]`` so a single
        editor can render either engine's data.
        """
        patterns: Dict[str, List[Any]] = {}
        for row in rows:
            pid = row["id"]
            patterns.setdefault(pid, []).extend(row["multipliers"])
        return patterns

    def _aggregate_curves(
        self,
        rows: List[Dict[str, Any]],
        curve_types: Optional[Dict[str, str]] = None,
        curve_descs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Aggregate curve rows into ``dict[id -> {type, points, description?}]``.

        ``type`` comes from the ``;PUMP:`` / ``;EFFICIENCY:`` etc.
        comment that precedes the data rows in standard EPANET output.
        Curves without a type comment get type ``""`` so the editor can
        prompt the user to classify them rather than guessing.

        ``description`` is the free-form text after the type marker
        on the same line (e.g. ``;PUMP: Pump curve for Pump VANTAGE_P1``
        → ``"Pump curve for Pump VANTAGE_P1"``). Omitted when the
        comment carries no description.
        """
        curve_types = curve_types or {}
        curve_descs = curve_descs or {}
        curves: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            cid = row["id"]
            entry = curves.setdefault(
                cid, {"type": curve_types.get(cid, ""), "points": []}
            )
            entry["points"].append({"x": row["x_value"], "y": row["y_value"]})
            if cid in curve_descs and "description" not in entry:
                entry["description"] = curve_descs[cid]
        return curves
    
    def _convert_value(self, value: str) -> Any:
        """Convert a string value to appropriate type."""
        if not value:
            return None
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value
    
    def decode_json(self, filepath: Union[str, Path]) -> Dict[str, Any]:
        """
        Decode a JSON file.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            Dictionary containing model data
        """
        filepath = Path(filepath)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def decode_parquet(self, filepath: Union[str, Path]) -> Dict[str, Any]:
        """
        Decode a Parquet file or directory.
        
        Args:
            filepath: Path to .parquet file or directory containing .parquet files
            
        Returns:
            Dictionary containing model data
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas and pyarrow are required for Parquet support")
        
        filepath = Path(filepath)
        
        # Check if it's a single file or directory
        if filepath.is_file():
            return self._decode_single_parquet(filepath)
        elif filepath.is_dir():
            return self._decode_multi_parquet(filepath)
        else:
            # Try without extension
            dir_path = filepath.with_suffix('')
            if dir_path.is_dir():
                return self._decode_multi_parquet(dir_path)
            raise FileNotFoundError(f"Parquet file or directory not found: {filepath}")
    
    def _decode_single_parquet(self, filepath: Path) -> Dict[str, Any]:
        """Decode a single Parquet file with multiple tables."""
        import pyarrow.parquet as pq
        
        model = {"metadata": {"format": "epanet_parquet", "version": "2.2"}}
        
        # Read the parquet file
        table = pq.read_table(filepath)
        df = table.to_pandas()
        
        # The single-file format stores section name in a column
        if 'section' in df.columns:
            for section_name in df['section'].unique():
                section_df = df[df['section'] == section_name].drop(columns=['section'])
                section_data = section_df.to_dict('records')
                model[section_name.lower()] = section_data
        else:
            # Assume it's metadata
            model.update(df.to_dict('records')[0] if len(df) > 0 else {})
        
        return model
    
    def _decode_multi_parquet(self, dirpath: Path) -> Dict[str, Any]:
        """Decode a directory of Parquet files."""
        model = {"metadata": {"format": "epanet_parquet", "version": "2.2"}}
        
        for filepath in dirpath.glob("*.parquet"):
            section_name = filepath.stem.lower()
            
            if section_name == "metadata":
                df = pd.read_parquet(filepath)
                if len(df) > 0:
                    model["metadata"].update(df.to_dict('records')[0])
            else:
                df = pd.read_parquet(filepath)
                
                # Handle text sections
                if section_name in [s.lower() for s in self.TEXT_SECTIONS]:
                    if 'content' in df.columns and len(df) > 0:
                        model[section_name] = df['content'].iloc[0]
                    else:
                        model[section_name] = ""
                # Handle key-value sections
                elif section_name in [s.lower() for s in self.KEYVALUE_SECTIONS]:
                    if len(df) > 0:
                        model[section_name] = df.to_dict('records')[0]
                    else:
                        model[section_name] = {}
                else:
                    model[section_name] = df.to_dict('records')
        
        return model
