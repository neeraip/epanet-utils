"""Tests for EPANET input decoder and encoder."""

import pytest
from pathlib import Path
import tempfile
import os
import json

from epanet_utils import EpanetInputDecoder, EpanetInputEncoder


class TestEpanetInputDecoder:
    """Test cases for EpanetInputDecoder class."""

    @pytest.fixture
    def decoder(self):
        """Create decoder instance."""
        return EpanetInputDecoder()

    @pytest.fixture
    def sample_inp_path(self):
        """Path to Net1 sample file."""
        base_path = Path(__file__).parent.parent / "EPANET Example Files" / "epanet-desktop" / "Net1"
        return base_path / "Net1.inp"

    @pytest.fixture
    def sample_model(self, decoder, sample_inp_path):
        """Load sample model."""
        if sample_inp_path.exists():
            return decoder.decode_file(sample_inp_path)
        pytest.skip("Sample file not found")

    def test_decode_inp_file(self, decoder, sample_inp_path):
        """Test decoding .inp file."""
        if not sample_inp_path.exists():
            pytest.skip("Sample file not found")

        model = decoder.decode_file(sample_inp_path)
        assert "junctions" in model
        assert "pipes" in model
        assert "metadata" in model

    def test_decode_junctions(self, sample_model):
        """Test junction parsing."""
        junctions = sample_model.get("junctions", [])
        assert len(junctions) == 9

        # Find junction 10
        j10 = next((j for j in junctions if j["id"] == 10), None)
        assert j10 is not None
        assert j10["elevation"] == 710

    def test_decode_reservoirs(self, sample_model):
        """Test reservoir parsing."""
        reservoirs = sample_model.get("reservoirs", [])
        assert len(reservoirs) == 1
        assert reservoirs[0]["id"] == 9
        assert reservoirs[0]["head"] == 800

    def test_decode_tanks(self, sample_model):
        """Test tank parsing."""
        tanks = sample_model.get("tanks", [])
        assert len(tanks) == 1
        assert tanks[0]["id"] == 2

    def test_decode_pipes(self, sample_model):
        """Test pipe parsing."""
        pipes = sample_model.get("pipes", [])
        assert len(pipes) == 12

    def test_decode_pumps(self, sample_model):
        """Test pump parsing."""
        pumps = sample_model.get("pumps", [])
        assert len(pumps) == 1
        assert pumps[0]["id"] == 9

    def test_decode_patterns(self, sample_model):
        """Test pattern parsing."""
        patterns = sample_model.get("patterns", [])
        assert len(patterns) >= 1

        # Pattern 1 should have 12 multipliers
        p1 = next((p for p in patterns if p["id"] == "1"), None)
        assert p1 is not None
        assert len(p1["multipliers"]) == 12

    def test_decode_curves(self, sample_model):
        """Test curve parsing."""
        curves = sample_model.get("curves", [])
        assert len(curves) >= 1

    def test_decode_options(self, sample_model):
        """Test options parsing."""
        options = sample_model.get("options", {})
        assert "units" in options
        assert options["units"] == "GPM"

    def test_decode_times(self, sample_model):
        """Test time settings parsing."""
        times = sample_model.get("times", {})
        assert "duration" in times

    def test_decode_coordinates(self, sample_model):
        """Test coordinates parsing."""
        coords = sample_model.get("coordinates", [])
        assert len(coords) > 0

    def test_decode_string(self, decoder):
        """Test decoding from string."""
        inp_content = """[TITLE]
Test Network

[JUNCTIONS]
;ID   Elev   Demand   Pattern
J1    100    50
J2    90     100

[RESERVOIRS]
;ID   Head   Pattern
R1    150

[PIPES]
;ID   Node1   Node2   Length   Diameter   Roughness   MinorLoss   Status
P1    R1      J1      1000     12         100         0           Open
P2    J1      J2      500      10         100         0           Open

[END]
"""
        model = decoder.decode_inp_string(inp_content)
        assert len(model["junctions"]) == 2
        assert len(model["reservoirs"]) == 1
        assert len(model["pipes"]) == 2


class TestEpanetInputEncoder:
    """Test cases for EpanetInputEncoder class."""

    @pytest.fixture
    def encoder(self):
        """Create encoder instance."""
        return EpanetInputEncoder()

    @pytest.fixture
    def decoder(self):
        """Create decoder instance."""
        return EpanetInputDecoder()

    @pytest.fixture
    def sample_model(self, decoder):
        """Create sample model."""
        inp_content = """[TITLE]
Test Network

[JUNCTIONS]
;ID   Elev   Demand   Pattern
J1    100    50
J2    90     100

[RESERVOIRS]
;ID   Head   Pattern
R1    150

[PIPES]
;ID   Node1   Node2   Length   Diameter   Roughness   MinorLoss   Status
P1    R1      J1      1000     12         100         0           Open
P2    J1      J2      500      10         100         0           Open

[PATTERNS]
;ID   Multipliers
1     1.0  1.2  1.4  1.2  1.0  0.8

[CURVES]
;ID   X-Value   Y-Value
1     0         100
1     500       80
1     1000      50

[OPTIONS]
Units   GPM
Headloss   H-W

[TIMES]
Duration   24:00
Hydraulic Timestep   1:00

[END]
"""
        return decoder.decode_inp_string(inp_content)

    def test_encode_to_string(self, encoder, sample_model):
        """Test encoding to string."""
        output = encoder.encode_to_inp_string(sample_model)
        assert "[JUNCTIONS]" in output
        assert "[PIPES]" in output
        assert "[END]" in output

    def test_encode_to_file(self, encoder, sample_model):
        """Test encoding to file."""
        with tempfile.NamedTemporaryFile(suffix=".inp", delete=False) as f:
            temp_path = f.name

        try:
            encoder.encode_to_inp_file(sample_model, temp_path)
            assert os.path.exists(temp_path)

            with open(temp_path, 'r') as f:
                content = f.read()

            assert "[JUNCTIONS]" in content
        finally:
            os.unlink(temp_path)

    def test_encode_to_json(self, encoder, sample_model):
        """Test encoding to JSON."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            encoder.encode_to_json(sample_model, temp_path, pretty=True)
            assert os.path.exists(temp_path)

            with open(temp_path, 'r') as f:
                data = json.load(f)

            assert "junctions" in data
            assert "pipes" in data
        finally:
            os.unlink(temp_path)

    def test_encode_to_parquet_single(self, encoder, sample_model):
        """Test encoding to single Parquet file."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name

        try:
            encoder.encode_to_parquet(sample_model, temp_path, single_file=True)
            assert os.path.exists(temp_path)
        finally:
            os.unlink(temp_path)

    def test_encode_to_parquet_multi(self, encoder, sample_model):
        """Test encoding to multiple Parquet files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            encoder.encode_to_parquet(sample_model, temp_dir, single_file=False)

            # Should have created multiple files
            parquet_files = list(Path(temp_dir).glob("*.parquet"))
            assert len(parquet_files) > 0

    # ------------------------------------------------------------------
    # OPTIONS QUALITY directive
    # ------------------------------------------------------------------
    # Regression suite for the bug where the encoder emitted
    # ``Mass Units mg/L`` and ``Quality Chemical Name <name>`` as
    # standalone [OPTIONS] lines. EPANET 2.3.3 rejects those with
    # Error 202 ("illegal numeric value"); the only legal shape is the
    # multi-token ``QUALITY <type> [<name>] [<units>]`` directive.

    def test_quality_none_omits_mass_units_line(self, encoder):
        """``quality=None`` + stray ``mass_units`` round-trips on the
        QUALITY line, never as a standalone ``Mass Units`` line."""
        out = encoder.encode_to_inp_string({
            "options": {
                "units": "GPM",
                "quality": "None",
                "mass_units": "mg/L",
            }
        })
        assert "Mass Units" not in out, "encoder leaked Mass Units as a standalone OPTIONS key"
        # Round-trip preserves the stray suffix on the QUALITY line.
        assert "Quality\tNone\tmg/L" in out

    def test_quality_none_without_mass_units(self, encoder):
        """Cleanest case: no mass_units → bare ``Quality\tNone``."""
        out = encoder.encode_to_inp_string({
            "options": {"units": "GPM", "quality": "None"}
        })
        # Use splitlines to avoid the leading-whitespace alignment varying.
        quality_lines = [ln.strip() for ln in out.splitlines() if ln.strip().startswith("Quality")]
        assert quality_lines == ["Quality\tNone"]

    def test_quality_chemical_with_name_and_units(self, encoder):
        """Chemical: name + mass-units fold onto the QUALITY line."""
        out = encoder.encode_to_inp_string({
            "options": {
                "quality": "Chemical",
                "quality_chemical_name": "Free Chlorine",
                "mass_units": "mg/L",
            }
        })
        assert "Quality\tChemical\tFree Chlorine\tmg/L" in out
        assert "Mass Units" not in out
        assert "Quality Chemical Name" not in out

    def test_quality_chemical_default_name_when_missing(self, encoder):
        """Chemical without an explicit name falls back to ``Chemical``
        — EPANET requires the name token, so emitting only the type
        would be invalid."""
        out = encoder.encode_to_inp_string({
            "options": {"quality": "Chemical", "mass_units": "ug/L"},
        })
        assert "Quality\tChemical\tChemical\tug/L" in out

    def test_quality_trace_emits_node(self, encoder):
        """Trace: node id is the third token; mass_units is omitted
        even if the dict carries one."""
        out = encoder.encode_to_inp_string({
            "options": {
                "quality": "Trace",
                "quality_trace_node": "N5",
                "mass_units": "mg/L",
            }
        })
        assert "Quality\tTrace\tN5" in out
        assert "Mass Units" not in out

    def test_quality_age_no_mass_units(self, encoder):
        """Age has no extra tokens unless the decoder captured a
        stray ``mass_units`` suffix to round-trip."""
        out = encoder.encode_to_inp_string({
            "options": {"quality": "Age"}
        })
        quality_lines = [ln.strip() for ln in out.splitlines() if ln.strip().startswith("Quality")]
        assert quality_lines == ["Quality\tAge"]

    def test_quality_options_roundtrip_chemical(self, encoder, decoder):
        """End-to-end: decode a Chemical QUALITY directive, re-encode
        it, and decode again — the dict shape is stable across one
        full cycle."""
        original = """[TITLE]
Test
[OPTIONS]
 Units\tGPM
 Quality\tChemical\tChlorine\tmg/L
[END]
"""
        m1 = decoder.decode_inp_string(original)
        assert m1["options"]["quality"] == "Chemical"
        assert m1["options"]["quality_chemical_name"] == "Chlorine"
        assert m1["options"]["mass_units"] == "mg/L"

        encoded = encoder.encode_to_inp_string(m1)
        m2 = decoder.decode_inp_string(encoded)
        assert m2["options"]["quality"] == "Chemical"
        assert m2["options"]["quality_chemical_name"] == "Chlorine"
        assert m2["options"]["mass_units"] == "mg/L"


class TestRoundTrip:
    """Test round-trip encoding/decoding."""

    @pytest.fixture
    def decoder(self):
        return EpanetInputDecoder()

    @pytest.fixture
    def encoder(self):
        return EpanetInputEncoder()

    @pytest.fixture
    def sample_inp_path(self):
        base_path = Path(__file__).parent.parent / "EPANET Example Files" / "epanet-desktop" / "Net1"
        return base_path / "Net1.inp"

    def test_inp_roundtrip(self, decoder, encoder, sample_inp_path):
        """Test .inp → dict → .inp roundtrip."""
        if not sample_inp_path.exists():
            pytest.skip("Sample file not found")

        # Decode original
        original = decoder.decode_file(sample_inp_path)

        # Encode to string
        encoded = encoder.encode_to_inp_string(original)

        # Decode again
        reloaded = decoder.decode_inp_string(encoded)

        # Compare
        assert len(original["junctions"]) == len(reloaded["junctions"])
        assert len(original["pipes"]) == len(reloaded["pipes"])

    def test_json_roundtrip(self, decoder, encoder, sample_inp_path):
        """Test .inp → JSON → dict roundtrip."""
        if not sample_inp_path.exists():
            pytest.skip("Sample file not found")

        original = decoder.decode_file(sample_inp_path)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            encoder.encode_to_json(original, temp_path)
            reloaded = decoder.decode_json(temp_path)

            assert len(original["junctions"]) == len(reloaded["junctions"])
            assert len(original["pipes"]) == len(reloaded["pipes"])
        finally:
            os.unlink(temp_path)

    def test_parquet_roundtrip(self, decoder, encoder, sample_inp_path):
        """Test .inp → Parquet → dict roundtrip."""
        if not sample_inp_path.exists():
            pytest.skip("Sample file not found")

        original = decoder.decode_file(sample_inp_path)

        with tempfile.TemporaryDirectory() as temp_dir:
            encoder.encode_to_parquet(original, temp_dir, single_file=False)
            reloaded = decoder.decode_parquet(temp_dir)

            assert len(original["junctions"]) == len(reloaded["junctions"])
            assert len(original["pipes"]) == len(reloaded["pipes"])
