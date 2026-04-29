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
