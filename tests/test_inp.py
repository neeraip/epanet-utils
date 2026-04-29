"""Tests for EPANET input file interface."""

import pytest
from pathlib import Path
import tempfile
import os

from epanet_utils import EpanetInput


class TestEpanetInput:
    """Test cases for EpanetInput class."""

    @pytest.fixture
    def sample_inp_path(self):
        """Path to Net1 sample file."""
        base_path = Path(__file__).parent.parent / "EPANET Example Files" / "epanet-desktop" / "Net1"
        return base_path / "Net1.inp"

    @pytest.fixture
    def sample_model(self, sample_inp_path):
        """Load sample model."""
        if sample_inp_path.exists():
            return EpanetInput(sample_inp_path)
        pytest.skip("Sample file not found")

    def test_load_file(self, sample_inp_path):
        """Test loading an input file."""
        if not sample_inp_path.exists():
            pytest.skip("Sample file not found")

        model = EpanetInput(sample_inp_path)
        assert model is not None
        assert len(model.junctions) > 0

    def test_context_manager(self, sample_inp_path):
        """Test context manager usage."""
        if not sample_inp_path.exists():
            pytest.skip("Sample file not found")

        with EpanetInput(sample_inp_path) as model:
            assert len(model.junctions) > 0

    def test_junctions(self, sample_model):
        """Test junction access."""
        assert len(sample_model.junctions) == 9
        junction = sample_model.get_junction("10")
        assert junction is not None
        assert junction["elevation"] == 710

    def test_pipes(self, sample_model):
        """Test pipe access."""
        assert len(sample_model.pipes) == 12
        pipe = sample_model.get_pipe("10")
        assert pipe is not None

    def test_reservoirs(self, sample_model):
        """Test reservoir access."""
        assert len(sample_model.reservoirs) == 1
        reservoir = sample_model.get_reservoir("9")
        assert reservoir is not None
        assert reservoir["head"] == 800

    def test_tanks(self, sample_model):
        """Test tank access."""
        assert len(sample_model.tanks) == 1
        tank = sample_model.get_tank("2")
        assert tank is not None

    def test_pumps(self, sample_model):
        """Test pump access."""
        assert len(sample_model.pumps) == 1
        pump = sample_model.get_pump("9")
        assert pump is not None

    def test_patterns(self, sample_model):
        """Test pattern access."""
        assert len(sample_model.patterns) >= 1
        pattern = sample_model.get_pattern("1")
        assert pattern is not None
        assert len(pattern["multipliers"]) == 12

    def test_curves(self, sample_model):
        """Test curve access."""
        assert len(sample_model.curves) >= 1
        curve = sample_model.get_curve("1")
        assert curve is not None

    def test_summary(self, sample_model):
        """Test summary statistics."""
        summary = sample_model.summary()
        assert summary["junctions"] == 9
        assert summary["reservoirs"] == 1
        assert summary["tanks"] == 1
        assert summary["pipes"] == 12
        assert summary["pumps"] == 1

    def test_save_and_reload(self, sample_model):
        """Test saving and reloading a model."""
        with tempfile.NamedTemporaryFile(suffix=".inp", delete=False) as f:
            temp_path = f.name

        try:
            sample_model.save(temp_path)

            # Reload and verify
            reloaded = EpanetInput(temp_path)
            assert len(reloaded.junctions) == len(sample_model.junctions)
            assert len(reloaded.pipes) == len(sample_model.pipes)
        finally:
            os.unlink(temp_path)

    def test_to_dict(self, sample_model):
        """Test conversion to dictionary."""
        model_dict = sample_model.to_dict()
        assert "junctions" in model_dict
        assert "pipes" in model_dict

    def test_to_dataframe(self, sample_model):
        """Test conversion to DataFrame."""
        df = sample_model.to_dataframe("junctions")
        assert len(df) == 9
        assert "id" in df.columns
        assert "elevation" in df.columns

    def test_add_junction(self):
        """Test adding a junction."""
        model = EpanetInput()
        junction = model.add_junction("NEW1", elevation=100, demand=50)
        assert junction["id"] == "NEW1"
        assert len(model.junctions) == 1

    def test_add_pipe(self):
        """Test adding a pipe."""
        model = EpanetInput()
        model.add_junction("J1", 100)
        model.add_junction("J2", 100)
        pipe = model.add_pipe("P1", "J1", "J2", length=1000, diameter=12, roughness=100)
        assert pipe["id"] == "P1"
        assert len(model.pipes) == 1

    def test_add_pattern(self):
        """Test adding a pattern."""
        model = EpanetInput()
        pattern = model.add_pattern("PAT1", [1.0, 1.2, 0.8, 0.6])
        assert pattern["id"] == "PAT1"
        assert len(pattern["multipliers"]) == 4

    def test_add_curve(self):
        """Test adding a curve."""
        model = EpanetInput()
        curve = model.add_curve("CURVE1", [(0, 100), (500, 80), (1000, 50)])
        assert curve["id"] == "CURVE1"
        assert len(curve["points"]) == 3


class TestEpanetInputEmpty:
    """Test cases for creating new models."""

    def test_create_empty(self):
        """Test creating an empty model."""
        model = EpanetInput()
        assert len(model.junctions) == 0
        assert len(model.pipes) == 0

    def test_set_title(self):
        """Test setting title."""
        model = EpanetInput()
        model.title = "Test Network"
        assert model.title == "Test Network"

    def test_build_simple_network(self):
        """Test building a simple network from scratch."""
        model = EpanetInput()
        model.title = "Simple Network"

        # Add reservoir
        model.add_reservoir("R1", head=100)

        # Add junctions
        model.add_junction("J1", elevation=50, demand=100)
        model.add_junction("J2", elevation=40, demand=150)

        # Add pipes
        model.add_pipe("P1", "R1", "J1", length=1000, diameter=12, roughness=100)
        model.add_pipe("P2", "J1", "J2", length=500, diameter=10, roughness=100)

        # Verify
        assert len(model.reservoirs) == 1
        assert len(model.junctions) == 2
        assert len(model.pipes) == 2

        summary = model.summary()
        assert summary["reservoirs"] == 1
        assert summary["junctions"] == 2
        assert summary["pipes"] == 2
