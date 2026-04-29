"""Tests for EPANET Binary Output File Parser."""

import sys
import os
from pathlib import Path

# Add src to path for testing
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import unittest

from epanet_utils import EpanetOutputDecoder, EpanetOutput


class TestEpanetOutputDecoder(unittest.TestCase):
    """Tests for EpanetOutputDecoder."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.data_dir = Path(__file__).parent.parent / 'data'
        cls.out_file = cls.data_dir / 'Net1.out'

        # Also check results folder
        cls.results_dir = Path(__file__).parent.parent / 'results'

    def get_output_file(self):
        """Get available output file for testing."""
        if self.out_file.exists():
            return self.out_file
        # Try finding an .out file in results folder
        if self.results_dir.exists():
            for result_dir in self.results_dir.iterdir():
                if result_dir.is_dir():
                    out_files = list(result_dir.glob('*.out'))
                    if out_files:
                        return out_files[0]
        return None

    def test_decoder_initialization(self):
        """Test decoder can be initialized."""
        decoder = EpanetOutputDecoder()
        self.assertIsNotNone(decoder)

    def test_decode_file_if_exists(self):
        """Test decoding actual output file if it exists."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        decoder = EpanetOutputDecoder()
        output = decoder.decode_file(out_file)

        # Verify basic structure
        self.assertIn('prolog', output)
        self.assertIn('epilog', output)
        self.assertIn('time_series', output)
        self.assertIn('energy_usage', output)

        prolog = output['prolog']

        # Check prolog fields
        if prolog.get('valid'):
            self.assertIn('num_nodes', prolog)
            self.assertIn('num_links', prolog)
            self.assertIn('num_periods', prolog)

            # Should have positive counts
            self.assertGreater(prolog['num_nodes'], 0)
            self.assertGreater(prolog['num_links'], 0)

    def test_decode_prolog_structure(self):
        """Test prolog structure is correctly parsed."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        decoder = EpanetOutputDecoder()
        output = decoder.decode_file(out_file)
        prolog = output['prolog']

        if prolog.get('valid'):
            # Check all expected prolog fields
            expected_fields = [
                'magic_number', 'version', 'num_nodes', 'num_links',
                'num_pumps', 'num_valves', 'num_reservoirs_tanks',
                'report_time_step', 'simulation_duration', 'num_periods'
            ]

            for field in expected_fields:
                self.assertIn(field, prolog, f"Missing field: {field}")

    def test_decode_time_series(self):
        """Test time series data is correctly parsed."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        decoder = EpanetOutputDecoder()
        output = decoder.decode_file(out_file)

        time_series = output['time_series']
        prolog = output['prolog']

        if prolog.get('valid') and prolog.get('num_periods', 0) > 0:
            # Should have node and link time series
            self.assertIn('nodes', time_series)
            self.assertIn('links', time_series)

            # Should have data for each period
            if time_series['nodes']:
                self.assertGreater(len(time_series['nodes']), 0)

                # Check first period has node data
                first_period = time_series['nodes'][0]
                if first_period:
                    first_node = first_period[0]
                    self.assertIn('demand', first_node)
                    self.assertIn('head', first_node)
                    self.assertIn('pressure', first_node)


class TestEpanetOutput(unittest.TestCase):
    """Tests for EpanetOutput high-level interface."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.data_dir = Path(__file__).parent.parent / 'data'
        cls.out_file = cls.data_dir / 'Net1.out'
        cls.results_dir = Path(__file__).parent.parent / 'results'

    def get_output_file(self):
        """Get available output file for testing."""
        if self.out_file.exists():
            return self.out_file
        # Try finding an .out file in results folder
        if self.results_dir.exists():
            for result_dir in self.results_dir.iterdir():
                if result_dir.is_dir():
                    out_files = list(result_dir.glob('*.out'))
                    if out_files:
                        return out_files[0]
        return None

    def test_output_context_manager(self):
        """Test output as context manager."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        with EpanetOutput(out_file) as output:
            self.assertIsNotNone(output.prolog)

    def test_output_properties(self):
        """Test output properties."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        with EpanetOutput(out_file) as output:
            if output.is_valid():
                self.assertGreater(output.num_nodes, 0)
                self.assertGreater(output.num_links, 0)
                self.assertGreater(output.num_periods, 0)

                # Check IDs are loaded
                self.assertGreater(len(output.node_ids), 0)
                self.assertGreater(len(output.link_ids), 0)

    def test_output_summary(self):
        """Test output summary method."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        with EpanetOutput(out_file) as output:
            summary = output.summary()

            self.assertIn('valid', summary)
            self.assertIn('num_nodes', summary)
            self.assertIn('num_links', summary)
            self.assertIn('num_periods', summary)

    def test_get_node_results(self):
        """Test getting results for specific node."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        with EpanetOutput(out_file) as output:
            if output.is_valid() and output.node_ids:
                node_id = output.node_ids[0]
                result = output.get_node_results(node_id)

                if result:
                    self.assertIn('demand', result)
                    self.assertIn('head', result)
                    self.assertIn('pressure', result)

    def test_get_link_results(self):
        """Test getting results for specific link."""
        out_file = self.get_output_file()
        if out_file is None:
            self.skipTest("No output file available for testing")

        with EpanetOutput(out_file) as output:
            if output.is_valid() and output.link_ids:
                link_id = output.link_ids[0]
                result = output.get_link_results(link_id)

                if result:
                    self.assertIn('flow', result)
                    self.assertIn('velocity', result)


if __name__ == '__main__':
    unittest.main()
