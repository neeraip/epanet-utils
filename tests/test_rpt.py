"""Tests for EPANET Report File Parser."""

import sys
import os
from pathlib import Path

# Add src to path for testing
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import unittest

from epanet_utils import EpanetReportDecoder, EpanetReport


class TestEpanetReportDecoder(unittest.TestCase):
    """Tests for EpanetReportDecoder."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.data_dir = Path(__file__).parent.parent / 'data'
        cls.rpt_file = cls.data_dir / 'Net1.rpt'

    def test_decoder_initialization(self):
        """Test decoder can be initialized."""
        decoder = EpanetReportDecoder()
        self.assertIsNotNone(decoder)

    def test_decode_string(self):
        """Test decoding report content from string."""
        content = """
  ******************************************************************
  *                           E P A N E T                          *
  *                   Hydraulic and Water Quality                  *
  *                   Analysis for Pipe Networks                   *
  *                          Version 2.3.03                        *
  ******************************************************************

  Analysis begun Thu Feb  5 16:08:17 2026

  Hydraulic Status:
  -----------------------------------------------------------------------
     0:00:00: Balanced after 4 trials
     1:00:00: Balanced after 2 trials

  Hydraulic Flow Balance (gpm)
  ================================
  Total Inflow:          1052.159
  Consumer Demand:       1100.000
  Total Outflow:         1100.000
  ================================

  Water Quality Mass Balance (mg)
  ================================
  Initial Mass:       7.47582e+06
  Mass Ratio:         1.00000
  ================================

  Analysis ended Thu Feb  5 16:08:17 2026
"""
        decoder = EpanetReportDecoder()
        report = decoder.decode_string(content)

        # Check version
        self.assertEqual(report['version'], '2.3.03')

        # Check timestamps
        self.assertIn('Feb', report['analysis_begun'])
        self.assertIn('Feb', report['analysis_ended'])

        # Check hydraulic status
        self.assertTrue(len(report['hydraulic_status']) > 0)
        first_event = report['hydraulic_status'][0]
        self.assertEqual(first_event['time'], '0:00:00')
        self.assertIn('Balanced', first_event['message'])

        # Check flow balance
        self.assertIn('total_inflow', report['flow_balance'])
        self.assertAlmostEqual(report['flow_balance']['total_inflow'], 1052.159)

        # Check quality balance
        self.assertIn('initial_mass', report['quality_balance'])

    def test_decode_file_if_exists(self):
        """Test decoding actual report file if it exists."""
        if not self.rpt_file.exists():
            self.skipTest(f"Test file not found: {self.rpt_file}")

        decoder = EpanetReportDecoder()
        report = decoder.decode_file(self.rpt_file)

        # Verify basic structure
        self.assertIn('version', report)
        self.assertIn('hydraulic_status', report)
        self.assertIn('flow_balance', report)
        self.assertIn('quality_balance', report)

        # Check that we got data
        self.assertIsNotNone(report['version'])
        self.assertTrue(len(report['hydraulic_status']) > 0)


class TestEpanetReport(unittest.TestCase):
    """Tests for EpanetReport high-level interface."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.data_dir = Path(__file__).parent.parent / 'data'
        cls.rpt_file = cls.data_dir / 'Net1.rpt'

    def test_report_context_manager(self):
        """Test report as context manager."""
        if not self.rpt_file.exists():
            self.skipTest(f"Test file not found: {self.rpt_file}")

        with EpanetReport(self.rpt_file) as report:
            self.assertIsNotNone(report.version)
            self.assertFalse(report.has_errors())

    def test_report_properties(self):
        """Test report properties."""
        if not self.rpt_file.exists():
            self.skipTest(f"Test file not found: {self.rpt_file}")

        with EpanetReport(self.rpt_file) as report:
            # Test all properties
            _ = report.version
            _ = report.analysis_begun
            _ = report.analysis_ended
            _ = report.hydraulic_status
            _ = report.flow_balance
            _ = report.quality_balance
            _ = report.node_results
            _ = report.link_results
            _ = report.energy_usage
            _ = report.warnings
            _ = report.errors

    def test_report_summary(self):
        """Test report summary method."""
        if not self.rpt_file.exists():
            self.skipTest(f"Test file not found: {self.rpt_file}")

        with EpanetReport(self.rpt_file) as report:
            summary = report.summary()

            self.assertIn('nodes_count', summary)
            self.assertIn('links_count', summary)
            self.assertIn('warnings_count', summary)
            self.assertIn('errors_count', summary)


if __name__ == '__main__':
    unittest.main()
