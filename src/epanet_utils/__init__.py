"""
EPANET Utilities - Parse EPANET input, report, and output files.

This package provides tools for working with EPA EPANET water distribution
system modeling files, including:
- Input file (.inp) parsing and generation
- Report file (.rpt) parsing
- Binary output file (.out) parsing
- Format conversion (JSON, Parquet)

Basic Usage:
    >>> from epanet_utils import EpanetInput, EpanetReport, EpanetOutput
    >>> 
    >>> # Parse input file
    >>> with EpanetInput("model.inp") as model:
    ...     print(model.junctions)
    >>> 
    >>> # Parse report file
    >>> with EpanetReport("simulation.rpt") as report:
    ...     print(report.flow_balance)
    >>> 
    >>> # Parse binary output file
    >>> with EpanetOutput("simulation.out") as output:
    ...     print(f"Nodes: {output.num_nodes}")
    ...     df = output.nodes_to_dataframe()
"""

__version__ = "0.1.0"

from .inp_decoder import EpanetInputDecoder
from .inp_encoder import EpanetInputEncoder
from .inp import EpanetInput
from .rpt_decoder import EpanetReportDecoder
from .rpt import EpanetReport
from .out_decoder import EpanetOutputDecoder
from .out import EpanetOutput
from .exports import (
    decode_to_data_json,
    encode_with_overlay,
    emit_report_json,
    emit_results_zarr,
    SPATIAL_SECTIONS,
    NON_SPATIAL_SECTIONS,
)

__all__ = [
    # Input file handling
    "EpanetInputDecoder",
    "EpanetInputEncoder",
    "EpanetInput",
    # Report file handling
    "EpanetReportDecoder",
    "EpanetReport",
    # Binary output file handling
    "EpanetOutputDecoder",
    "EpanetOutput",
    # Producer-side exports (Console / WRM API)
    "decode_to_data_json",
    "encode_with_overlay",
    "emit_report_json",
    "emit_results_zarr",
    "SPATIAL_SECTIONS",
    "NON_SPATIAL_SECTIONS",
    # Version
    "__version__",
]
