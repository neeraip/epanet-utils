# EPANET-Utils

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Utilities for parsing EPA EPANET input (`.inp`), report (`.rpt`), and binary output (`.out`) files.

## Features

- **Parse EPANET Input Files** - Read `.inp` files into Python dictionaries
- **Generate EPANET Input Files** - Create `.inp` files from Python data structures
- **Parse Report Files** - Parse text report files (`.rpt`) with hydraulic status and balances
- **Parse Binary Output Files** - Read binary output files (`.out`) with time series data
- **Format Conversion** - Convert between `.inp`, JSON, and Parquet formats
- **High-Level API** - Easy-to-use `EpanetInput`, `EpanetReport`, and `EpanetOutput` classes
- **Low-Level API** - Direct access via decoder/encoder classes
- **Pandas Integration** - Export sections and results as DataFrames

## Installation

```bash
# Clone the repository
git clone https://github.com/neeraip/epanet-utils.git
cd epanet-utils

# Install in development mode
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

## Quick Start

### Read an EPANET Input File

```python
from epanet_utils import EpanetInput

# Using context manager
with EpanetInput("network.inp") as model:
    print(f"Junctions: {len(model.junctions)}")
    print(f"Pipes: {len(model.pipes)}")
    
    # Access individual components
    for junction in model.junctions:
        print(f"{junction['id']}: elevation={junction['elevation']}")
```

### Create a Network from Scratch

```python
from epanet_utils import EpanetInput

model = EpanetInput()
model.title = "My Water Network"

# Add components
model.add_reservoir("R1", head=200)
model.add_junction("J1", elevation=100, demand=50)
model.add_junction("J2", elevation=95, demand=75)
model.add_pipe("P1", "R1", "J1", length=1000, diameter=12, roughness=100)
model.add_pipe("P2", "J1", "J2", length=500, diameter=10, roughness=100)

# Save the model
model.save("my_network.inp")
```

### Convert Between Formats

```python
from epanet_utils import EpanetInput

with EpanetInput("network.inp") as model:
    # Convert to JSON
    model.to_json("network.json", pretty=True)
    
    # Convert to Parquet (directory with multiple files)
    model.to_parquet("network_parquet/", single_file=False)
    
    # Convert to Parquet (single file)
    model.to_parquet("network.parquet", single_file=True)
```

### Parse Report Files (.rpt)

```python
from epanet_utils import EpanetReport

with EpanetReport("simulation.rpt") as report:
    # Get EPANET version and timestamps
    print(f"Version: {report.version}")
    print(f"Analysis began: {report.analysis_begun}")
    
    # Get hydraulic status events
    for event in report.hydraulic_status[:5]:
        print(f"  {event['time']}: {event['message']}")
    
    # Get flow and quality balances
    print(f"Flow Balance: {report.flow_balance}")
    print(f"Quality Balance: {report.quality_balance}")
    
    # Check for errors/warnings
    if report.has_warnings():
        print("Warnings:", report.warnings)
    
    # Get results as DataFrame
    nodes_df = report.nodes_to_dataframe()
    links_df = report.links_to_dataframe()
```

### Parse Binary Output Files (.out)

```python
from epanet_utils import EpanetOutput

with EpanetOutput("simulation.out") as output:
    # Get network summary
    print(f"Valid: {output.is_valid()}")
    print(f"Nodes: {output.num_nodes}")
    print(f"Links: {output.num_links}")
    print(f"Periods: {output.num_periods}")
    
    # Get node/link IDs
    print(f"Node IDs: {output.node_ids}")
    print(f"Link IDs: {output.link_ids}")
    
    # Get results for specific elements
    node_result = output.get_node_results("J1")
    print(f"Node J1: demand={node_result['demand']}, head={node_result['head']}")
    
    link_result = output.get_link_results("P1")
    print(f"Pipe P1: flow={link_result['flow']}, velocity={link_result['velocity']}")
    
    # Get time series for a node
    time_series = output.get_node_time_series("J1")
    for ts in time_series:
        print(f"Period {ts['period']}: pressure={ts['pressure']}")
    
    # Convert to DataFrames
    nodes_df = output.nodes_to_dataframe()  # All periods
    nodes_p0 = output.nodes_to_dataframe(period=0)  # Single period
    links_df = output.links_to_dataframe()
    energy_df = output.energy_to_dataframe()
```

### Low-Level API

```python
from epanet_utils import EpanetInputDecoder, EpanetInputEncoder

decoder = EpanetInputDecoder()
encoder = EpanetInputEncoder()

# Decode to dictionary
model_dict = decoder.decode_file("network.inp")

# Modify the dictionary
model_dict['title'] = "Modified Network"

# Encode back to file
encoder.encode_to_inp_file(model_dict, "modified.inp")
encoder.encode_to_json(model_dict, "network.json")
```

## Usage Scenarios

### Scenario 1: Basic Input File Operations

```python
from epanet_utils import EpanetInput

with EpanetInput("network.inp") as model:
    # Get model summary
    print(model.summary())
    # {'junctions': 9, 'reservoirs': 1, 'tanks': 1, 'pipes': 12, ...}
    
    # Access components by ID
    junction = model.get_junction("J1")
    pipe = model.get_pipe("P1")
    pump = model.get_pump("PUMP1")
```

### Scenario 2: Export to DataFrame

```python
from epanet_utils import EpanetInput

with EpanetInput("network.inp") as model:
    # Export sections as DataFrames
    junctions_df = model.to_dataframe("junctions")
    pipes_df = model.to_dataframe("pipes")
    
    # Analyze data
    print(f"Average elevation: {junctions_df['elevation'].mean():.2f}")
    print(f"Total pipe length: {pipes_df['length'].sum():,.0f}")
```

### Scenario 3: Modify and Save

```python
from epanet_utils import EpanetInput

with EpanetInput("network.inp") as model:
    # Modify junction elevation
    junction = model.get_junction("J1")
    if junction:
        junction['elevation'] += 10
    
    # Add a new pipe
    model.add_pipe("P_NEW", "J1", "J2", length=500, diameter=8, roughness=100)
    
    # Save modified model
    model.save("modified_network.inp")
```

### Scenario 4: Round-Trip Conversion

```python
from epanet_utils import EpanetInputDecoder, EpanetInputEncoder

decoder = EpanetInputDecoder()
encoder = EpanetInputEncoder()

# Load from INP
model = decoder.decode_file("original.inp")

# Convert to JSON
encoder.encode_to_json(model, "model.json", pretty=True)

# Load from JSON
json_model = decoder.decode_json("model.json")

# Convert to Parquet
encoder.encode_to_parquet(json_model, "model_parquet/", single_file=False)

# Load from Parquet
parquet_model = decoder.decode_parquet("model_parquet/")

# Convert back to INP
encoder.encode_to_inp_file(parquet_model, "final.inp")
```

### Scenario 5: Batch Processing

```python
from pathlib import Path
from epanet_utils import EpanetInputDecoder, EpanetInputEncoder

decoder = EpanetInputDecoder()
encoder = EpanetInputEncoder()

# Convert all .inp files in a directory to JSON
for inp_file in Path("models/").glob("*.inp"):
    model = decoder.decode_file(str(inp_file))
    json_file = inp_file.with_suffix('.json')
    encoder.encode_to_json(model, str(json_file), pretty=True)
    print(f"Converted {inp_file.name} → {json_file.name}")
```

## Testing

```bash
# Run all tests
pytest -q

# Run with coverage
pytest --cov=epanet_utils --cov-report=html

# Run specific test file
pytest tests/test_inp.py -v
```

## Running Examples

```bash
# Example 1: Basic input file operations
python examples/example1/example1.py

# Example 2: Creating networks
python examples/example2/example2.py
```

## Project Structure

```
epanet-utils/
├── src/
│   └── epanet_utils/              # Main package
│       ├── __init__.py            # Package exports
│       ├── inp.py                 # High-level input file interface
│       ├── inp_decoder.py         # Decode .inp/JSON/Parquet → dict
│       ├── inp_encoder.py         # Encode dict → .inp/JSON/Parquet
│       ├── rpt.py                 # High-level report file interface
│       ├── rpt_decoder.py         # Decode .rpt → dict
│       ├── out.py                 # High-level output file interface
│       └── out_decoder.py         # Decode binary .out → dict
├── examples/
│   ├── example1/                  # Basic input file example
│   └── example2/                  # Creating networks example
├── tests/
│   ├── test_inp.py                # Input file interface tests
│   ├── test_inp_decoder_encoder.py  # Core parsing tests
│   ├── test_rpt.py                # Report parser tests
│   └── test_out.py                # Binary output parser tests
├── docs/
│   └── EPANET_INPUT_FILE.md       # Complete EPANET input file reference
├── setup.py                       # Package configuration
├── pyproject.toml                 # Modern Python packaging config
├── requirements.txt               # Core dependencies
├── requirements-dev.txt           # Development dependencies
└── README.md                      # This file
```

## API Reference

### EpanetInput

High-level interface for EPANET input files.

```python
class EpanetInput:
    # Properties
    title: str                     # Model title
    junctions: List[Dict]          # Junction nodes
    reservoirs: List[Dict]         # Reservoir nodes
    tanks: List[Dict]              # Tank nodes
    pipes: List[Dict]              # Pipe links
    pumps: List[Dict]              # Pump links
    valves: List[Dict]             # Valve links
    patterns: List[Dict]           # Time patterns
    curves: List[Dict]             # Data curves
    options: Dict                  # Analysis options
    times: Dict                    # Time settings
    coordinates: List[Dict]        # Node coordinates
    
    # Methods
    save(filepath)                 # Save to .inp file
    to_json(filepath, pretty)      # Export to JSON
    to_parquet(filepath, single)   # Export to Parquet
    to_dict() -> Dict              # Get as dictionary
    to_dataframe(section) -> DataFrame  # Get section as DataFrame
    summary() -> Dict              # Get component counts
    
    # Lookup methods
    get_junction(id) -> Dict       # Find junction by ID
    get_pipe(id) -> Dict           # Find pipe by ID
    get_pump(id) -> Dict           # Find pump by ID
    get_tank(id) -> Dict           # Find tank by ID
    get_reservoir(id) -> Dict      # Find reservoir by ID
    get_valve(id) -> Dict          # Find valve by ID
    get_pattern(id) -> Dict        # Find pattern by ID
    get_curve(id) -> Dict          # Find curve by ID
    
    # Add methods
    add_junction(id, elevation, demand, pattern) -> Dict
    add_pipe(id, node1, node2, length, diameter, roughness, ...) -> Dict
    add_reservoir(id, head, pattern) -> Dict
    add_tank(id, elevation, init_level, ...) -> Dict
    add_pump(id, node1, node2, parameters) -> Dict
    add_pattern(id, multipliers) -> Dict
    add_curve(id, points) -> Dict
```

### EpanetReport

High-level interface for EPANET report files (.rpt).

```python
class EpanetReport:
    # Properties
    version: str                   # EPANET version
    analysis_begun: str            # Analysis start timestamp
    analysis_ended: str            # Analysis end timestamp
    hydraulic_status: List[Dict]   # Timestep status events
    flow_balance: Dict             # Hydraulic flow balance
    quality_balance: Dict          # Water quality mass balance
    energy_usage: Dict             # Energy usage summary
    node_results: List[Dict]       # Node results
    link_results: List[Dict]       # Link results
    warnings: List[str]            # Warning messages
    errors: List[str]              # Error messages
    
    # Methods
    to_dataframe(section) -> DataFrame
    nodes_to_dataframe() -> DataFrame
    links_to_dataframe() -> DataFrame
    to_dict() -> Dict
    summary() -> Dict
    has_errors() -> bool
    has_warnings() -> bool
    
    # Lookup methods
    get_node_result(node_id) -> Dict
    get_link_result(link_id) -> Dict
    get_pump_energy(pump_id) -> Dict
```

### EpanetOutput

High-level interface for EPANET binary output files (.out).

```python
class EpanetOutput:
    # Properties
    prolog: Dict                   # File header/metadata
    epilog: Dict                   # Summary statistics
    energy_usage: List[Dict]       # Pump energy data
    node_results: List[Dict]       # Final period node results
    link_results: List[Dict]       # Final period link results
    time_series: Dict              # Full time series data
    
    # Network properties
    num_nodes: int                 # Number of nodes
    num_links: int                 # Number of links
    num_pumps: int                 # Number of pumps
    num_periods: int               # Number of reporting periods
    node_ids: List[str]            # List of node IDs
    link_ids: List[str]            # List of link IDs
    title: str                     # Simulation title
    version: int                   # EPANET version
    report_time_step: int          # Report time step (seconds)
    simulation_duration: int       # Duration (seconds)
    
    # Methods
    is_valid() -> bool             # Check if file parsed successfully
    summary() -> Dict              # Get summary statistics
    to_dict() -> Dict              # Get complete output as dict
    
    # DataFrame methods
    to_dataframe(result_type, period) -> DataFrame
    nodes_to_dataframe(period) -> DataFrame
    links_to_dataframe(period) -> DataFrame
    energy_to_dataframe() -> DataFrame
    
    # Element lookup methods
    get_node_results(node_id, period) -> Dict
    get_link_results(link_id, period) -> Dict
    get_node_time_series(node_id) -> List[Dict]
    get_link_time_series(link_id) -> List[Dict]
```

## Dependencies

### Required
- Python 3.8+
- pandas >= 1.0.0
- pyarrow >= 10.0.0

### Development
- pytest >= 7.0.0
- pytest-cov >= 4.0.0

## Known Limitations

1. **Round-trip Formatting**: Some cosmetic differences may occur
   - Comments may not be preserved in exact original positions
   - Whitespace normalized to EPANET standard format
   - All data and structure fully preserved

2. **Complex Sections**: Some sections have simplified handling
   - `[CONTROLS]` - Stored as text
   - `[RULES]` - Stored as text

## License

MIT License

## Contact

For questions or issues, please open a GitHub issue.

## Related Projects

- [swmm-utils](https://github.com/neeraip/swmm-utils) - Similar utilities for EPA SWMM
- [EPANET](https://www.epa.gov/water-research/epanet) - EPA's Water Distribution Modeling Software
