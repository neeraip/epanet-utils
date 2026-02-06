#!/usr/bin/env python3
"""
Example 1: Basic EPANET Input File Operations

This example demonstrates:
- Loading an EPANET input file
- Accessing model components
- Converting between formats (INP, JSON, Parquet)
- Modifying and saving models
"""

from pathlib import Path
import sys

# Add parent directory to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from epanet_utils import EpanetInput, EpanetInputDecoder, EpanetInputEncoder


def main():
    # Path to sample input file
    sample_file = Path(__file__).parent.parent.parent / "EPANET Example Files" / "epanet-desktop" / "Net1" / "Net1.inp"
    
    if not sample_file.exists():
        print(f"Sample file not found: {sample_file}")
        print("Please ensure EPANET Example Files are in the correct location.")
        return
    
    print("=" * 60)
    print("EPANET-Utils Example 1: Basic Input File Operations")
    print("=" * 60)
    
    # --- Scenario 1: Load and inspect a model ---
    print("\n--- Scenario 1: Load and Inspect Model ---")
    
    with EpanetInput(sample_file) as model:
        print(f"Loaded: {sample_file.name}")
        print(f"\nModel Summary:")
        summary = model.summary()
        for component, count in summary.items():
            print(f"  {component.capitalize()}: {count}")
        
        print(f"\nTitle: {model.title}")
    
    # --- Scenario 2: Access individual components ---
    print("\n--- Scenario 2: Access Individual Components ---")
    
    with EpanetInput(sample_file) as model:
        print("\nJunctions:")
        for junction in model.junctions[:3]:
            print(f"  ID: {junction['id']}, Elevation: {junction['elevation']}, Demand: {junction['demand']}")
        if len(model.junctions) > 3:
            print(f"  ... and {len(model.junctions) - 3} more")
        
        print("\nPipes:")
        for pipe in model.pipes[:3]:
            print(f"  ID: {pipe['id']}, {pipe['node1']} → {pipe['node2']}, "
                  f"Length: {pipe['length']}, Diameter: {pipe['diameter']}")
        if len(model.pipes) > 3:
            print(f"  ... and {len(model.pipes) - 3} more")
        
        print("\nReservoirs:")
        for reservoir in model.reservoirs:
            print(f"  ID: {reservoir['id']}, Head: {reservoir['head']}")
        
        print("\nTanks:")
        for tank in model.tanks:
            print(f"  ID: {tank['id']}, Elevation: {tank['elevation']}, "
                  f"Initial Level: {tank['init_level']}")
        
        print("\nPumps:")
        for pump in model.pumps:
            print(f"  ID: {pump['id']}, {pump['node1']} → {pump['node2']}, "
                  f"Parameters: {pump['parameters']}")
    
    # --- Scenario 3: Convert to JSON ---
    print("\n--- Scenario 3: Convert to JSON ---")
    
    output_dir = Path(__file__).parent
    json_file = output_dir / "Net1.json"
    
    with EpanetInput(sample_file) as model:
        model.to_json(json_file, pretty=True)
        print(f"Saved JSON: {json_file}")
        print(f"File size: {json_file.stat().st_size:,} bytes")
    
    # --- Scenario 4: Convert to Parquet ---
    print("\n--- Scenario 4: Convert to Parquet ---")
    
    parquet_dir = output_dir / "Net1_parquet"
    
    with EpanetInput(sample_file) as model:
        model.to_parquet(parquet_dir, single_file=False)
        
        parquet_files = list(parquet_dir.glob("*.parquet"))
        print(f"Saved Parquet directory: {parquet_dir}")
        print(f"Files created: {len(parquet_files)}")
        total_size = sum(f.stat().st_size for f in parquet_files)
        print(f"Total size: {total_size:,} bytes")
    
    # --- Scenario 5: Get DataFrames ---
    print("\n--- Scenario 5: Get DataFrames ---")
    
    with EpanetInput(sample_file) as model:
        junctions_df = model.to_dataframe("junctions")
        pipes_df = model.to_dataframe("pipes")
        
        print("\nJunctions DataFrame:")
        print(junctions_df.to_string(index=False))
        
        print("\nPipes DataFrame (first 5):")
        print(pipes_df.head().to_string(index=False))
    
    # --- Scenario 6: Modify and Save ---
    print("\n--- Scenario 6: Modify and Save ---")
    
    modified_file = output_dir / "Net1_modified.inp"
    
    with EpanetInput(sample_file) as model:
        # Modify title
        original_title = model.title
        model.title = "Modified " + original_title
        
        # Modify a junction elevation
        junction = model.get_junction("10")
        if junction:
            original_elev = junction["elevation"]
            junction["elevation"] = original_elev + 10
            print(f"Modified junction 10 elevation: {original_elev} → {junction['elevation']}")
        
        # Save modified model
        model.save(modified_file)
        print(f"Saved modified model: {modified_file}")
    
    # --- Scenario 7: Low-level API ---
    print("\n--- Scenario 7: Low-level Decoder/Encoder API ---")
    
    decoder = EpanetInputDecoder()
    encoder = EpanetInputEncoder()
    
    # Decode to dictionary
    model_dict = decoder.decode_file(sample_file)
    print(f"Decoded to dict with {len(model_dict)} sections")
    
    # Encode back to string
    inp_string = encoder.encode_to_inp_string(model_dict)
    print(f"Encoded to INP string ({len(inp_string):,} characters)")
    
    print("\n" + "=" * 60)
    print("Example 1 completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
