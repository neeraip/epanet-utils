#!/usr/bin/env python3
"""
Example 2: Creating Networks and Advanced Operations

This example demonstrates:
- Creating a network from scratch
- Adding components programmatically
- Working with patterns and curves
- Round-trip conversions
"""

from pathlib import Path
import sys

# Add parent directory to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from epanet_utils import EpanetInput, EpanetInputDecoder, EpanetInputEncoder


def main():
    print("=" * 60)
    print("EPANET-Utils Example 2: Creating Networks")
    print("=" * 60)
    
    output_dir = Path(__file__).parent
    
    # --- Scenario 1: Create a simple network from scratch ---
    print("\n--- Scenario 1: Create Simple Network ---")
    
    model = EpanetInput()
    model.title = "Simple Water Distribution Network\nCreated with epanet-utils"
    
    # Add a reservoir (water source)
    model.add_reservoir("R1", head=200)
    print("Added reservoir R1 (head=200)")
    
    # Add junctions
    model.add_junction("J1", elevation=150, demand=100)
    model.add_junction("J2", elevation=145, demand=150)
    model.add_junction("J3", elevation=140, demand=200)
    model.add_junction("J4", elevation=135, demand=100)
    print("Added 4 junctions")
    
    # Add a tank
    model.add_tank("T1", elevation=180, init_level=10, min_level=0, 
                   max_level=20, diameter=50)
    print("Added tank T1")
    
    # Add pipes
    model.add_pipe("P1", "R1", "J1", length=1000, diameter=12, roughness=100)
    model.add_pipe("P2", "J1", "J2", length=500, diameter=10, roughness=100)
    model.add_pipe("P3", "J2", "J3", length=500, diameter=8, roughness=100)
    model.add_pipe("P4", "J3", "J4", length=500, diameter=8, roughness=100)
    model.add_pipe("P5", "J1", "T1", length=300, diameter=10, roughness=100)
    model.add_pipe("P6", "T1", "J4", length=400, diameter=10, roughness=100)
    print("Added 6 pipes")
    
    # Add a demand pattern
    model.add_pattern("PAT1", [0.5, 0.8, 1.0, 1.2, 1.5, 1.3, 1.1, 0.9, 0.7, 0.5, 0.4, 0.5])
    print("Added demand pattern PAT1")
    
    # Add a pump curve
    model.add_curve("PUMP1", [(0, 150), (500, 120), (1000, 80)])
    print("Added pump curve PUMP1")
    
    # Set options
    model.options["units"] = "GPM"
    model.options["headloss"] = "H-W"
    
    # Set time parameters
    model.times["duration"] = "24:00"
    model.times["hydraulic_timestep"] = "1:00"
    model.times["pattern_timestep"] = "2:00"
    
    # Print summary
    print(f"\nNetwork Summary:")
    for component, count in model.summary().items():
        print(f"  {component.capitalize()}: {count}")
    
    # Save the network
    simple_file = output_dir / "simple_network.inp"
    model.save(simple_file)
    print(f"\nSaved to: {simple_file}")
    
    # --- Scenario 2: Verify by reloading ---
    print("\n--- Scenario 2: Verify by Reloading ---")
    
    reloaded = EpanetInput(simple_file)
    print(f"Reloaded: {simple_file.name}")
    print(f"Junctions: {len(reloaded.junctions)}")
    print(f"Pipes: {len(reloaded.pipes)}")
    print(f"Patterns: {len(reloaded.patterns)}")
    print(f"Curves: {len(reloaded.curves)}")
    
    # Verify pattern
    pattern = reloaded.get_pattern("PAT1")
    if pattern:
        print(f"Pattern PAT1 multipliers: {pattern['multipliers']}")
    
    # --- Scenario 3: Round-trip conversion test ---
    print("\n--- Scenario 3: Round-trip Conversion Test ---")
    
    decoder = EpanetInputDecoder()
    encoder = EpanetInputEncoder()
    
    # Original → JSON → Dict
    json_file = output_dir / "simple_network.json"
    reloaded.to_json(json_file, pretty=True)
    from_json = decoder.decode_json(json_file)
    print(f"INP → JSON: {len(from_json['junctions'])} junctions")
    
    # Dict → Parquet → Dict
    parquet_dir = output_dir / "simple_network_parquet"
    encoder.encode_to_parquet(from_json, parquet_dir, single_file=False)
    from_parquet = decoder.decode_parquet(parquet_dir)
    print(f"JSON → Parquet → Dict: {len(from_parquet['junctions'])} junctions")
    
    # Dict → INP → Dict
    final_file = output_dir / "simple_network_final.inp"
    encoder.encode_to_inp_file(from_parquet, final_file)
    final = decoder.decode_file(final_file)
    print(f"Parquet → INP: {len(final['junctions'])} junctions")
    
    print("\nRound-trip verification:")
    print(f"  Original junctions: {len(reloaded.junctions)}")
    print(f"  Final junctions: {len(final['junctions'])}")
    print(f"  Match: {len(reloaded.junctions) == len(final['junctions'])}")
    
    # --- Scenario 4: Work with a more complex network ---
    print("\n--- Scenario 4: Working with Net3 ---")
    
    net3_file = Path(__file__).parent.parent.parent / "EPANET Example Files" / "asce-tf-wdst" / "Net3" / "Net3.inp"
    
    if net3_file.exists():
        with EpanetInput(net3_file) as net3:
            print(f"Loaded: {net3_file.name}")
            print(f"\nNet3 Summary:")
            for component, count in net3.summary().items():
                print(f"  {component.capitalize()}: {count}")
            
            # Get some statistics
            if net3.junctions:
                elevations = [j['elevation'] for j in net3.junctions if j.get('elevation') is not None]
                if elevations:
                    print(f"\nJunction elevation range: {min(elevations)} to {max(elevations)}")
            
            if net3.pipes:
                lengths = [p['length'] for p in net3.pipes if p.get('length') is not None]
                if lengths:
                    print(f"Pipe length range: {min(lengths)} to {max(lengths)}")
                    print(f"Total pipe length: {sum(lengths):,.0f}")
    else:
        print(f"Net3 file not found: {net3_file}")
    
    print("\n" + "=" * 60)
    print("Example 2 completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
