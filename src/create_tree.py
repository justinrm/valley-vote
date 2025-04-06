"""Create the directory structure for the valley-vote project."""

# Standard library imports
import os
import sys
from pathlib import Path
from typing import List, Tuple

def create_directory_structure() -> Tuple[int, int]:
    """Creates the necessary directory structure for the valley-vote project.
    
    This function creates a standardized directory structure based on the project's needs.
    It ensures all required directories exist for data collection, processing, and analysis.
    
    Returns:
        Tuple[int, int]: A tuple containing (created_count, skipped_count)
            - created_count: Number of directories created
            - skipped_count: Number of directories that already existed
    """
    # Define the base directory for the project
    base_dir = Path("valley-vote")

    # Define the core directory structure relative to the base_dir
    # This list is derived from analyzing data_collection.py and scrape_finance_idaho.py
    directories: List[str] = [
        # Raw data directories from data_collection.py
        "data/raw/legislators",
        "data/raw/bills",
        "data/raw/votes",
        "data/raw/committees",
        "data/raw/committee_memberships",
        "data/raw/sponsors",
        "data/raw/campaign_finance",         # Parent dir for finance data
        "data/raw/campaign_finance/idaho",   # Specific subdir used by scrape_finance_idaho.py
        "data/raw/demographics",             # Stub dir from data_collection.py
        "data/raw/elections",                # Stub dir from data_collection.py
        "data/raw/texts",                    # For bill texts (from data_collection.py)
        "data/raw/amendments",               # For bill amendments (from data_collection.py)
        "data/raw/supplements",              # For bill supplements (from data_collection.py)

        # Processed data directory
        "data/processed",

        # Other data-related directories (common practice)
        "data/models",
        "data/outputs",

        # Code and project directories
        "src",
        "frontend",       # Included as per original structure
        "notebooks",
        "docs"
    ]

    print(f"Attempting to create directory structure under: {base_dir.absolute()}")

    # Create the base directory first if it doesn't exist
    try:
        base_dir.mkdir(exist_ok=True)
        print(f"Base directory {'created' if not base_dir.exists() else 'already exists'}: {base_dir}")
    except OSError as e:
        print(f"ERROR: Could not create base directory '{base_dir}'. Check permissions. Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Create each sub-directory in the structure
    created_count = 0
    skipped_count = 0
    
    for directory in directories:
        path = base_dir / directory
        try:
            # Check if directory exists before attempting creation
            if path.exists():
                print(f"Directory already exists: {path}")
                skipped_count += 1
            else:
                path.mkdir(parents=True, exist_ok=True)
                print(f"Created directory: {path}")
                created_count += 1
        except OSError as e:
            print(f"ERROR: Could not create directory '{path}'. Check permissions. Error: {e}", file=sys.stderr)
            continue

    print(f"\nDirectory structure creation complete.")
    print(f"Created: {created_count} directories.")
    print(f"Skipped (already existed): {skipped_count} directories.")

    # Print the visual structure
    print("\nThe following structure should now exist:")
    print("""
/valley-vote
├── data/
│   ├── raw/
│   │   ├── legislators/
│   │   ├── bills/
│   │   ├── votes/
│   │   ├── committees/
│   │   ├── committee_memberships/
│   │   ├── sponsors/
│   │   ├── campaign_finance/
│   │   │   └── idaho/
│   │   ├── demographics/
│   │   ├── elections/
│   │   ├── texts/
│   │   ├── amendments/
│   │   └── supplements/
│   ├── processed/
│   ├── models/
│   └── outputs/
├── src/
├── frontend/
├── notebooks/
└── docs/
""")
    
    return created_count, skipped_count

if __name__ == "__main__":
    create_directory_structure()
