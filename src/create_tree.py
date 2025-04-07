"""Create the directory structure for the valley-vote project."""

# Standard library imports
import os
import sys
from pathlib import Path
from typing import List, Tuple

def create_directory_structure(base_path: str = ".") -> Tuple[int, int]:
    """Creates the necessary directory structure for the valley-vote project.
    
    Ensures all required directories exist based on the documented structure
    in README.md.

    Args:
        base_path (str): The base path relative to which the structure will be created.
                         Defaults to the current directory (".").

    Returns:
        Tuple[int, int]: A tuple containing (created_count, skipped_count)
    """
    # Define the base directory for the project
    base_dir = Path(base_path)

    # Define the core directory structure relative to the base_dir
    # Updated based on README.md project structure section (as of recent edits)
    directories: List[str] = [
        # Top Level Data Dirs
        "data/artifacts/debug", # Debug artifacts (e.g., Playwright traces)
        "data/logs",
        "data/processed",
        "data/raw/amendments",
        "data/raw/bills",
        "data/raw/campaign_finance", # Parent dir
        # "data/raw/campaign_finance/idaho", # Specific subdir for Idaho if needed, removed for simplicity now
        "data/raw/committee_memberships",
        "data/raw/committees",
        "data/raw/demographics",
        "data/raw/elections",
        "data/raw/legislators",
        "data/raw/sponsors",
        "data/raw/supplements",
        "data/raw/texts",
        "data/raw/votes",
        "data/models", # Added from previous script, standard practice
        "data/outputs", # Added from previous script, standard practice

        # Code and project directories
        "src",
        "notebooks",
        "docs",
        "tests", # Added based on README and standard practice
        # "frontend", # Commented out as it's purely planned, let's create core structure first
    ]

    print(f"Attempting to create directory structure under: {base_dir.resolve()}")

    # Create the base directory first if it doesn't exist and is not the current dir
    if base_path != ".":
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
                # print(f"Directory already exists: {path}") # Reduce verbosity
                skipped_count += 1
            else:
                path.mkdir(parents=True, exist_ok=True)
                print(f"Created directory: {path}")
                created_count += 1
        except OSError as e:
            print(f"ERROR: Could not create directory '{path}'. Check permissions. Error: {e}", file=sys.stderr)
            # Decide whether to continue or exit on error
            # continue
            sys.exit(f"Failed creating structure at {path}")

    print(f"\nDirectory structure creation complete.")
    print(f"Created: {created_count} directories.")
    print(f"Skipped (already existed): {skipped_count} directories.")

    # Print the visual structure based on the directories list
    print("\nThe following structure should now exist (relative to {base_dir}):")
    # Basic text representation
    tree = { "data": { "artifacts": {"debug": {}}, 
                      "logs": {}, 
                      "processed": {}, 
                      "raw": {"amendments": {}, "bills": {}, "campaign_finance": {}, 
                              "committee_memberships": {}, "committees": {}, "demographics": {}, 
                              "elections": {}, "legislators": {}, "sponsors": {}, 
                              "supplements": {}, "texts": {}, "votes": {}},
                      "models": {}, 
                      "outputs": {}
                    },
             "src": {},
             "notebooks": {},
             "docs": {},
             "tests": {}
           }
    
    def print_tree(d, indent=''):
        # Sort keys for consistent output
        keys = sorted(d.keys())
        last_key = keys[-1] if keys else None
        for i, key in enumerate(keys):
            connector = "└── " if i == len(keys) - 1 else "├── "
            print(f"{indent}{connector}{key}/")
            new_indent = indent + ("    " if i == len(keys) - 1 else "│   ")
            if isinstance(d[key], dict) and d[key]: # Only recurse if dict is not empty
                 print_tree(d[key], new_indent)

    print(f"{base_dir}/")
    print_tree(tree, "")
    
    return created_count, skipped_count

if __name__ == "__main__":
    # Allow specifying a base path, otherwise use current dir
    target_base_path = sys.argv[1] if len(sys.argv) > 1 else "."
    create_directory_structure(base_path=target_base_path)
