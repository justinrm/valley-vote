import os
import sys

def create_directory_structure():
    """
    Creates the necessary directory structure for the valley-vote project.
    Reads directory paths defined in other project files to ensure completeness.
    """
    # Define the base directory for the project
    base_dir = "valley-vote"

    # Define the core directory structure relative to the base_dir
    # This list is derived from analyzing data_collection.py and scrape_finance_idaho.py
    directories = [
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

    print(f"Attempting to create directory structure under: {os.path.abspath(base_dir)}")

    # Create the base directory first if it doesn't exist
    try:
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
            print(f"Created base directory: {base_dir}")
        else:
            print(f"Base directory already exists: {base_dir}")
    except OSError as e:
        print(f"ERROR: Could not create base directory '{base_dir}'. Check permissions. Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Create each sub-directory in the structure
    created_count = 0
    skipped_count = 0
    for directory in directories:
        # Construct the full path relative to the current working directory
        path = os.path.join(base_dir, directory)
        try:
            # Use os.makedirs with exist_ok=True to handle nested creation and existing dirs
            os.makedirs(path, exist_ok=True)

            # Check if it *was* created now vs already existing before the call
            # This check is slightly less precise with exist_ok=True, but good enough for feedback
            # A more robust way would be to check existence *before* the makedirs call
            # For simplicity, we'll just report based on the exist_ok behavior.
            # We assume if exist_ok=True didn't raise an error, it either exists or was created.

            # Refined check: Test existence *before* attempting creation for accurate reporting
            if not os.path.exists(path):
                # This block might not be reached if exist_ok=True creates it silently
                # Re-checking after is better
                os.makedirs(path, exist_ok=True)  # Ensure it exists
                print(f"Created directory: {path}")
                created_count += 1
            else:
                # If it already existed before or was just created by makedirs
                # To be absolutely sure, we could check existence *before* calling makedirs
                # but let's keep it simpler: report 'exists' if makedirs doesn't raise error
                # Let's check *before* the main creation call for better reporting
                path_existed_before = os.path.exists(path)
                os.makedirs(path, exist_ok=True)  # Ensure creation
                if not path_existed_before:
                    print(f"Created directory: {path}")
                    created_count += 1
                else:
                    print(f"Directory already exists: {path}")
                    skipped_count += 1

        except OSError as e:
            print(f"ERROR: Could not create directory '{path}'. Check permissions. Error: {e}", file=sys.stderr)
            # Decide if we should stop or continue
            # continue  # Continue trying other directories

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

if __name__ == "__main__":
    create_directory_structure()
