import os

def create_directory_structure():
    # Define the base directory
    base_dir = "valley-vote"
    
    # Define the directory structure
    directories = [
        "data/raw/legislators",
        "data/raw/bills",
        "data/raw/votes",
        "data/raw/committees",
        "data/raw/committee_memberships",
        "data/raw/sponsors",
        "data/processed",
        "data/models",
        "data/outputs",
        "src",
        "frontend",
        "notebooks",
        "docs"
    ]
    
    # Create the base directory if it doesn't exist
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"Created directory: {base_dir}")
    
    # Create each directory in the structure
    for directory in directories:
        path = os.path.join(base_dir, directory)
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Created directory: {path}")
        else:
            print(f"Directory already exists: {path}")
    
    print("\nDirectory structure creation complete!")
    print("The following structure has been created:")
    print("""
    /valley-vote
    ├── data/
    │   ├── raw/
    │   │   ├── legislators/
    │   │   ├── bills/
    │   │   ├── votes/
    │   │   ├── committees/
    │   │   ├── committee_memberships/
    │   │   └── sponsors/
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
