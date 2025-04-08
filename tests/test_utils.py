"""Tests for utility functions."""
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest
# Removed sys.path manipulation, rely on package install or pytest config
# import sys

from src.utils import save_json, convert_to_csv, setup_project_paths, clean_name, map_vote_value, VOTE_TEXT_MAP # Import necessary items

# Add src directory to sys.path to allow importing utils
# This assumes tests are run from the project root
# SRC_DIR = Path(__file__).parent.parent / "src"
# sys.path.insert(0, str(SRC_DIR))

def test_save_json():
    """Test JSON saving functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test basic save
        data = {'test': 'data'}
        path = Path(tmpdir) / 'test.json'
        assert save_json(data, path) is True
        assert path.exists()
        with path.open('r') as f:
            loaded = json.load(f)
        assert loaded == data

        # Test nested directory creation
        nested_path = Path(tmpdir) / 'nested' / 'test.json'
        assert save_json(data, nested_path) is True
        assert nested_path.exists()

def test_convert_to_csv():
    """Test CSV conversion functionality."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test with list of dicts
        data = [
            {'name': 'John', 'age': 30},
            {'name': 'Jane', 'age': 25}
        ]
        path = Path(tmpdir) / 'test.csv'
        rows = convert_to_csv(data, path)
        assert rows == 2
        assert path.exists()
        df = pd.read_csv(path)
        assert len(df) == 2
        assert list(df.columns) == ['name', 'age']

        # Test with empty data
        empty_path = Path(tmpdir) / 'empty.csv'
        rows = convert_to_csv([], empty_path)
        assert rows == 0
        assert empty_path.exists()

        # Test with specified columns
        columns = ['name', 'age', 'city']
        col_path = Path(tmpdir) / 'columns.csv'
        rows = convert_to_csv(data, col_path, columns=columns)
        assert rows == 2
        df = pd.read_csv(col_path)
        assert list(df.columns) == columns
        assert pd.isna(df['city']).all()

def test_setup_project_paths():
    """Test project path setup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        paths = setup_project_paths(tmpdir)
        
        # Check that all expected directories exist
        assert paths['base'] == Path(tmpdir)
        assert paths['raw'].exists()
        assert paths['processed'].exists()
        assert paths['legislators'].exists()
        assert paths['bills'].exists()
        assert paths['votes'].exists()
        
        # Check directory structure
        assert paths['legislators'].parent == paths['raw']
        assert paths['bills'].parent == paths['raw']
        assert paths['votes'].parent == paths['raw']

        # Test with default path
        default_paths = setup_project_paths()
        assert default_paths['base'] == Path('data')
        assert all(isinstance(p, Path) for p in default_paths.values()) 

# --- Test cases for clean_name --- 
# Using pytest.mark.parametrize to run the function with multiple inputs/outputs
@pytest.mark.parametrize(
    "input_name, expected_output",
    [
        # Basic cases
        ("John Smith", "John Smith"),
        ("Jane Doe", "Jane Doe"),
        # Titles
        ("Rep. John Smith", "John Smith"),
        ("Senator Jane Doe", "Jane Doe"),
        ("Sen. John Smith", "John Smith"),
        ("Representative Jane Doe", "Jane Doe"),
        ("Delegate John Smith", "John Smith"),
        ("Del. Jane Doe", "Jane Doe"),
        # Suffixes
        ("John Smith Jr.", "John Smith"),
        ("Jane Doe Sr", "Jane Doe"),
        ("John Smith III", "John Smith"),
        ("Jane Doe IV", "Jane Doe"),
        # Titles and Suffixes
        ("Rep. John Smith Jr.", "John Smith"),
        ("Sen Jane Doe III", "Jane Doe"),
        # Whitespace
        ("  John   Smith  ", "John Smith"),
        ("\tJane Doe\n", "Jane Doe"),
        # Punctuation (commas often separate suffix or role)
        ("Smith, John", "Smith, John"), # Assuming simple comma handling doesn't reorder yet
        ("Doe, Jane (R)", "Doe, Jane"), # Handles parenthetical party
        ("John Smith (D-District 5)", "John Smith"),
        # Middle names/initials
        ("John F. Smith", "John F. Smith"),
        ("Jane Alice Doe", "Jane Alice Doe"),
        ("Rep. John F. Smith Jr.", "John F. Smith"),
        # Edge cases
        ("Smith", "Smith"),
        ("J.", "J."), # Very short names
        ("", ""),   # Empty string
        (None, None), # None input
        # Potential tricky cases (based on implementation regex)
        ("Mr. John Smith", "John Smith"),
        ("Ms. Jane Doe", "Jane Doe"),
        ("Dr. John Smith", "John Smith"),
        ("(R) John Smith", "John Smith"), # Party at start
    ]
)
def test_clean_name(input_name, expected_output):
    """Tests the clean_name function with various inputs."""
    assert clean_name(input_name) == expected_output

# Example of a test that might fail if comma handling is simple
# def test_clean_name_comma_reorder():
#    """Test specifically if 'Last, First' is reordered."""
#    # This test would depend on the exact implementation detail
#    assert clean_name("Smith, John") == "John Smith" 

# --- Test cases for map_vote_value ---
@pytest.mark.parametrize(
    "input_vote, expected_output",
    [
        # Direct matches from VOTE_TEXT_MAP
        ("Yes", 1),
        ("Aye", 1),
        ("Yea", 1),
        ("No", -1),
        ("Nay", -1),
        ("Absent", 0),
        ("Excused", 0),
        ("Not Voting", 0),
        ("Present", 0),
        # Case variations
        ("yes", 1),
        ("nAY", -1),
        ("aBsEnT", 0),
        # Values not in map
        ("Maybe", None),
        ("Undecided", None),
        ("Yea ", 1), # Trailing space should be handled by strip
        (" Nay", -1), # Leading space should be handled by strip
        ("", None), # Empty string
        (None, None), # None input
    ]
)
def test_map_vote_value(input_vote, expected_output):
    """Tests the map_vote_value function with various inputs."""
    assert map_vote_value(input_vote) == expected_output

# Example of a test that might fail if comma handling is simple
# def test_clean_name_comma_reorder():
#    """Test specifically if 'Last, First' is reordered."""
#    # This test would depend on the exact implementation detail
#    assert clean_name("Smith, John") == "John Smith" 