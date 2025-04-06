"""Tests for utility functions."""
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.utils import save_json, convert_to_csv, setup_project_paths

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