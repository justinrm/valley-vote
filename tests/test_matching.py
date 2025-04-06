"""Tests for finance to legislator matching functionality."""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.match_finance_to_leg import parse_committee_name, match_finance_to_legislators

def test_parse_committee_name():
    """Test committee name parsing."""
    # Test valid committee names
    assert parse_committee_name("Committee to Elect John Smith") == "John Smith"
    assert parse_committee_name("Friends of Jane Doe for Senate") == "Jane Doe"
    assert parse_committee_name("Citizens for Bob Wilson") == "Bob Wilson"
    
    # Test invalid or non-committee names
    assert parse_committee_name("John Smith") is None  # No committee indicators
    assert parse_committee_name("") is None
    assert parse_committee_name(None) is None
    assert parse_committee_name("12345") is None

def test_match_finance_to_legislators():
    """Test finance to legislator matching."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        legislators_data = [
            {'legislator_id': 1, 'name': 'John Smith'},
            {'legislator_id': 2, 'name': 'Jane Doe'},
            {'legislator_id': 3, 'name': 'Bob Wilson'}
        ]
        legislators_file = Path(tmpdir) / 'legislators.csv'
        pd.DataFrame(legislators_data).to_csv(legislators_file, index=False)

        # Test direct name matching
        finance_data = [
            {'name': 'John Smith', 'amount': 1000},
            {'name': 'Jane M. Doe', 'amount': 2000},  # Should match despite middle initial
            {'name': 'Unknown Person', 'amount': 3000}  # Should not match
        ]
        finance_file = Path(tmpdir) / 'finance.csv'
        pd.DataFrame(finance_data).to_csv(finance_file, index=False)

        output_file = Path(tmpdir) / 'matched.csv'
        match_finance_to_legislators(finance_file, legislators_file, output_file)

        # Check results
        results = pd.read_csv(output_file)
        assert len(results) == 3
        assert results.loc[0, 'matched_legislator_id'] == 1  # John Smith
        assert results.loc[1, 'matched_legislator_id'] == 2  # Jane Doe
        assert pd.isna(results.loc[2, 'matched_legislator_id'])  # Unknown Person

        # Test committee name matching
        committee_data = [
            {'committee_name': 'Committee to Elect John Smith', 'amount': 1000},
            {'committee_name': 'Friends of Jane Doe', 'amount': 2000},
            {'committee_name': 'Not a Committee Name', 'amount': 3000}
        ]
        committee_file = Path(tmpdir) / 'committees.csv'
        pd.DataFrame(committee_data).to_csv(committee_file, index=False)

        output_file = Path(tmpdir) / 'matched_committees.csv'
        match_finance_to_legislators(committee_file, legislators_file, output_file)

        # Check results
        results = pd.read_csv(output_file)
        assert len(results) == 3
        assert results.loc[0, 'matched_legislator_id'] == 1  # John Smith
        assert results.loc[1, 'matched_legislator_id'] == 2  # Jane Doe
        assert pd.isna(results.loc[2, 'matched_legislator_id'])  # Not a committee

def test_match_finance_edge_cases():
    """Test edge cases in finance matching."""
    with tempfile.TemporaryDirectory() as tmpdir:
        legislators_data = [
            {'legislator_id': 1, 'name': 'John Smith'}
        ]
        legislators_file = Path(tmpdir) / 'legislators.csv'
        pd.DataFrame(legislators_data).to_csv(legislators_file, index=False)

        # Test empty finance file
        empty_finance = Path(tmpdir) / 'empty.csv'
        pd.DataFrame(columns=['name', 'amount']).to_csv(empty_finance, index=False)
        
        output_file = Path(tmpdir) / 'matched_empty.csv'
        match_finance_to_legislators(empty_finance, legislators_file, output_file)
        results = pd.read_csv(output_file)
        assert len(results) == 0

        # Test missing required columns
        bad_finance = Path(tmpdir) / 'bad.csv'
        pd.DataFrame({'amount': [1000]}).to_csv(bad_finance, index=False)
        
        output_file = Path(tmpdir) / 'matched_bad.csv'
        match_finance_to_legislators(bad_finance, legislators_file, output_file)
        results = pd.read_csv(output_file)
        assert len(results) == 1
        assert pd.isna(results.loc[0, 'matched_legislator_id']) 