"""Tests for the finance data collection module (finance_collection.py)."""

import pytest
import argparse
from unittest.mock import patch, MagicMock, call
from pathlib import Path
import pandas as pd
import requests
from src.utils import clean_name # Assuming clean_name is in utils

# Import functions and classes to test
from src.finance_collection import (
    fetch_finance_data,
    get_candidates_list,
    get_candidate_contributions,
    collect_finance_data,
    consolidate_finance_data,
    main_finance_collection,
    FinanceAPIError,
    FinanceRateLimitError,
    match_finance_to_legislators # Add this import
)

# Mock paths dictionary (subset relevant to finance)
@pytest.fixture
def mock_finance_paths():
    """Fixture for mock project paths relevant to finance."""
    base = Path('/fake/data')
    raw_finance = base / 'raw' / 'finance'
    return {
        'base': base,
        'raw_finance': raw_finance,
        'raw_legislators': base / 'raw' / 'legislators', # Needed for matching
        'processed': base / 'processed',
        'logs': base / 'logs',
    }

# Mock successful API response for candidates
@pytest.fixture
def mock_candidates_response():
    """Fixture for a successful candidates list API response."""
    return {
        'status': 'success',
        'candidates': [
            {'id': 'cand1', 'name': 'Alice Legislator', 'party': 'D'},
            {'id': 'cand2', 'name': 'Bob Lawmaker', 'party': 'R'}
        ],
        'pagination': {'page': 1, 'pages': 1, 'per_page': 100, 'total': 2}
    }

# Mock successful API response for contributions (page 1)
@pytest.fixture
def mock_contributions_response_page1():
    """Fixture for a successful contributions API response (page 1)."""
    return {
        'status': 'success',
        'contributions': [
            {'id': 'contrib1', 'donor': 'Donor A', 'amount': 100.00},
            {'id': 'contrib2', 'donor': 'Donor B', 'amount': 250.50}
        ],
        'pagination': {'page': 1, 'pages': 2, 'per_page': 2, 'total': 3}
    }

# Mock successful API response for contributions (page 2)
@pytest.fixture
def mock_contributions_response_page2():
    """Fixture for a successful contributions API response (page 2)."""
    return {
        'status': 'success',
        'contributions': [
            {'id': 'contrib3', 'donor': 'Donor C', 'amount': 50.00}
        ],
        'pagination': {'page': 2, 'pages': 2, 'per_page': 2, 'total': 3}
    }

# Mock empty contributions response
@pytest.fixture
def mock_contributions_response_empty():
    """Fixture for an empty contributions API response."""
    return {
        'status': 'success',
        'contributions': [],
        'pagination': {'page': 1, 'pages': 1, 'per_page': 100, 'total': 0}
    }

# Mock legislator data for matching
@pytest.fixture
def mock_legislators_df():
    """Fixture for a mock legislators DataFrame."""
    return pd.DataFrame([
        {'legislator_id': 101, 'name': 'Alice Legislator', 'party': 'D'},
        {'legislator_id': 102, 'name': 'Robert Lawmaker', 'party': 'R'}, # Slightly different name
        {'legislator_id': 103, 'name': 'Charlie Committee', 'party': 'I'}
    ])

@pytest.fixture
def sample_finance_df():
    """Fixture for sample finance DataFrame before matching."""
    return pd.DataFrame([
        {'candidate_name': 'Alice Legislator', 'amount': 100, 'year': 2024},
        {'candidate_name': ' Bob Lawmaker ', 'amount': 200, 'year': 2024}, # Needs cleaning/fuzzy
        {'candidate_name': 'ALICE LEGISLATOR', 'amount': 50, 'year': 2023}, # Different case, same person
        {'candidate_name': 'Unknown Candidate', 'amount': 300, 'year': 2024}, # No match expected
        {'candidate_name': 'Charles Committee', 'amount': 150, 'year': 2024}, # Should match Charlie
    ])

# --- Start adding test functions below ---

@patch('src.finance_collection.requests.get')
@patch('src.finance_collection.time.sleep') # Mock sleep to speed up tests
@patch('src.finance_collection.FINANCE_BASE_URL', 'https://api.examplefinance.com') # Mock base URL
@patch('src.finance_collection.FINANCE_API_KEY', 'fake_api_key') # Mock API key
def test_fetch_finance_data_success(mock_sleep, mock_get):
    """Test successful fetching of finance API data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "success", "data": [{"key": "value"}]}
    mock_get.return_value = mock_response

    result = fetch_finance_data("test_endpoint", {"param1": "val1"})

    assert result == {"status": "success", "data": [{"key": "value"}]}
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert args[0] == "https://api.examplefinance.com/test_endpoint"
    assert kwargs['params'] == {"param1": "val1", "api_key": "fake_api_key"}
    mock_sleep.assert_called_once()

@patch('src.finance_collection.requests.get')
@patch('src.finance_collection.time.sleep')
@patch('src.finance_collection.FINANCE_BASE_URL', 'https://api.examplefinance.com')
@patch('src.finance_collection.FINANCE_API_KEY', 'fake_api_key')
def test_fetch_finance_data_rate_limit_retry(mock_sleep, mock_get):
    """Test retry logic upon hitting FinanceRateLimitError (429)."""
    mock_rate_limit_response = MagicMock()
    mock_rate_limit_response.status_code = 429

    mock_success_response = MagicMock()
    mock_success_response.status_code = 200
    mock_success_response.json.return_value = {"status": "success", "data": "ok"}

    # Simulate 429 then success
    mock_get.side_effect = [mock_rate_limit_response, mock_success_response]

    result = fetch_finance_data("rate_limit_test", {})

    assert result == {"status": "success", "data": "ok"}
    assert mock_get.call_count == 2
    assert mock_sleep.call_count >= 1 # Ensure retry sleep occurred

@patch('src.finance_collection.requests.get')
@patch('src.finance_collection.time.sleep')
@patch('src.finance_collection.FINANCE_BASE_URL', 'https://api.examplefinance.com')
@patch('src.finance_collection.FINANCE_API_KEY', 'fake_api_key')
def test_fetch_finance_data_api_error(mock_sleep, mock_get):
    """Test handling of API error response (status: error)."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "error", "error": {"message": "Invalid parameter"}}
    mock_get.return_value = mock_response

    result = fetch_finance_data("api_error_test", {})

    assert result is None # Expect None on API error
    mock_get.assert_called_once()
    mock_sleep.assert_called_once()

@patch('src.finance_collection.requests.get')
@patch('src.finance_collection.time.sleep')
@patch('src.finance_collection.FINANCE_BASE_URL', 'https://api.examplefinance.com')
@patch('src.finance_collection.FINANCE_API_KEY', 'fake_api_key')
@patch('src.finance_collection.FINANCE_MAX_RETRIES', 3) # Ensure retry count is known
def test_fetch_finance_data_http_error(mock_sleep, mock_get):
    """Test handling of non-429 HTTP errors (should raise RequestException for retry)."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    # Configure raise_for_status on the mock object *itself* if called
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error", response=mock_response)
    mock_get.return_value = mock_response

    # Expect RequestException after retries fail
    with pytest.raises(requests.exceptions.RequestException):
        fetch_finance_data("http_error_test", {})

    assert mock_get.call_count == 3 # Check against FINANCE_MAX_RETRIES
    assert mock_sleep.call_count >= 2 # Retries involve sleep

@patch('src.finance_collection.FINANCE_API_KEY', None) # No API Key set
def test_fetch_finance_data_no_api_key():
    """Test that fetch_finance_data returns None if API key is not set."""
    # Don't need patch requests/sleep as it should exit early
    result = fetch_finance_data("no_key_test", {})
    assert result is None

# --- Tests for get_candidates_list ---

@patch('src.finance_collection.fetch_finance_data')
def test_get_candidates_list_success(mock_fetch, mock_candidates_response):
    """Test successfully getting a list of candidates."""
    mock_fetch.return_value = mock_candidates_response
    year = 2024
    state = 'ID'

    candidates = get_candidates_list(year, state)

    assert candidates is not None
    assert len(candidates) == 2
    assert candidates[0]['name'] == 'Alice Legislator'
    mock_fetch.assert_called_once_with(
        'candidates',
        {'year': year, 'state': state, 'office_type': 'legislative'}
    )

@patch('src.finance_collection.fetch_finance_data')
def test_get_candidates_list_api_failure(mock_fetch):
    """Test get_candidates_list when the underlying API call fails."""
    mock_fetch.return_value = None # Simulate fetch failure

    candidates = get_candidates_list(2024, 'ID')

    assert candidates is None
    mock_fetch.assert_called_once()

@patch('src.finance_collection.fetch_finance_data')
def test_get_candidates_list_empty_response(mock_fetch):
    """Test get_candidates_list with an empty but valid API response."""
    mock_fetch.return_value = {'status': 'success', 'candidates': []}

    candidates = get_candidates_list(2024, 'ID')

    assert candidates == []
    mock_fetch.assert_called_once()

# --- Tests for get_candidate_contributions ---

@patch('src.finance_collection.fetch_finance_data')
def test_get_contributions_success_single_page(mock_fetch, mock_contributions_response_empty):
    """Test getting contributions that fit on a single page."""
    # Mock response where total fits on one page
    single_page_response = mock_contributions_response_empty.copy()
    single_page_response['contributions'] = [
        {'id': 'c1', 'donor': 'D1', 'amount': 10},
        {'id': 'c2', 'donor': 'D2', 'amount': 20}
    ]
    single_page_response['pagination']['total'] = 2
    single_page_response['pagination']['pages'] = 1
    single_page_response['pagination']['per_page'] = 100

    mock_fetch.return_value = single_page_response

    contributions = get_candidate_contributions('cand123', 2024)

    assert contributions is not None
    assert len(contributions) == 2
    assert contributions[0]['id'] == 'c1'
    mock_fetch.assert_called_once_with(
        'contributions',
        {'candidate_id': 'cand123', 'year': 2024, 'per_page': 100, 'page': 1}
    )

@patch('src.finance_collection.fetch_finance_data')
def test_get_contributions_success_multiple_pages(
    mock_fetch,
    mock_contributions_response_page1,
    mock_contributions_response_page2,
    mock_contributions_response_empty # Use empty for the call after the last page
):
    """Test getting contributions spanning multiple pages."""
    # Simulate page 1, page 2, then an empty page 3 response
    mock_fetch.side_effect = [
        mock_contributions_response_page1,
        mock_contributions_response_page2,
        mock_contributions_response_empty
    ]

    contributions = get_candidate_contributions('cand456', 2024)

    assert contributions is not None
    assert len(contributions) == 3 # 2 from page 1, 1 from page 2
    assert contributions[0]['id'] == 'contrib1'
    assert contributions[2]['id'] == 'contrib3'

    assert mock_fetch.call_count == 3
    # Check params for each page call
    mock_fetch.assert_has_calls([
        call('contributions', {'candidate_id': 'cand456', 'year': 2024, 'per_page': 100, 'page': 1}),
        call('contributions', {'candidate_id': 'cand456', 'year': 2024, 'per_page': 100, 'page': 2}),
        call('contributions', {'candidate_id': 'cand456', 'year': 2024, 'per_page': 100, 'page': 3})
    ])

@patch('src.finance_collection.fetch_finance_data')
def test_get_contributions_api_failure(mock_fetch):
    """Test get_contributions when the first API call fails."""
    mock_fetch.return_value = None # Simulate fetch failure

    contributions = get_candidate_contributions('cand789', 2024)

    assert contributions is None
    mock_fetch.assert_called_once()

@patch('src.finance_collection.fetch_finance_data')
def test_get_contributions_no_contributions(mock_fetch, mock_contributions_response_empty):
    """Test get_contributions when the candidate has no contributions."""
    mock_fetch.return_value = mock_contributions_response_empty

    contributions = get_candidate_contributions('cand000', 2024)

    assert contributions == [] # Expect empty list, not None
    mock_fetch.assert_called_once()

# --- Tests for collect_finance_data (Orchestrator) ---

@patch('src.finance_collection.get_candidates_list')
@patch('src.finance_collection.get_candidate_contributions')
@patch('src.finance_collection.save_json')
@patch('pathlib.Path.mkdir') # Mock directory creation
def test_collect_finance_data_success(
    mock_mkdir, mock_save_json, mock_get_contributions, mock_get_candidates,
    mock_finance_paths, mock_candidates_response, mock_contributions_response_page1
):
    """Test the main finance data collection orchestration."""
    year = 2024
    state = 'ID'
    mock_get_candidates.return_value = mock_candidates_response['candidates']
    mock_get_contributions.return_value = mock_contributions_response_page1['contributions']

    result = collect_finance_data(year, state, mock_finance_paths)

    assert result is True

    # Check candidates list fetched and saved
    mock_get_candidates.assert_called_once_with(year, state)
    expected_candidates_path = mock_finance_paths['raw_finance'] / str(year) / f"candidates_{state}_{year}.json"
    mock_save_json.assert_any_call(mock_candidates_response['candidates'], expected_candidates_path)

    # Check contributions fetched and saved for each candidate
    assert mock_get_contributions.call_count == 2 # Called for cand1 and cand2
    mock_get_contributions.assert_has_calls([
        call('cand1', year),
        call('cand2', year)
    ], any_order=True)

    expected_contrib_path_cand1 = mock_finance_paths['raw_finance'] / str(year) / "contributions" / "cand1.json"
    expected_contrib_path_cand2 = mock_finance_paths['raw_finance'] / str(year) / "contributions" / "cand2.json"
    mock_save_json.assert_has_calls([
        call(mock_contributions_response_page1['contributions'], expected_contrib_path_cand1),
        call(mock_contributions_response_page1['contributions'], expected_contrib_path_cand2)
    ], any_order=True)

    # Check directories created
    assert mock_mkdir.call_count >= 2 # Year dir and contributions dir

@patch('src.finance_collection.get_candidates_list')
@patch('pathlib.Path.mkdir')
def test_collect_finance_data_no_candidates(mock_mkdir, mock_get_candidates, mock_finance_paths):
    """Test collect_finance_data when no candidates are returned."""
    mock_get_candidates.return_value = None # Simulate failure to get candidates

    result = collect_finance_data(2024, 'ID', mock_finance_paths)

    assert result is False
    mock_get_candidates.assert_called_once_with(2024, 'ID')
    # Ensure mkdir still called for initial setup
    mock_mkdir.assert_called()

@patch('src.finance_collection.get_candidates_list')
@patch('src.finance_collection.get_candidate_contributions')
@patch('src.finance_collection.save_json')
@patch('pathlib.Path.mkdir')
def test_collect_finance_data_contribution_failure(
    mock_mkdir, mock_save_json, mock_get_contributions, mock_get_candidates,
    mock_finance_paths, mock_candidates_response
):
    """Test collect_finance_data when fetching contributions fails for one candidate."""
    year = 2024
    state = 'ID'
    mock_get_candidates.return_value = mock_candidates_response['candidates']
    # Simulate success for cand1, failure for cand2
    mock_get_contributions.side_effect = [[{'id': 'c1'}], None]

    result = collect_finance_data(year, state, mock_finance_paths)

    assert result is True # Should still be considered True overall, but log errors
    mock_get_candidates.assert_called_once()
    assert mock_get_contributions.call_count == 2

    # Check save_json was called for candidates and cand1's contributions, but not cand2
    expected_candidates_path = mock_finance_paths['raw_finance'] / str(year) / f"candidates_{state}_{year}.json"
    expected_contrib_path_cand1 = mock_finance_paths['raw_finance'] / str(year) / "contributions" / "cand1.json"
    
    # Check calls to save_json (might need more specific assertion if order matters)
    calls = [call(mock_candidates_response['candidates'], expected_candidates_path),
             call([{'id': 'c1'}], expected_contrib_path_cand1)]
    mock_save_json.assert_has_calls(calls, any_order=True)
    # Verify it wasn't called for cand2
    for c in mock_save_json.call_args_list:
        assert "cand2.json" not in str(c.args[1])

# --- Tests for consolidate_finance_data ---

@patch('src.finance_collection.load_json')
@patch('src.finance_collection.convert_to_csv')
@patch('pathlib.Path.glob')
@patch('pathlib.Path.is_file')
def test_consolidate_finance_data_success(
    mock_is_file, mock_glob, mock_convert_csv, mock_load_json,
    mock_finance_paths, mock_candidates_response, mock_contributions_response_page1
):
    """Test successful consolidation of candidates and contributions."""
    year = 2024
    state = 'ID'
    years = [year]

    # Mock file structure and content
    mock_is_file.return_value = True
    raw_finance_path = mock_finance_paths['raw_finance']
    year_path = raw_finance_path / str(year)
    candidates_file_path = year_path / f"candidates_{state}_{year}.json"
    contrib_dir_path = year_path / "contributions"
    contrib_file_path1 = contrib_dir_path / "cand1.json"
    contrib_file_path2 = contrib_dir_path / "cand2.json"

    # Mock glob results
    def glob_side_effect(pattern):
        if pattern == f"candidates_{state}_*.json":
            return [candidates_file_path]
        elif pattern == "*.json" and Path(self.parent) == contrib_dir_path: # Check parent dir
             return [contrib_file_path1, contrib_file_path2]
        return []
    # Need to mock Path('...').glob directly if possible, or patch Path construction
    # Simplified: Mocking Path.glob generally for this test
    mock_glob.side_effect = glob_side_effect

    # Mock load_json results
    def load_json_side_effect(path):
        if path == candidates_file_path:
            return mock_candidates_response['candidates']
        elif path == contrib_file_path1:
            return mock_contributions_response_page1['contributions']
        elif path == contrib_file_path2:
            # Simulate slightly different contributions for cand2
            return [{'id': 'contrib10', 'donor': 'Donor X', 'amount': 500.00}]
        return None
    mock_load_json.side_effect = load_json_side_effect

    result = consolidate_finance_data(years, state, mock_finance_paths)

    assert result is True

    # Assert convert_to_csv called for candidates and contributions
    assert mock_convert_csv.call_count == 2

    # Check candidates CSV call
    expected_candidates_out_path = mock_finance_paths['processed'] / f"finance_candidates_{state}_{year}.csv"
    args_cand, _ = mock_convert_csv.call_args_list[0] # Assuming candidates is first
    assert args_cand[1] == expected_candidates_out_path
    assert len(args_cand[0]) == 2 # Number of candidates
    assert args_cand[0][0]['id'] == 'cand1'

    # Check contributions CSV call
    expected_contributions_out_path = mock_finance_paths['processed'] / f"finance_contributions_{state}_{year}.csv"
    args_contrib, _ = mock_convert_csv.call_args_list[1] # Assuming contributions is second
    assert args_contrib[1] == expected_contributions_out_path
    assert len(args_contrib[0]) == 3 # Total contributions (2 from cand1, 1 from cand2)
    assert args_contrib[0]['candidate_id'] == 'cand1' # Check added candidate_id
    assert args_contrib[2]['candidate_id'] == 'cand2'
    assert args_contrib[0]['id'] == 'contrib1'
    assert args_contrib[2]['id'] == 'contrib10'

@patch('src.finance_collection.load_json')
@patch('src.finance_collection.convert_to_csv')
@patch('pathlib.Path.glob')
@patch('pathlib.Path.is_file')
def test_consolidate_finance_data_no_files(mock_is_file, mock_glob, mock_convert_csv, mock_load_json, mock_finance_paths):
    """Test consolidation when raw files are missing."""
    mock_is_file.return_value = False # Simulate no files found
    mock_glob.return_value = []

    result = consolidate_finance_data([2024], 'ID', mock_finance_paths)

    assert result is True # Function might still succeed but produce empty CSVs
    mock_load_json.assert_not_called()
    # Check convert_to_csv is called with empty lists
    assert mock_convert_csv.call_count == 2
    args_cand, _ = mock_convert_csv.call_args_list[0]
    args_contrib, _ = mock_convert_csv.call_args_list[1]
    assert args_cand[0] == [] # Empty list for candidates
    assert args_contrib[0] == [] # Empty list for contributions

# --- Tests for main_finance_collection (Main Orchestrator) ---

@patch('src.finance_collection.collect_finance_data')
@patch('src.finance_collection.consolidate_finance_data')
@patch('src.finance_collection.pd.read_csv') # Used before calling match
@patch('src.finance_collection.match_finance_to_legislators')
@patch('src.finance_collection.convert_to_csv') # Used to save matched data
def test_main_finance_collection_all_steps(
    mock_convert_csv, mock_match, mock_read_csv, mock_consolidate, mock_collect,
    mock_finance_paths, mock_legislators_df # Use legislators fixture
):
    """Test main orchestrator running collect, consolidate, and match."""
    years = [2024]
    state = 'ID'
    mock_collect.return_value = True # Simulate successful collection
    mock_consolidate.return_value = True # Simulate successful consolidation

    # Mock read_csv to return candidates and legislators DFs
    mock_candidates_df = pd.DataFrame([{'id': 'cand1', 'name': 'Alice Legislator'}])
    mock_read_csv.side_effect = [
        mock_candidates_df, # First call reads candidates CSV
        mock_legislators_df # Second call reads legislators CSV
    ]

    # Mock match_finance_to_legislators to return a matched DF
    mock_matched_df = pd.DataFrame([{'id': 'cand1', 'legislator_id': 101}])
    mock_match.return_value = mock_matched_df

    result = main_finance_collection(years, state, mock_finance_paths, match_to_legislators=True)

    assert result is True
    mock_collect.assert_called_once_with(2024, state, mock_finance_paths)
    mock_consolidate.assert_called_once_with(years, state, mock_finance_paths)

    # Check read_csv calls
    expected_candidates_csv = mock_finance_paths['processed'] / f"finance_candidates_{state}_2024.csv"
    expected_legislators_csv = mock_finance_paths['processed'] / f"legislators_{state}.csv"
    mock_read_csv.assert_has_calls([
        call(expected_candidates_csv),
        call(expected_legislators_csv)
    ])

    # Check matching call
    pd.testing.assert_frame_equal(mock_match.call_args[0][0], mock_candidates_df)
    pd.testing.assert_frame_equal(mock_match.call_args[0][1], mock_legislators_df)
    assert mock_match.call_args[1]['threshold'] == 85 # Check default threshold

    # Check saving matched data
    expected_matched_csv = mock_finance_paths['processed'] / f"finance_candidates_matched_{state}_2024.csv"
    # Check the first arg (data) and second arg (path) passed to convert_to_csv
    pd.testing.assert_frame_equal(mock_convert_csv.call_args[0][0], mock_matched_df)
    assert mock_convert_csv.call_args[0][1] == expected_matched_csv

@patch('src.finance_collection.collect_finance_data')
@patch('src.finance_collection.consolidate_finance_data')
@patch('src.finance_collection.match_finance_to_legislators')
def test_main_finance_collection_skip_match(
    mock_match, mock_consolidate, mock_collect,
    mock_finance_paths
):
    """Test main orchestrator skipping the matching step."""
    years = [2024]
    state = 'ID'
    mock_collect.return_value = True
    mock_consolidate.return_value = True

    # Run with match_to_legislators=False
    result = main_finance_collection(years, state, mock_finance_paths, match_to_legislators=False)

    assert result is True
    mock_collect.assert_called_once()
    mock_consolidate.assert_called_once()
    mock_match.assert_not_called() # Ensure matching was skipped

@patch('src.finance_collection.collect_finance_data')
@patch('src.finance_collection.consolidate_finance_data')
def test_main_finance_collection_collect_fails(
    mock_consolidate, mock_collect,
    mock_finance_paths
):
    """Test main orchestrator when collection step fails."""
    years = [2024]
    state = 'ID'
    mock_collect.return_value = False # Simulate collection failure

    result = main_finance_collection(years, state, mock_finance_paths)

    assert result is False # Overall result should be False
    mock_collect.assert_called_once()
    mock_consolidate.assert_not_called() # Consolidation should be skipped

# --- Tests for match_finance_to_legislators ---

# Test basic exact and fuzzy matching
def test_match_finance_basic(sample_finance_df, mock_legislators_df):
    """Test basic exact and fuzzy matching works."""
    matched_df = match_finance_to_legislators(sample_finance_df, mock_legislators_df, score_cutoff=85)

    # Check Alice (Exact)
    alice_matches = matched_df[matched_df['candidate_name_orig'] == 'Alice Legislator']
    assert len(alice_matches) == 1
    assert alice_matches.iloc[0]['legislator_id'] == 101
    assert alice_matches.iloc[0]['match_score'] == 100

    # Check Bob (Fuzzy + Cleaning) - ' Bob Lawmaker ' vs 'Robert Lawmaker'
    bob_matches = matched_df[matched_df['candidate_name_orig'] == ' Bob Lawmaker ']
    assert len(bob_matches) == 1
    assert bob_matches.iloc[0]['legislator_id'] == 102
    assert bob_matches.iloc[0]['match_score'] > 85 # Fuzzy match score

    # Check Alice (Different Case)
    alice_case_matches = matched_df[matched_df['candidate_name_orig'] == 'ALICE LEGISLATOR']
    assert len(alice_case_matches) == 1
    assert alice_case_matches.iloc[0]['legislator_id'] == 101
    assert alice_case_matches.iloc[0]['match_score'] == 100 # Should be exact after cleaning

    # Check Charlie (Fuzzy) - 'Charles Committee' vs 'Charlie Committee'
    charlie_matches = matched_df[matched_df['candidate_name_orig'] == 'Charles Committee']
    assert len(charlie_matches) == 1
    assert charlie_matches.iloc[0]['legislator_id'] == 103
    assert charlie_matches.iloc[0]['match_score'] > 85

    # Check Unknown (No Match)
    unknown_matches = matched_df[matched_df['candidate_name_orig'] == 'Unknown Candidate']
    assert len(unknown_matches) == 1
    assert pd.isna(unknown_matches.iloc[0]['legislator_id'])
    assert unknown_matches.iloc[0]['match_score'] < 85 # Or check if NaN/0 depending on implementation

    # Check total rows
    assert len(matched_df) == len(sample_finance_df)
    assert 'legislator_id' in matched_df.columns
    assert 'match_score' in matched_df.columns
    assert 'candidate_name_orig' in matched_df.columns # Assumes function adds this

# Test score cutoff
def test_match_finance_score_cutoff(sample_finance_df, mock_legislators_df):
    """Test that the score_cutoff parameter works."""
    # High cutoff - only exact matches should pass
    matched_df_high_cutoff = match_finance_to_legislators(sample_finance_df, mock_legislators_df, score_cutoff=99)

    # Alice should match
    assert not pd.isna(matched_df_high_cutoff.loc[matched_df_high_cutoff['candidate_name_orig'] == 'Alice Legislator', 'legislator_id'].iloc[0])
    assert not pd.isna(matched_df_high_cutoff.loc[matched_df_high_cutoff['candidate_name_orig'] == 'ALICE LEGISLATOR', 'legislator_id'].iloc[0])

    # Bob and Charlie should *not* match (assuming fuzzy score < 99)
    assert pd.isna(matched_df_high_cutoff.loc[matched_df_high_cutoff['candidate_name_orig'] == ' Bob Lawmaker ', 'legislator_id'].iloc[0])
    assert pd.isna(matched_df_high_cutoff.loc[matched_df_high_cutoff['candidate_name_orig'] == 'Charles Committee', 'legislator_id'].iloc[0])

    # Unknown should not match
    assert pd.isna(matched_df_high_cutoff.loc[matched_df_high_cutoff['candidate_name_orig'] == 'Unknown Candidate', 'legislator_id'].iloc[0])

# Test empty inputs
def test_match_finance_empty_inputs(sample_finance_df, mock_legislators_df):
    """Test matching with empty finance data."""
    empty_finance_df = pd.DataFrame(columns=['candidate_name', 'amount', 'year'])
    matched_df = match_finance_to_legislators(empty_finance_df, mock_legislators_df)
    assert matched_df.empty
    assert 'legislator_id' in matched_df.columns # Check columns exist even if empty

    # Test with empty legislators
    # Need to call the fixture function to get the DataFrame
    sample_finance_df_copy = sample_finance_df # No need to call if fixture returns df directly
    empty_leg_df = pd.DataFrame(columns=['legislator_id', 'name', 'party'])
    matched_df_empty_leg = match_finance_to_legislators(sample_finance_df_copy, empty_leg_df)
    assert len(matched_df_empty_leg) == len(sample_finance_df_copy)
    assert pd.isna(matched_df_empty_leg['legislator_id']).all() # No IDs should be assigned
    assert (matched_df_empty_leg['match_score'] == 0).all() # Or NaN depending on implementation

# Test that clean_name is called (requires mocking)
@patch('src.finance_collection.clean_name') # Patch where clean_name is *used*
def test_match_finance_uses_clean_name(mock_clean, sample_finance_df, mock_legislators_df):
    """Verify that clean_name is called during matching."""
    # Give clean_name a simple side effect for testing
    mock_clean.side_effect = lambda x: x.strip().lower() if isinstance(x, str) else x

    match_finance_to_legislators(sample_finance_df, mock_legislators_df, score_cutoff=85)

    # Expected calls: once per finance name, once per legislator name
    expected_finance_calls = [call('Alice Legislator'), call(' Bob Lawmaker '), call('ALICE LEGISLATOR'), call('Unknown Candidate'), call('Charles Committee')]
    expected_leg_calls = [call('Alice Legislator'), call('Robert Lawmaker'), call('Charlie Committee')]

    # Check if clean_name was called with the expected names
    # Note: The order might vary, and it might be called multiple times per name depending on the implementation details (e.g., inside loops).
    # A robust check verifies *at least* these calls happened.
    # Collect actual calls to check against expected
    actual_calls = mock_clean.call_args_list
    # Check finance names were cleaned
    for expected_call in expected_finance_calls:
        assert expected_call in actual_calls
    # Check legislator names were cleaned
    for expected_call in expected_leg_calls:
        assert expected_call in actual_calls

    assert mock_clean.call_count >= len(expected_finance_calls) + len(expected_leg_calls)

# --- (End of file) ---