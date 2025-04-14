"""Tests for the data collection orchestration functions in data_collection.py."""

import pytest
import argparse
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from datetime import datetime # Import datetime

# Import the function to test
from src.data_collection import run_collection

# Mock paths dictionary returned by setup_project_paths
@pytest.fixture
def mock_paths():
    """Fixture for mock project paths."""
    return {
        'base': Path('/fake/data'),
        'raw_sessions': Path('/fake/data/raw/sessions'),
        'raw_legislators': Path('/fake/data/raw/legislators'),
        'raw_committees': Path('/fake/data/raw/committees'),
        'raw_bills': Path('/fake/data/raw/bills'),
        'raw_votes': Path('/fake/data/raw/votes'),
        'raw_sponsors': Path('/fake/data/raw/sponsors'),
        'raw_committee_memberships': Path('/fake/data/raw/committee_memberships'),
        'raw_texts': Path('/fake/data/raw/texts'),
        'raw_amendments': Path('/fake/data/raw/amendments'),
        'raw_supplements': Path('/fake/data/raw/supplements'),
        'processed': Path('/fake/data/processed'),
        'artifacts': Path('/fake/data/artifacts'),
        'logs': Path('/fake/data/logs'),
        'dataset_hashes_file': Path('/fake/data/artifacts/legiscan_dataset_hashes.json') # Added this key
    }

# Mock session data returned by get_session_list
@pytest.fixture
def mock_sessions():
    """Fixture for mock session data."""
    return [
        {'session_id': 1, 'year_start': 2023, 'session_name': 'Session 2023'},
        {'session_id': 2, 'year_start': 2024, 'session_name': 'Session 2024'}
    ]

# Mock args object returned by argparse
@pytest.fixture
def mock_args_defaults():
    """Fixture for default mock command-line arguments."""
    args = argparse.Namespace()
    args.state = 'ID'
    args.start_year = 2023
    args.end_year = 2024
    args.run = None # Default run all steps applicable
    args.fetch_texts = False
    args.fetch_amendments = False
    args.fetch_supplements = False
    args.force_dataset_download = False
    args.data_dir = None
    # Add any other relevant default arguments from the script's parser
    return args

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
@patch('datetime.datetime') # Patch datetime to control current year
def test_run_collection_defaults(
    mock_datetime, mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths, mock_sessions
):
    """Test run_collection runs all steps by default for ID state."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_load_hashes.return_value = {} # Start with empty hashes
    mock_get_sessions.return_value = mock_sessions
    # Define the expected path returned by scraping, needed for matching
    mock_scrape_return_path = mock_paths['raw_committee_memberships'] / '2024' / 'scraped_memberships_raw_ID_2024.json'
    mock_scrape.return_value = str(mock_scrape_return_path) # Return path as string
    # Mock current year for consistent testing of scraping/matching
    mock_datetime.now.return_value.year = 2024

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert core setup calls
    mock_setup_paths.assert_called_once_with(base_dir=None)
    mock_load_hashes.assert_called_once_with(mock_paths)

    # Assert API calls are made (since args.run is None)
    mock_get_sessions.assert_called_once_with('ID', list(range(2023, 2025)))
    mock_collect_legislators.assert_called_once_with(mock_sessions, mock_paths)
    assert mock_collect_committees.call_count == 2 # Called per session
    mock_collect_committees.assert_has_calls([
        call(mock_sessions[0], mock_paths),
        call(mock_sessions[1], mock_paths)
    ], any_order=True) # Order might vary depending on internal logic

    assert mock_collect_bills.call_count == 2 # Called per session
    expected_fetch_flags = {'fetch_texts': False, 'fetch_amendments': False, 'fetch_supplements': False}
    # Note: The third argument to collect_bills_votes_sponsors is the dataset_hashes dict
    mock_collect_bills.assert_has_calls([
        call(mock_sessions[0], mock_paths, {}, fetch_flags=expected_fetch_flags, force_download=False),
        call(mock_sessions[1], mock_paths, {}, fetch_flags=expected_fetch_flags, force_download=False)
    ], any_order=True)

    # Assert ID-specific scraping and matching calls (for current year 2024)
    current_year = 2024 # Based on mocked datetime
    mock_scrape.assert_called_once_with('ID', current_year, mock_paths)
    mock_match.assert_called_once_with('ID', current_year, mock_paths, str(mock_scrape_return_path))

    # Assert consolidation calls are made (since args.run is None)
    assert mock_consolidate.call_count == 5 # committees, bills, votes, sponsors, memberships
    years_to_consolidate = list(range(2023, 2025))
    # Check calls with specific arguments - be precise about keys expected by consolidate_yearly_data
    mock_consolidate.assert_has_calls([
        call(data_type='committees', years=years_to_consolidate, paths=mock_paths, state_abbr='ID', columns=None), # Add default columns=None if applicable
        call(data_type='bills', years=years_to_consolidate, paths=mock_paths, state_abbr='ID', columns=None),
        call(data_type='votes', years=years_to_consolidate, paths=mock_paths, state_abbr='ID', columns=None),
        call(data_type='sponsors', years=years_to_consolidate, paths=mock_paths, state_abbr='ID', columns=None),
        call(data_type='committee_memberships', paths=mock_paths, state_abbr='ID', years=None, columns=None) # Membership consolidation might differ
    ], any_order=True)

    # Assert saving hashes
    mock_save_hashes.assert_called_once_with({}, mock_paths) # Saved potentially updated hashes

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
@patch('datetime.datetime') # Patch datetime
def test_run_collection_skip_api(
    mock_datetime, mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths # Use default args fixture
):
    """Test run_collection skips API calls with --run specified without 'api'."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_args_defaults.run = ['scrape_members', 'match_members', 'consolidate'] # Simulate skipping API
    mock_scrape_return_path = mock_paths['raw_committee_memberships'] / '2024' / 'scraped_memberships_raw_ID_2024.json'
    mock_scrape.return_value = str(mock_scrape_return_path)
    mock_datetime.now.return_value.year = 2024
    mock_load_hashes.return_value = {} # Still load hashes

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert API calls NOT made
    mock_get_sessions.assert_not_called()
    mock_collect_legislators.assert_not_called()
    mock_collect_committees.assert_not_called()
    mock_collect_bills.assert_not_called()

    # Assert other calls relevant to the --run argument ARE made
    mock_setup_paths.assert_called_once()
    mock_load_hashes.assert_called_once() # Hashes still loaded
    mock_scrape.assert_called_once()
    mock_match.assert_called_once()
    assert mock_consolidate.call_count == 5 # Consolidation runs
    mock_save_hashes.assert_called_once() # Hashes saved even if API skipped

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members') # Still need to mock scrape/match even if not checked
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data') # Still need to mock consolidate
@patch('src.data_collection._save_dataset_hashes')
@patch('datetime.datetime') # Patch datetime
def test_run_collection_fetch_flags(
    mock_datetime, mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths, mock_sessions
):
    """Test run_collection passes fetch flags correctly to collect_bills_votes_sponsors."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_load_hashes.return_value = {}
    mock_get_sessions.return_value = mock_sessions
    mock_args_defaults.fetch_texts = True
    mock_args_defaults.fetch_amendments = True
    # Keep supplements false (default)
    mock_datetime.now.return_value.year = 2024 # Mock year for scrape/match

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert fetch flags passed to collect_bills_votes_sponsors
    expected_fetch_flags = {'fetch_texts': True, 'fetch_amendments': True, 'fetch_supplements': False}
    assert mock_collect_bills.call_count == 2
    mock_collect_bills.assert_has_calls([
        call(mock_sessions[0], mock_paths, {}, fetch_flags=expected_fetch_flags, force_download=False),
        call(mock_sessions[1], mock_paths, {}, fetch_flags=expected_fetch_flags, force_download=False)
    ], any_order=True)

    # We don't need to assert every other call here, focus is on fetch flags
    mock_get_sessions.assert_called_once() # Verify API part ran

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
@patch('datetime.datetime') # Patch datetime
def test_run_collection_run_specific(
    mock_datetime, mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths, mock_sessions
):
    """Test run_collection runs only specified steps with --run."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_load_hashes.return_value = {}
    mock_get_sessions.return_value = mock_sessions
    mock_args_defaults.run = ['api', 'consolidate'] # Only run API and consolidation
    mock_datetime.now.return_value.year = 2024 # Mock year even though scrape/match skipped

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert API calls ARE made
    mock_get_sessions.assert_called_once()
    mock_collect_legislators.assert_called_once()
    assert mock_collect_committees.call_count == 2
    assert mock_collect_bills.call_count == 2

    # Assert scraping/matching NOT called as they are not in args.run
    mock_scrape.assert_not_called()
    mock_match.assert_not_called()

    # Assert consolidation IS called as it is in args.run
    assert mock_consolidate.call_count == 5

    # Assert saving hashes IS called
    mock_save_hashes.assert_called_once()

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
def test_run_collection_non_id_state(
    mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths, mock_sessions
):
    """Test run_collection skips scrape/match for non-ID states."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_load_hashes.return_value = {}
    mock_get_sessions.return_value = mock_sessions
    mock_args_defaults.state = 'CA' # Change state to non-ID

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert API calls ARE made
    mock_get_sessions.assert_called_once_with('CA', list(range(2023, 2025)))
    mock_collect_legislators.assert_called_once()
    assert mock_collect_bills.call_count == 2

    # Assert scraping/matching NOT called for non-ID state
    mock_scrape.assert_not_called()
    mock_match.assert_not_called()

    # Assert consolidation IS called (should consolidate API data)
    # Should be 4 types: committees, bills, votes, sponsors
    assert mock_consolidate.call_count == 4
    years_to_consolidate = list(range(2023, 2025))
    mock_consolidate.assert_has_calls([
        call(data_type='committees', years=years_to_consolidate, paths=mock_paths, state_abbr='CA', columns=None),
        call(data_type='bills', years=years_to_consolidate, paths=mock_paths, state_abbr='CA', columns=None),
        call(data_type='votes', years=years_to_consolidate, paths=mock_paths, state_abbr='CA', columns=None),
        call(data_type='sponsors', years=years_to_consolidate, paths=mock_paths, state_abbr='CA', columns=None),
    ], any_order=True)

    # Assert saving hashes
    mock_save_hashes.assert_called_once()

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
def test_run_collection_force_download(
    mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths, mock_sessions
):
    """Test run_collection passes force_download flag correctly."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_load_hashes.return_value = {} # Assume no prior hashes
    mock_get_sessions.return_value = mock_sessions
    mock_args_defaults.force_dataset_download = True # Set force download flag
    mock_datetime.now.return_value.year = 2024 # Mock year for scrape/match

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert force_download flag passed to collect_bills_votes_sponsors
    expected_fetch_flags = {'fetch_texts': False, 'fetch_amendments': False, 'fetch_supplements': False}
    assert mock_collect_bills.call_count == 2
    mock_collect_bills.assert_has_calls([
        call(mock_sessions[0], mock_paths, {}, fetch_flags=expected_fetch_flags, force_download=True),
        call(mock_sessions[1], mock_paths, {}, fetch_flags=expected_fetch_flags, force_download=True)
    ], any_order=True)

    # Verify other main steps ran
    mock_get_sessions.assert_called_once()
    mock_scrape.assert_called_once() # Ran for ID state

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
def test_run_collection_data_dir(
    mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths # No mock_sessions needed as we just check setup_paths
):
    """Test run_collection passes data_dir to setup_project_paths."""
    # --- Setup Mocks ---
    custom_data_dir = '/custom/path/data'
    mock_args_defaults.data_dir = custom_data_dir
    # We don't need setup_paths to return anything specific for this test
    mock_setup_paths.return_value = mock_paths # Still return something valid
    mock_get_sessions.return_value = [] # Prevent further calls

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert setup_project_paths called with the custom directory
    mock_setup_paths.assert_called_once_with(base_dir=custom_data_dir)

@patch('src.data_collection.setup_project_paths')
@patch('src.data_collection._load_dataset_hashes')
@patch('src.data_collection.get_session_list')
@patch('src.data_collection.collect_legislators')
@patch('src.data_collection.collect_committee_definitions')
@patch('src.data_collection.collect_bills_votes_sponsors')
@patch('src.data_collection.scrape_committee_members')
@patch('src.data_collection.match_committee_members')
@patch('src.data_collection.consolidate_yearly_data')
@patch('src.data_collection._save_dataset_hashes')
def test_run_collection_no_sessions(
    mock_save_hashes, mock_consolidate, mock_match, mock_scrape,
    mock_collect_bills, mock_collect_committees, mock_collect_legislators,
    mock_get_sessions, mock_load_hashes, mock_setup_paths,
    mock_args_defaults, mock_paths
):
    """Test run_collection handles case where no sessions are found."""
    # --- Setup Mocks ---
    mock_setup_paths.return_value = mock_paths
    mock_load_hashes.return_value = {}
    mock_get_sessions.return_value = [] # Simulate no sessions found

    # --- Execute Function ---
    run_collection(mock_args_defaults)

    # --- Assertions ---
    # Assert core setup calls happened
    mock_setup_paths.assert_called_once()
    mock_load_hashes.assert_called_once()
    mock_get_sessions.assert_called_once()

    # Assert subsequent API calls depending on sessions are NOT made
    mock_collect_legislators.assert_not_called()
    mock_collect_committees.assert_not_called()
    mock_collect_bills.assert_not_called()

    # Assert scraping/matching (which depend on current year, not sessions) ARE still called for ID
    # Need to mock datetime here too for consistency
    with patch('datetime.datetime') as mock_dt:
        mock_dt.now.return_value.year = 2024
        run_collection(mock_args_defaults) # Re-run inside patch if needed, or ensure patch covers initial run
        mock_scrape.assert_called_once_with('ID', 2024, mock_paths)
        mock_match.assert_called_once() # Assuming scrape returns a valid path

    # Assert consolidation still called (might consolidate empty lists or just memberships)
    assert mock_consolidate.call_count >= 1 # At least membership consolidation should happen

    # Assert saving hashes happens
    mock_save_hashes.assert_called_once()
