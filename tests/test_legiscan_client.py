"""Tests for LegiScan API client functionality."""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import pandas as pd

# Assuming fetch_api_data, get_session_list, and errors are now in legiscan_client
from src.legiscan_client import (
    fetch_api_data,
    get_session_list,
    APIRateLimitError,
    APIResourceNotFoundError
)

@pytest.fixture
def mock_session_list_response():
    """Sample session list API response."""
    return {
        "status": "OK",
        "sessions": [
            {
                "session_id": 1234,
                "state_id": 13,
                "year_start": 2023,
                "year_end": 2024,
                "prefile": 0,
                "sine_die": 0,
                "prior": 0,
                "special": 0,
                "session_tag": "Regular Session",
                "session_title": "2023-2024 Regular Session",
                "session_name": "2023-2024",
                "dataset_hash": "abc123"
            },
            {
                "session_id": 1233,
                "state_id": 13,
                "year_start": 2021,
                "year_end": 2022,
                "prefile": 0,
                "sine_die": 1,
                "prior": 1,
                "special": 0,
                "session_tag": "Regular Session",
                "session_title": "2021-2022 Regular Session",
                "session_name": "2021-2022",
                "dataset_hash": "def456"
            }
        ]
    }

@pytest.fixture
def mock_bill_response():
    """Sample bill API response. (Kept for potential future tests in this file)"""
    return {
        "status": "OK",
        "bill": {
            "bill_id": 5678,
            "change_hash": "xyz789",
            "session_id": 1234,
            "session": {"session_id": 1234, "year_start": 2023},
            "url": "https://legiscan.com/ID/bill/HB123/2023",
            "state_link": "https://legislature.idaho.gov/bills/HB123",
            "bill_number": "HB123",
            "bill_type": "B",
            "bill_type_id": "1",
            "body": "H",
            "body_id": 1,
            "title": "Test Bill",
            "description": "A bill for testing",
            "status": 1,
            "status_date": "2023-01-15",
            "subjects": [
                {"subject_id": 1, "subject_name": "Education"},
                {"subject_id": 2, "subject_name": "Taxes"}
            ],
            "texts": [
                {"doc_id": 1, "date": "2023-01-01", "type": "Introduced", "mime": "text/html"}
            ]
        }
    }

def test_fetch_api_data_success():
    """Test successful API data fetch."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "OK", "data": "test"}
    
    # Patch requests.get as it's called directly by fetch_api_data
    with patch('requests.get', return_value=mock_response):
        result = fetch_api_data('testOp', {'param': 'value'})
        assert result == {"status": "OK", "data": "test"}

def test_fetch_api_data_rate_limit():
    """Test rate limit handling."""
    mock_response = MagicMock()
    mock_response.status_code = 429
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(APIRateLimitError):
            fetch_api_data('testOp', {'param': 'value'})

def test_fetch_api_data_not_found():
    """Test resource not found handling."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "ERROR",
        "alert": {"message": "Bill not found"} # Example error message
    }
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(APIResourceNotFoundError):
            fetch_api_data('testOp', {'param': 'value'})

def test_get_session_list(mock_session_list_response):
    """Test session list retrieval."""
    # Patch fetch_api_data within the legiscan_client module
    with patch('src.legiscan_client.fetch_api_data', return_value=mock_session_list_response):
        sessions = get_session_list('ID', [2023, 2024])
        assert len(sessions) == 1
        assert sessions[0]['session_id'] == 1234
        assert sessions[0]['year_start'] == 2023

def test_get_session_list_empty():
    """Test handling of empty session list."""
    with patch('src.legiscan_client.fetch_api_data', return_value={"status": "OK", "sessions": []}):
        sessions = get_session_list('ID', [2023, 2024])
        assert len(sessions) == 0

def test_get_session_list_error():
    """Test handling of API error when fetch_api_data returns None."""
    with patch('src.legiscan_client.fetch_api_data', return_value=None):
        sessions = get_session_list('ID', [2023, 2024])
        assert len(sessions) == 0 # Expect empty list if fetch fails

def test_get_session_list_year_filter(mock_session_list_response):
    """Test session list year filtering."""
    with patch('src.legiscan_client.fetch_api_data', return_value=mock_session_list_response):
        sessions = get_session_list('ID', [2021, 2022])
        assert len(sessions) == 1
        assert sessions[0]['session_id'] == 1233
        assert sessions[0]['year_start'] == 2021 