"""Tests for the news data collection module (news_collection.py)."""

import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
import pandas as pd
import requests # May be needed for mocking responses in other tests
from datetime import datetime, timedelta # Import for mocking

# Import functions and classes to test from src.news_collection
# Assume these are the main components based on file structure analysis
from src.news_collection import (
    fetch_news_data,
    search_news_articles,
    generate_queries_for_bill,
    collect_news_for_bill,
    collect_news_for_bills,
    extract_article_text,
    fetch_full_text_for_articles,
    process_and_enrich_news_data,
    main_news_collection,
    ensure_nltk_resources,
    NewsAPIError,
    NewsRateLimitError
)

# Mock paths dictionary (adjust keys as needed for news data)
@pytest.fixture
def mock_news_paths():
    """Fixture for mock project paths relevant to news."""
    base = Path('/fake/data')
    raw_news = base / 'raw' / 'news'
    return {
        'base': base,
        'raw_news': raw_news,
        'raw_bills': base / 'raw' / 'bills', # Assume needed for bill data
        'processed': base / 'processed',
        'logs': base / 'logs',
    }

# Mock Bill Data
@pytest.fixture
def mock_bill_data_basic():
    """Fixture for basic mock bill data."""
    return {
        'bill_id': 123,
        'bill_number': 'HB 101',
        'title': 'An Act Relating to Renewable Energy Standards',
        'description': 'This bill increases the renewable portfolio standard for electric utilities.',
        'session_id': 1,
        'state_link': 'http://example.com/hb101',
        'status_date': '2024-01-15',
        'status': 1 # Example status code
    }

@pytest.fixture
def mock_bill_data_minimal():
    """Fixture for minimal mock bill data (missing title/desc)."""
    return {
        'bill_id': 456,
        'bill_number': 'SB 50',
        'session_id': 2
    }

# Mock News API Responses (Placeholders for now)
@pytest.fixture
def mock_news_search_success():
    """Fixture for a successful news search API response."""
    return {
        'status': 'ok',
        'totalResults': 2,
        'articles': [
            {'source': {'id': 'source1', 'name': 'News Source A'}, 'title': 'HB 101 Discussed', 'url': 'http://news.com/a', 'publishedAt': '2024-01-20T10:00:00Z', 'content': 'Discussion about HB 101...'},
            {'source': {'id': 'source2', 'name': 'News Source B'}, 'title': 'Legislature Debates Energy', 'url': 'http://news.com/b', 'publishedAt': '2024-01-21T11:00:00Z', 'content': 'Debate on renewable energy...'}
        ]
    }

@pytest.fixture
def mock_news_search_empty():
    """Fixture for an empty news search API response."""
    return {
        'status': 'ok',
        'totalResults': 0,
        'articles': []
    }

# --- Tests for generate_queries_for_bill ---

# Mock NLTK resources check to avoid actual downloads during tests
@patch('src.news_collection.ensure_nltk_resources', MagicMock())
# Mock NLTK functions if needed within the test function scope
@patch('src.news_collection.word_tokenize', MagicMock(side_effect=lambda x: x.lower().split()))
@patch('src.news_collection.stopwords.words', MagicMock(return_value=['an', 'act', 'to', 'the', 'of', 'for']))
@patch('src.news_collection.sent_tokenize', MagicMock(side_effect=lambda x: [f'{x}.'])) # Simple sentence split
def test_generate_queries_basic(mock_bill_data_basic):
    """Test generating queries for a bill with title and description."""
    state = 'ID'
    queries = generate_queries_for_bill(mock_bill_data_basic, state)

    assert isinstance(queries, list)
    # Check bill number queries
    assert f"{state} {mock_bill_data_basic['bill_number']}" in queries
    assert f"{state} legislature {mock_bill_data_basic['bill_number']}" in queries
    # Check keyword query (mocked tokenization/stopwords)
    # Expected keywords: 'relating', 'renewable', 'energy', 'standards'
    expected_keyword_query = f"{state} legislature relating renewable energy standards"
    assert expected_keyword_query in queries
    # Check description query (mocked sentence/word tokenization)
    # Expected: "ID This bill increases renewable portfolio standard" (first 8 words)
    expected_desc_query = f"{state} this bill increases renewable portfolio standard for electric"
    assert expected_desc_query in queries

@patch('src.news_collection.ensure_nltk_resources', MagicMock())
@patch('src.news_collection.word_tokenize', MagicMock(side_effect=lambda x: x.lower().split()))
@patch('src.news_collection.stopwords.words', MagicMock(return_value=['a', 'the']))
@patch('src.news_collection.sent_tokenize', MagicMock(side_effect=lambda x: [f'{x}.']))
def test_generate_queries_minimal(mock_bill_data_minimal):
    """Test generating queries with minimal bill info (only number)."""
    state = 'WY'
    queries = generate_queries_for_bill(mock_bill_data_minimal, state)

    assert isinstance(queries, list)
    assert len(queries) == 2 # Should only have bill number queries
    assert f"{state} {mock_bill_data_minimal['bill_number']}" in queries
    assert f"{state} legislature {mock_bill_data_minimal['bill_number']}" in queries

@patch('src.news_collection.ensure_nltk_resources', MagicMock())
def test_generate_queries_no_number():
    """Test generating queries when bill number is missing."""
    bill_data = {
        'bill_id': 789,
        'title': 'Simple Topic',
        'description': 'A short description.'
    }
    state = 'NV'
    queries = generate_queries_for_bill(bill_data, state)
    
    assert isinstance(queries, list)
    # Bill number queries should be absent
    for query in queries:
        assert 'bill_number' not in query.lower() # Approximation
        
    # Check that other queries (keyword/desc) might still be generated
    assert len(queries) > 0 

@patch('src.news_collection.ensure_nltk_resources', MagicMock())
@patch('src.news_collection.word_tokenize', MagicMock(side_effect=Exception("NLTK Error"))) # Simulate NLTK failure
def test_generate_queries_nltk_error(mock_bill_data_basic):
    """Test query generation resilience if NLTK fails."""
    state = 'ID'
    # Expect it to log a warning but still produce bill number queries
    queries = generate_queries_for_bill(mock_bill_data_basic, state)
    
    assert isinstance(queries, list)
    # Should still contain bill number queries
    assert f"{state} {mock_bill_data_basic['bill_number']}" in queries
    assert f"{state} legislature {mock_bill_data_basic['bill_number']}" in queries
    # Keyword/description queries might be absent due to the error
    assert len(queries) == 2 # Assuming failure prevents other queries


# --- Tests for fetch_news_data ---

@patch('src.news_collection.requests.get')
@patch('src.news_collection.time.sleep') # Mock sleep for speed
@patch('src.news_collection.NEWS_API_URL', 'https://api.example-news.com')
@patch('src.news_collection.NEWS_API_KEY', 'fake_news_key')
def test_fetch_news_data_success(mock_sleep, mock_get):
    """Test successful fetching of news API data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "ok", "articles": [{"title": "Test Article"}]}
    mock_get.return_value = mock_response

    endpoint = "everything"
    params = {"q": "test"}
    result = fetch_news_data(endpoint, params)

    assert result == {"status": "ok", "articles": [{"title": "Test Article"}]}
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert args[0] == f"https://api.example-news.com/{endpoint}"
    expected_params = params.copy()
    expected_params['apiKey'] = 'fake_news_key'
    assert kwargs['params'] == expected_params
    mock_sleep.assert_called_once() # Should sleep once before the request

@patch('src.news_collection.requests.get')
@patch('src.news_collection.time.sleep')
@patch('src.news_collection.NEWS_API_URL', 'https://api.example-news.com')
@patch('src.news_collection.NEWS_API_KEY', 'fake_news_key')
def test_fetch_news_data_api_error_response(mock_sleep, mock_get):
    """Test handling of API error response (status: error)."""
    mock_response = MagicMock()
    mock_response.status_code = 200 # API might return 200 but indicate error in JSON
    mock_response.json.return_value = {"status": "error", "code": "apiKeyInvalid", "message": "Your API key is invalid."}
    mock_get.return_value = mock_response

    result = fetch_news_data("everything", {"q": "test"})

    assert result is None # Expect None on API-reported error
    mock_get.assert_called_once()
    mock_sleep.assert_called_once()

@patch('src.news_collection.requests.get')
@patch('src.news_collection.time.sleep')
@patch('src.news_collection.NEWS_API_URL', 'https://api.example-news.com')
@patch('src.news_collection.NEWS_API_KEY', 'fake_news_key')
def test_fetch_news_data_rate_limit_retry(mock_sleep, mock_get):
    """Test retry logic upon hitting NewsRateLimitError (429)."""
    mock_rate_limit_response = MagicMock()
    mock_rate_limit_response.status_code = 429
    # No need for raise_for_status mock here as 429 is handled specifically

    mock_success_response = MagicMock()
    mock_success_response.status_code = 200
    mock_success_response.json.return_value = {"status": "ok", "articles": ["Success after retry"]}

    # Simulate 429 then success
    mock_get.side_effect = [mock_rate_limit_response, mock_success_response]

    result = fetch_news_data("everything", {"q": "retry_test"})

    assert result == {"status": "ok", "articles": ["Success after retry"]}
    assert mock_get.call_count == 2
    # One sleep before first attempt, one sleep for retry backoff
    assert mock_sleep.call_count == 2

@patch('src.news_collection.requests.get')
@patch('src.news_collection.time.sleep')
@patch('src.news_collection.NEWS_API_URL', 'https://api.example-news.com')
@patch('src.news_collection.NEWS_API_KEY', 'fake_news_key')
@patch('src.news_collection.NEWS_MAX_RETRIES', 2) # Reduce retries for faster test
def test_fetch_news_data_http_error_retry_fail(mock_sleep, mock_get):
    """Test retry logic for non-429 HTTP errors, failing after retries."""
    mock_error_response = MagicMock()
    mock_error_response.status_code = 500
    mock_error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error", response=mock_error_response)
    mock_get.return_value = mock_error_response

    # Expect RequestException after retries fail
    with pytest.raises(requests.exceptions.RequestException):
        fetch_news_data("everything", {"q": "http_error"})

    assert mock_get.call_count == 2 # Initial attempt + 1 retry = NEWS_MAX_RETRIES
    # One sleep before each attempt + one sleep for retry backoff
    assert mock_sleep.call_count == 2 + (2 - 1)


@patch('src.news_collection.NEWS_API_KEY', None) # Simulate missing key
@patch('src.news_collection.requests.get') # Need to patch get so it's not called
@patch('src.news_collection.time.sleep') # Need to patch sleep so it's not called
def test_fetch_news_data_no_api_key(mock_sleep, mock_get):
    """Test that fetch_news_data returns None and logs error if API key is missing."""
    result = fetch_news_data("everything", {"q": "no_key"})

    assert result is None
    mock_get.assert_not_called() # Should not attempt API call
    mock_sleep.assert_not_called() # Should not sleep


# --- Tests for search_news_articles ---

@patch('src.news_collection.fetch_news_data')
def test_search_news_articles_success(mock_fetch, mock_news_search_success):
    """Test a successful search_news_articles call."""
    mock_fetch.return_value = mock_news_search_success
    query = "Idaho budget"
    
    result = search_news_articles(query)

    assert result == mock_news_search_success
    mock_fetch.assert_called_once()
    args, kwargs = mock_fetch.call_args
    assert args[0] == 'everything' # Check endpoint
    expected_params = {
        'q': query,
        'language': 'en', # Default language
        'pageSize': 100, # Default page size (capped)
        'page': 1, # Default page
        'sortBy': 'relevancy'
    }
    assert args[1] == expected_params

@patch('src.news_collection.fetch_news_data')
def test_search_news_articles_with_options(mock_fetch, mock_news_search_success):
    """Test search_news_articles with specific date, language, and page options."""
    mock_fetch.return_value = mock_news_search_success
    query = "healthcare legislation"
    from_d = "2024-03-01"
    to_d = "2024-03-15"
    lang = "es"
    page_s = 50
    pg = 2

    result = search_news_articles(query, from_date=from_d, to_date=to_d, language=lang, page_size=page_s, page=pg)

    assert result == mock_news_search_success
    mock_fetch.assert_called_once()
    args, kwargs = mock_fetch.call_args
    assert args[0] == 'everything'
    expected_params = {
        'q': query,
        'language': lang,
        'pageSize': page_s,
        'page': pg,
        'sortBy': 'relevancy',
        'from': from_d, # Check date keys
        'to': to_d
    }
    assert args[1] == expected_params

@patch('src.news_collection.fetch_news_data')
def test_search_news_articles_page_size_limit(mock_fetch, mock_news_search_success):
    """Test that pageSize is capped at 100."""
    mock_fetch.return_value = mock_news_search_success
    query = "education"
    
    search_news_articles(query, page_size=150) # Request more than max

    mock_fetch.assert_called_once()
    args, kwargs = mock_fetch.call_args
    assert args[1]['pageSize'] == 100 # Assert it was capped

@patch('src.news_collection.fetch_news_data')
def test_search_news_articles_fetch_fails_none(mock_fetch):
    """Test search_news_articles when fetch_news_data returns None."""
    mock_fetch.return_value = None
    query = "infrastructure bill"
    
    result = search_news_articles(query)

    assert result is None
    mock_fetch.assert_called_once_with('everything', {
        'q': query, 'language': 'en', 'pageSize': 100, 'page': 1, 'sortBy': 'relevancy'
    })

@patch('src.news_collection.fetch_news_data', side_effect=requests.exceptions.Timeout("API Timeout"))
def test_search_news_articles_fetch_raises(mock_fetch):
    """Test search_news_articles when fetch_news_data raises an exception."""
    query = "tax policy"
    
    # Expect the function to catch the exception and return None
    result = search_news_articles(query)

    assert result is None
    mock_fetch.assert_called_once() # Verify fetch was still called

# --- Tests for collect_news_for_bill ---

@patch('src.news_collection.save_json')
@patch('src.news_collection.search_news_articles')
@patch('src.news_collection.generate_queries_for_bill')
@patch('src.news_collection.datetime') # Mock datetime to control date calculations
@patch('pathlib.Path.mkdir') # Mock directory creation
def test_collect_news_for_bill_success(
    mock_mkdir, mock_dt, mock_generate_queries, mock_search, mock_save,
    mock_bill_data_basic, mock_news_paths, mock_news_search_success
):
    """Test successful collection of news for a single bill."""
    state = 'ID'
    output_dir = mock_news_paths['raw_news'] / state / 'bills'
    bill_id = mock_bill_data_basic['bill_id']
    expected_output_path = output_dir / f"{bill_id}_news.json"

    # Mock generated queries
    queries = [f"{state} HB 101", f"{state} legislature renewable energy"]
    mock_generate_queries.return_value = queries

    # Mock search results (return success for first query, empty for second)
    # Add unique URLs to the mock success data
    mock_response1 = mock_news_search_success.copy()
    mock_response1['articles'] = [
        {'title': 'Article A', 'url': 'http://news.com/a', 'publishedAt': '2024-01-20T10:00:00Z', 'content': '...'},
        {'title': 'Article B', 'url': 'http://news.com/b', 'publishedAt': '2024-01-21T11:00:00Z', 'content': '...'}
    ]
    mock_response2 = {'status': 'ok', 'totalResults': 0, 'articles': []} # Empty response
    mock_search.side_effect = [mock_response1, mock_response2]

    # Mock datetime and date calculations
    # Ensure the mock strptime handles the expected format
    mock_status_date_str = mock_bill_data_basic['status_date'] # '2024-01-15'
    mock_status_date = datetime(2024, 1, 15)
    # Configure the mock to return the datetime object when strptime is called with the specific string and format
    mock_dt.strptime.side_effect = lambda d, fmt: mock_status_date if d == mock_status_date_str and fmt == '%Y-%m-%d' else datetime.strptime(d, fmt)
    mock_dt.now.return_value = datetime(2024, 5, 15) # Mock 'now' if needed elsewhere
    mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw) # Allow creating other datetimes

    from_date_dt = mock_status_date - timedelta(days=30) # Default days_before=30
    to_date_dt = mock_status_date + timedelta(days=60) # Default days_after=60
    from_date_str = from_date_dt.strftime('%Y-%m-%d')
    to_date_str = to_date_dt.strftime('%Y-%m-%d')

    # Call the function
    result = collect_news_for_bill(mock_bill_data_basic, state, mock_news_paths['raw_news'])

    # Assertions
    assert result is True
    mock_generate_queries.assert_called_once_with(mock_bill_data_basic, state)
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_dt.strptime.assert_called_once_with(mock_status_date_str, '%Y-%m-%d') # Verify strptime was called correctly

    # Check search calls
    assert mock_search.call_count == len(queries)
    mock_search.assert_has_calls([
        call(query=queries[0], from_date=from_date_str, to_date=to_date_str, language='en', page_size=100, page=1),
        call(query=queries[1], from_date=from_date_str, to_date=to_date_str, language='en', page_size=100, page=1),
    ], any_order=True) # Order might depend on query list order

    # Check save call
    mock_save.assert_called_once()
    args_save, kwargs_save = mock_save.call_args
    saved_path = args_save[0]
    saved_data = args_save[1]
    assert saved_path == expected_output_path
    assert isinstance(saved_data, list)
    # Check that only unique articles from mock_response1 were saved
    assert len(saved_data) == len(mock_response1['articles'])
    saved_urls = {article['url'] for article in saved_data}
    expected_urls = {article['url'] for article in mock_response1['articles']}
    assert saved_urls == expected_urls
    # Check that bill_id was added to each article
    for article in saved_data:
        assert article['associated_bill_id'] == bill_id

@patch('src.news_collection.save_json')
@patch('src.news_collection.search_news_articles')
@patch('src.news_collection.generate_queries_for_bill')
@patch('pathlib.Path.mkdir')
def test_collect_news_for_bill_no_queries(
    mock_mkdir, mock_generate_queries, mock_search, mock_save,
    mock_bill_data_basic, mock_news_paths
):
    """Test collect_news_for_bill when no queries are generated."""
    mock_generate_queries.return_value = [] # No queries

    result = collect_news_for_bill(mock_bill_data_basic, 'ID', mock_news_paths['raw_news'])

    assert result is False # Should fail if no queries
    mock_generate_queries.assert_called_once()
    mock_search.assert_not_called()
    mock_save.assert_not_called()
    mock_mkdir.assert_called_once() # Directory might still be created

@patch('src.news_collection.save_json')
@patch('src.news_collection.search_news_articles')
@patch('src.news_collection.generate_queries_for_bill')
@patch('src.news_collection.datetime')
@patch('pathlib.Path.mkdir')
def test_collect_news_for_bill_search_fails(
    mock_mkdir, mock_dt, mock_generate_queries, mock_search, mock_save,
    mock_bill_data_basic, mock_news_paths
):
    """Test collect_news_for_bill when search_news_articles returns None."""
    queries = ["query1"]
    mock_generate_queries.return_value = queries
    mock_search.return_value = None # Simulate search failure

    # Mock datetime for date calculation
    mock_status_date_str = mock_bill_data_basic['status_date']
    mock_status_date = datetime(2024, 1, 15)
    mock_dt.strptime.side_effect = lambda d, fmt: mock_status_date if d == mock_status_date_str and fmt == '%Y-%m-%d' else datetime.strptime(d, fmt)
    mock_dt.now.return_value = datetime(2024, 5, 15)
    mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

    result = collect_news_for_bill(mock_bill_data_basic, 'ID', mock_news_paths['raw_news'])

    assert result is True # Function might still succeed but save empty data
    mock_generate_queries.assert_called_once()
    mock_search.assert_called_once() # Called once for the single query
    mock_save.assert_called_once()
    args_save, kwargs_save = mock_save.call_args
    saved_data = args_save[1]
    assert saved_data == [] # Expect empty list to be saved if search fails

@patch('src.news_collection.save_json', side_effect=IOError("Disk full"))
@patch('src.news_collection.search_news_articles')
@patch('src.news_collection.generate_queries_for_bill')
@patch('src.news_collection.datetime')
@patch('pathlib.Path.mkdir')
def test_collect_news_for_bill_save_fails(
    mock_mkdir, mock_dt, mock_generate_queries, mock_search, mock_save,
    mock_bill_data_basic, mock_news_paths, mock_news_search_success
):
    """Test collect_news_for_bill when save_json fails."""
    queries = ["query1"]
    mock_generate_queries.return_value = queries
    mock_search.return_value = mock_news_search_success # Search succeeds

    # Mock datetime for date calculation
    mock_status_date_str = mock_bill_data_basic['status_date']
    mock_status_date = datetime(2024, 1, 15)
    mock_dt.strptime.side_effect = lambda d, fmt: mock_status_date if d == mock_status_date_str and fmt == '%Y-%m-%d' else datetime.strptime(d, fmt)
    mock_dt.now.return_value = datetime(2024, 5, 15)
    mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)

    result = collect_news_for_bill(mock_bill_data_basic, 'ID', mock_news_paths['raw_news'])

    assert result is False # Should fail if saving fails
    mock_generate_queries.assert_called_once()
    mock_search.assert_called_once()
    mock_save.assert_called_once() # Save was attempted

@patch('src.news_collection.save_json') # Mock save_json even though we check search args
@patch('src.news_collection.search_news_articles')
@patch('src.news_collection.generate_queries_for_bill')
@patch('pathlib.Path.mkdir') # Mock mkdir
def test_collect_news_for_bill_no_status_date(
    mock_mkdir, mock_generate_queries, mock_search, mock_save,
    mock_bill_data_minimal, mock_news_paths, mock_news_search_success
):
    """Test collect_news_for_bill when bill lacks a status_date."""
    # Bill data fixture missing 'status_date'
    state = 'WY'
    queries = [f"{state} SB 50"]
    mock_generate_queries.return_value = queries
    mock_search.return_value = mock_news_search_success

    # We don't need to mock datetime here as it shouldn't be used

    collect_news_for_bill(mock_bill_data_minimal, state, mock_news_paths['raw_news'])

    # Check that search was called *without* date parameters
    mock_search.assert_called_once()
    args, kwargs = mock_search.call_args
    search_params = args[1] # Params are the second positional argument
    assert 'from' not in search_params # News API uses 'from', not 'from_date'
    assert 'to' not in search_params
    assert search_params['q'] == queries[0]
    # Ensure save was still called (with whatever data was found)
    mock_save.assert_called_once()


# --- Tests for collect_news_for_bills ---

@patch('src.news_collection.collect_news_for_bill')
@patch('src.news_collection.tqdm', lambda x, **kwargs: x) # Mock tqdm to disable progress bar
def test_collect_news_for_bills_success(
    mock_collect_single, mock_bill_data_basic, mock_bill_data_minimal, mock_news_paths
):
    """Test successful collection for multiple bills."""
    bills_list = [mock_bill_data_basic, mock_bill_data_minimal]
    state = 'ID'
    mock_collect_single.return_value = True # Simulate success for each bill

    result = collect_news_for_bills(bills_list, state, mock_news_paths)

    assert result is True
    assert mock_collect_single.call_count == len(bills_list)
    mock_collect_single.assert_has_calls([
        call(mock_bill_data_basic, state, mock_news_paths['raw_news']),
        call(mock_bill_data_minimal, state, mock_news_paths['raw_news']),
    ], any_order=True) # Order of calls might not be guaranteed depending on implementation

@patch('src.news_collection.collect_news_for_bill')
@patch('src.news_collection.tqdm', lambda x, **kwargs: x)
def test_collect_news_for_bills_max_bills_limit(
    mock_collect_single, mock_bill_data_basic, mock_bill_data_minimal, mock_news_paths
):
    """Test that the max_bills parameter limits processing."""
    bills_list = [mock_bill_data_basic, mock_bill_data_minimal, {'bill_id': 999}] # Three bills
    state = 'ID'
    max_to_process = 2
    mock_collect_single.return_value = True

    result = collect_news_for_bills(bills_list, state, mock_news_paths, max_bills=max_to_process)

    assert result is True
    assert mock_collect_single.call_count == max_to_process # Only called for the limited number

@patch('src.news_collection.collect_news_for_bill')
@patch('src.news_collection.tqdm', lambda x, **kwargs: x)
def test_collect_news_for_bills_single_failure(
    mock_collect_single, mock_bill_data_basic, mock_bill_data_minimal, mock_news_paths
):
    """Test collect_news_for_bills when one bill fails."""
    bills_list = [mock_bill_data_basic, mock_bill_data_minimal]
    state = 'ID'
    # Simulate failure for the second bill
    mock_collect_single.side_effect = [True, False]

    result = collect_news_for_bills(bills_list, state, mock_news_paths)

    assert result is False # Overall result should be False if any fail
    assert mock_collect_single.call_count == len(bills_list) # Should attempt all

@patch('src.news_collection.collect_news_for_bill')
@patch('src.news_collection.tqdm', lambda x, **kwargs: x)
def test_collect_news_for_bills_empty_list(mock_collect_single, mock_news_paths):
    """Test collect_news_for_bills with an empty list of bills."""
    bills_list = []
    state = 'ID'

    result = collect_news_for_bills(bills_list, state, mock_news_paths)

    assert result is True # Should succeed if there's nothing to process
    mock_collect_single.assert_not_called()

@patch('src.news_collection.collect_news_for_bill')
@patch('src.news_collection.tqdm', lambda x, **kwargs: x)
def test_collect_news_for_bills_year_filter(
    mock_collect_single, mock_news_paths
):
    """Test collect_news_for_bills filtering by year (if implemented)."""
    # Assumes bill data has a 'session' dict with 'year_start'/'year_end'
    bill1 = {'bill_id': 1, 'session': {'year_start': 2023, 'year_end': 2023}}
    bill2 = {'bill_id': 2, 'session': {'year_start': 2024, 'year_end': 2024}}
    bill3 = {'bill_id': 3, 'session': {'year_start': 2024, 'year_end': 2024}}
    bills_list = [bill1, bill2, bill3]
    state = 'ID'
    target_year = 2024
    mock_collect_single.return_value = True

    result = collect_news_for_bills(bills_list, state, mock_news_paths, year=target_year)

    assert result is True
    assert mock_collect_single.call_count == 2 # Only bills from 2024 should be processed
    # Check that it was called with the correct bills
    calls = mock_collect_single.call_args_list
    processed_bill_ids = {c.args[0]['bill_id'] for c in calls}
    assert processed_bill_ids == {2, 3}


# --- Placeholder Tests for Other Functions ---
# (test_collect_news_for_bills placeholder removed)

# --- End of File --- 