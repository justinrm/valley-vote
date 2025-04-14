# Standard library imports
import logging
import json
import time
import random
from datetime import datetime, timedelta
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterable, Union, Tuple, Set

# Third-party imports
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords

# Local imports
from .config import (
    NEWS_API_KEY,
    NEWS_API_URL,
    NEWS_MAX_RETRIES,
    NEWS_DEFAULT_WAIT_SECONDS,
    DATA_COLLECTION_LOG_FILE,
)
from .utils import (
    setup_logging,
    save_json,
    convert_to_csv,
    fetch_page,
    load_json,
    clean_text,
    ensure_dir,
    setup_project_paths
)

# --- Configure Logging ---
logger = logging.getLogger(__name__)

# --- NLTK Download Check ---
def ensure_nltk_resources():
    """Download required NLTK resources if not already available."""
    try:
        resources = ['punkt', 'stopwords']
        for resource in resources:
            try:
                # Test if the resource exists by attempting to use it
                if resource == 'punkt':
                    sent_tokenize("Test sentence.")
                elif resource == 'stopwords':
                    stopwords.words('english')
            except LookupError:
                logger.info(f"Downloading NLTK resource: {resource}")
                nltk.download(resource, quiet=True)
    except Exception as e:
        logger.warning(f"Error ensuring NLTK resources: {e}. Some NLP functions may not work.")

# --- Custom Exceptions ---
class NewsAPIError(Exception):
    """Custom exception for API errors during news data collection."""
    pass

class NewsRateLimitError(Exception):
    """Custom exception for rate limiting during news API requests."""
    pass

# --- API Fetching Logic ---
@retry(
    stop=stop_after_attempt(NEWS_MAX_RETRIES),
    wait=wait_exponential(multiplier=1.5, min=2, max=60),
    retry=retry_if_exception_type((requests.exceptions.RequestException, NewsRateLimitError)),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
)
def fetch_news_data(endpoint: str, params: Dict[str, Any], wait_time: Optional[float] = None) -> Optional[Dict]:
    """
    Fetch news article data from API with retry logic and error handling.

    Args:
        endpoint: API endpoint to fetch from.
        params: API parameters for the request.
        wait_time: Optional override for wait time before this specific call.

    Returns:
        Dictionary containing the JSON response data, or None on failure.

    Raises:
        NewsRateLimitError: If rate limit is hit (triggers retry).
        requests.exceptions.RequestException: For network issues (triggers retry).
    """
    if not NEWS_API_KEY:
        logger.error("Cannot fetch news data: NEWS_API_KEY is not set.")
        return None

    request_params = params.copy()
    request_params['apiKey'] = NEWS_API_KEY
    
    base_wait = wait_time if wait_time is not None else NEWS_DEFAULT_WAIT_SECONDS
    sleep_duration = max(0.1, base_wait + random.uniform(-0.2, 0.4))
    logger.debug(f"Sleeping for {sleep_duration:.2f}s before news API request")
    time.sleep(sleep_duration)

    try:
        logger.info(f"Fetching news data from endpoint: {endpoint}")
        log_params = {k: v for k, v in request_params.items() if k != 'apiKey'}
        logger.debug(f"Request params (apiKey omitted): {log_params}")

        url = f"{NEWS_API_URL}/{endpoint}"
        response = requests.get(url, params=request_params, timeout=30, headers={'Accept': 'application/json'})

        if response.status_code == 429:
            logger.warning(f"News API rate limit hit (HTTP 429). Backing off...")
            raise NewsRateLimitError("Rate limit exceeded")

        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response from news API. Status: {response.status_code}")
            return None

        if data.get('status') == 'error':
            error_msg = data.get('message', 'Unknown news API error')
            logger.error(f"News API error: {error_msg}")
            return None
        
        logger.debug(f"Successfully fetched news data from endpoint: {endpoint}")
        return data

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 'N/A'
        logger.error(f"HTTP error {status_code} fetching news data: {str(e)}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception fetching news data: {str(e)}")
        raise

def search_news_articles(
    query: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    language: str = 'en',
    page_size: int = 100,
    page: int = 1
) -> Optional[Dict[str, Any]]:
    """
    Search for news articles based on query and date range.
    
    Args:
        query: Search query string
        from_date: Start date in format 'YYYY-MM-DD'
        to_date: End date in format 'YYYY-MM-DD'
        language: Language code (default: 'en' for English)
        page_size: Number of results per page (max 100)
        page: Page number to fetch
        
    Returns:
        Dictionary with articles and metadata or None on failure
    """
    logger.info(f"Searching news articles for query: '{query}'")
    
    params = {
        'q': query,
        'language': language,
        'pageSize': min(page_size, 100),  # API limit is 100 results per page
        'page': page,
        'sortBy': 'relevancy'
    }
    
    if from_date:
        params['from'] = from_date
    
    if to_date:
        params['to'] = to_date
    
    try:
        response = fetch_news_data('everything', params)
        if not response or response.get('status') != 'ok' or 'articles' not in response:
            logger.error(f"Failed to fetch valid news data for query: '{query}'")
            return None
            
        total_results = response.get('totalResults', 0)
        articles = response.get('articles', [])
        logger.info(f"Found {total_results} total articles for query '{query}', returned {len(articles)} results (page {page})")
        
        return response
    except Exception as e:
        logger.error(f"Error searching news articles: {str(e)}", exc_info=True)
        return None

def generate_queries_for_bill(bill_data: Dict[str, Any], state: str) -> List[str]:
    """
    Generate search queries based on bill information.
    
    Args:
        bill_data: Dictionary containing bill information
        state: Two-letter state code
        
    Returns:
        List of search queries for the bill
    """
    queries = []
    
    # Try to get bill title and number
    bill_number = bill_data.get('bill_number', '')
    title = bill_data.get('title', '')
    description = bill_data.get('description', '')
    
    # Generate bill number query
    if bill_number:
        queries.append(f"{state} {bill_number}")
        queries.append(f"{state} legislature {bill_number}")
    
    # Generate keyword queries from title
    if title:
        # Simple keyword extraction (could be improved with NLP)
        # Remove common words and keep only significant terms
        try:
            ensure_nltk_resources()
            stop_words = set(stopwords.words('english'))
            words = word_tokenize(title.lower())
            keywords = [word for word in words if word.isalnum() and word not in stop_words and len(word) > 3]
            
            if len(keywords) >= 3:
                # Use top keywords
                top_keywords = keywords[:5]  # Limit to 5 keywords
                keyword_query = " ".join(top_keywords)
                queries.append(f"{state} legislature {keyword_query}")
        except Exception as e:
            logger.warning(f"Error generating keyword queries: {e}")
    
    # Add a description-based query if available and not too long
    if description and len(description) < 150:
        # Extract first sentence only
        try:
            ensure_nltk_resources()
            first_sentence = sent_tokenize(description)[0]
            # Simplify by taking first few words
            words = word_tokenize(first_sentence)
            simplified = " ".join(words[:8])  # First 8 words
            queries.append(f"{state} {simplified}")
        except Exception as e:
            logger.warning(f"Error generating description query: {e}")
    
    # Deduplicate queries and return
    return list(set(queries))

def collect_news_for_bill(
    bill_data: Dict[str, Any],
    state: str,
    output_dir: Path,
    days_before: int = 30,
    days_after: int = 60
) -> bool:
    """
    Collect news articles related to a specific bill.
    
    Args:
        bill_data: Dictionary containing bill information
        state: Two-letter state code
        output_dir: Directory to save the collected news
        days_before: Number of days before bill introduction to search
        days_after: Number of days after bill introduction to search
        
    Returns:
        True if collection was successful, False otherwise
    """
    bill_id = bill_data.get('bill_id')
    bill_number = bill_data.get('bill_number', '')
    
    if not bill_id:
        logger.error("Missing bill_id in bill data")
        return False
    
    logger.info(f"Collecting news for bill ID {bill_id} ({bill_number if bill_number else 'unnamed'})")
    
    # Generate date range for search
    introduced_date_str = bill_data.get('date_introduced')
    if not introduced_date_str:
        logger.warning(f"Missing introduction date for bill {bill_id}, using current date")
        introduced_date = datetime.now()
    else:
        try:
            introduced_date = datetime.fromisoformat(introduced_date_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            logger.warning(f"Invalid date format for bill {bill_id}: {introduced_date_str}, using current date")
            introduced_date = datetime.now()
    
    # Calculate date range
    from_date = (introduced_date - timedelta(days=days_before)).strftime('%Y-%m-%d')
    to_date = (introduced_date + timedelta(days=days_after)).strftime('%Y-%m-%d')
    
    # Generate search queries
    queries = generate_queries_for_bill(bill_data, state)
    if not queries:
        logger.warning(f"No valid search queries generated for bill {bill_id}")
        return False
    
    # Create output file path
    output_file = output_dir / f"news_bill_{bill_id}.json"
    
    # Skip if already downloaded (unless implementing force_refresh)
    if output_file.exists():
        logger.debug(f"Skipping existing news file for bill {bill_id}")
        return True
    
    # Search for articles with each query
    all_articles = []
    seen_urls = set()
    
    for query in queries:
        try:
            logger.debug(f"Searching with query: '{query}' for bill {bill_id}")
            result = search_news_articles(
                query=query,
                from_date=from_date,
                to_date=to_date,
                language='en',
                page_size=100,
                page=1
            )
            
            if not result or 'articles' not in result:
                logger.warning(f"No results for query '{query}' for bill {bill_id}")
                continue
            
            articles = result.get('articles', [])
            
            # Add query information to each article
            for article in articles:
                # Skip duplicates based on URL
                url = article.get('url')
                if not url or url in seen_urls:
                    continue
                
                seen_urls.add(url)
                article['search_query'] = query
                article['bill_id'] = bill_id
                all_articles.append(article)
            
            logger.debug(f"Found {len(articles)} articles for query '{query}' (bill {bill_id})")
        except Exception as e:
            logger.error(f"Error searching news for query '{query}': {str(e)}", exc_info=True)
    
    # If we found articles, save them
    if all_articles:
        save_json(all_articles, output_file)
        logger.info(f"Saved {len(all_articles)} news articles for bill {bill_id} to {output_file}")
        return True
    else:
        logger.warning(f"No news articles found for bill {bill_id}")
        # Save empty list as placeholder
        save_json([], output_file)
        return False

def collect_news_for_bills(
    bills: List[Dict[str, Any]],
    state: str,
    paths: Dict[str, Path],
    year: Optional[int] = None,
    max_bills: Optional[int] = None
) -> bool:
    """
    Collect news articles for a list of bills.
    
    Args:
        bills: List of bill dictionaries
        state: Two-letter state code
        paths: Project paths dictionary
        year: Optional year to use for output directory organization
        max_bills: Optional limit on number of bills to process
        
    Returns:
        True if overall collection was successful, False otherwise
    """
    logger.info(f"Starting news collection for {len(bills)} bills from {state}")
    
    # Ensure output directories exist
    news_dir = paths.get('raw_news')
    if not news_dir:
        logger.error("News directory not configured in paths")
        return False
    
    if year:
        news_dir = news_dir / str(year)
    
    news_dir.mkdir(parents=True, exist_ok=True)
    
    # Apply max_bills limit if specified
    if max_bills and max_bills > 0 and max_bills < len(bills):
        logger.info(f"Limiting to {max_bills} bills as requested")
        # Prioritize bills with more complete data
        bills = sorted(bills, key=lambda b: 1 if b.get('title') and b.get('bill_number') else 0, reverse=True)
        bills = bills[:max_bills]
    
    # Process each bill
    success_count = 0
    failure_count = 0
    
    for bill in tqdm(bills, desc=f"Collecting news for bills ({state})", unit="bill"):
        bill_id = bill.get('bill_id')
        if not bill_id:
            logger.warning(f"Skipping bill without ID: {bill}")
            continue
        
        success = collect_news_for_bill(bill, state, news_dir)
        if success:
            success_count += 1
        else:
            failure_count += 1
    
    total_bills = len(bills)
    logger.info(f"News collection complete: {success_count}/{total_bills} successful, {failure_count}/{total_bills} failed")
    
    # Return overall success status (allowing for some failures)
    return failure_count == 0 or (success_count > 0 and failure_count < total_bills * 0.3)  # Allow up to 30% failure rate

def extract_article_text(article_url: str) -> Optional[str]:
    """
    Extract full article text from a news article URL.
    
    Args:
        article_url: URL of the news article
        
    Returns:
        Extracted article text or None if extraction failed
    """
    try:
        logger.debug(f"Extracting text from URL: {article_url}")
        
        # Fetch page content
        soup = fetch_page(article_url)
        if not soup:
            logger.warning(f"Failed to fetch/parse page at URL: {article_url}")
            return None
        
        # Extract article text
        # Note: This is a simple implementation and might need customization based on site structure
        # Try common article container selectors
        article_selectors = [
            'article', 'div.article', 'div.content', '.article-body', '.story-body',
            '.entry-content', 'main', '#main-content', '.post-content'
        ]
        
        article_content = None
        for selector in article_selectors:
            content = soup.select_one(selector)
            if content and len(content.get_text(strip=True)) > 200:  # Minimum length to be valid article
                article_content = content
                break
        
        if not article_content:
            # Fallback: try to find largest text container
            article_candidates = soup.find_all(['p', 'div'])
            article_candidates = [c for c in article_candidates if len(c.get_text(strip=True)) > 100]
            if article_candidates:
                # Sort by text length
                article_candidates.sort(key=lambda x: len(x.get_text(strip=True)), reverse=True)
                article_content = article_candidates[0]
        
        if not article_content:
            logger.warning(f"Could not identify article content at URL: {article_url}")
            return None
        
        # Clean the extracted text
        full_text = article_content.get_text(separator=' ', strip=True)
        
        # Basic cleaning
        full_text = re.sub(r'\s+', ' ', full_text)  # Replace multiple spaces with single space
        full_text = re.sub(r'[^\w\s.,;:!?\'"-]', '', full_text)  # Remove special characters
        
        if len(full_text) < 100:
            logger.warning(f"Extracted text too short ({len(full_text)} chars) from URL: {article_url}")
            return None
        
        logger.debug(f"Successfully extracted {len(full_text)} chars from URL: {article_url}")
        return full_text
    
    except Exception as e:
        logger.error(f"Error extracting text from URL {article_url}: {str(e)}", exc_info=True)
        return None

def fetch_full_text_for_articles(
    articles: List[Dict[str, Any]], 
    max_articles: Optional[int] = None,
    skip_existing: bool = True
) -> List[Dict[str, Any]]:
    """
    Fetch full text content for a list of news articles.
    
    Args:
        articles: List of article dictionaries with URLs
        max_articles: Optional maximum number of articles to process
        skip_existing: Skip articles that already have full text
        
    Returns:
        Updated list of articles with full text where available
    """
    logger.info(f"Fetching full text for {len(articles)} articles")
    
    if max_articles and max_articles > 0 and max_articles < len(articles):
        logger.info(f"Limiting to {max_articles} articles as requested")
        # Prioritize articles with more complete data
        articles = sorted(
            articles, 
            key=lambda a: 1 if a.get('title') and a.get('description') and a.get('url') else 0, 
            reverse=True
        )
        articles = articles[:max_articles]
    
    articles_with_text = []
    success_count = 0
    failure_count = 0
    
    for article in tqdm(articles, desc="Fetching article text", unit="article"):
        # Skip if already has content and skip_existing is True
        if skip_existing and article.get('fullTextContent'):
            articles_with_text.append(article)
            success_count += 1
            continue
        
        url = article.get('url')
        if not url:
            logger.warning(f"Skipping article without URL: {article.get('title', 'Unnamed')}")
            failure_count += 1
            articles_with_text.append(article)  # Still add to results
            continue
        
        # Try to extract full text
        full_text = extract_article_text(url)
        if full_text:
            article['fullTextContent'] = full_text
            success_count += 1
        else:
            failure_count += 1
        
        articles_with_text.append(article)
        
        # Add a short delay between requests to avoid overwhelming sites
        time.sleep(random.uniform(1.0, 2.0))
    
    total_articles = len(articles)
    logger.info(f"Text extraction complete: {success_count}/{total_articles} successful, {failure_count}/{total_articles} failed")
    
    return articles_with_text

def process_and_enrich_news_data(
    raw_news_dir: Path,
    processed_dir: Path,
    fetch_full_text: bool = False,
    max_full_text_articles: Optional[int] = None
) -> bool:
    """
    Process and enrich raw news data, optionally fetching full text.
    
    Args:
        raw_news_dir: Directory containing raw news files
        processed_dir: Directory to save processed output
        fetch_full_text: Whether to attempt to fetch full article text
        max_full_text_articles: Maximum articles to fetch full text for (per bill)
        
    Returns:
        True if processing was successful, False otherwise
    """
    logger.info(f"Processing news data from {raw_news_dir}")
    
    # Ensure output directories exist
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all news data files
    news_files = list(raw_news_dir.glob("news_bill_*.json"))
    if not news_files:
        logger.warning(f"No news files found in {raw_news_dir}")
        return False
    
    logger.info(f"Found {len(news_files)} news files to process")
    
    # Process each file
    all_news_articles = []
    bill_article_counts = {}
    
    for news_file in tqdm(news_files, desc="Processing news files", unit="file"):
        try:
            # Extract bill_id from filename
            match = re.search(r'news_bill_(\d+)\.json', news_file.name)
            if not match:
                logger.warning(f"Could not extract bill_id from filename: {news_file.name}")
                continue
            
            bill_id = match.group(1)
            
            # Load articles
            articles = load_json(news_file)
            if not articles:
                logger.debug(f"No articles in {news_file}")
                continue
            
            # Set bill_id on all articles if not already set
            for article in articles:
                if 'bill_id' not in article:
                    article['bill_id'] = bill_id
            
            # Fetch full text if requested
            if fetch_full_text:
                articles = fetch_full_text_for_articles(
                    articles,
                    max_articles=max_full_text_articles
                )
            
            # Add to collection
            bill_article_counts[bill_id] = len(articles)
            all_news_articles.extend(articles)
            
        except Exception as e:
            logger.error(f"Error processing news file {news_file}: {str(e)}", exc_info=True)
    
    # Save consolidated data
    if all_news_articles:
        # Convert to DataFrame for CSV export
        try:
            # Create basic DataFrame with common fields
            basic_fields = ['title', 'url', 'publishedAt', 'source', 'author', 'description', 'bill_id', 'search_query']
            news_df = pd.DataFrame([
                {field: article.get(field) for field in basic_fields if field in article}
                for article in all_news_articles
            ])
            
            # Add source name from nested source object
            if 'source' in news_df.columns:
                news_df['source_name'] = news_df['source'].apply(lambda x: x.get('name') if isinstance(x, dict) else x)
                news_df.drop('source', axis=1, inplace=True)
            
            # Save to CSV
            news_csv = processed_dir / "news_articles.csv"
            news_df.to_csv(news_csv, index=False)
            logger.info(f"Saved {len(news_df)} processed news articles to {news_csv}")
            
            # If we have full text content, save to separate file
            if fetch_full_text:
                # Create a list of articles with full text
                articles_with_text = [a for a in all_news_articles if a.get('fullTextContent')]
                
                if articles_with_text:
                    full_text_df = pd.DataFrame([
                        {
                            'url': article.get('url'),
                            'bill_id': article.get('bill_id'),
                            'fullTextContent': article.get('fullTextContent')
                        }
                        for article in articles_with_text
                    ])
                    
                    full_text_csv = processed_dir / "news_articles_full_text.csv"
                    full_text_df.to_csv(full_text_csv, index=False)
                    logger.info(f"Saved {len(full_text_df)} articles with full text to {full_text_csv}")
            
            # Also save raw JSON for preservation
            raw_json_file = processed_dir / "news_articles_raw.json"
            save_json(all_news_articles, raw_json_file)
            logger.debug(f"Saved raw JSON data to {raw_json_file}")
            
            # Generate summary statistics
            bills_with_news = len(bill_article_counts)
            total_articles = len(all_news_articles)
            avg_articles_per_bill = total_articles / bills_with_news if bills_with_news > 0 else 0
            
            logger.info(f"News data summary: {total_articles} articles for {bills_with_news} bills (avg: {avg_articles_per_bill:.1f} articles/bill)")
            return True
            
        except Exception as e:
            logger.error(f"Error creating news DataFrame: {str(e)}", exc_info=True)
            return False
    else:
        logger.warning("No news articles to process")
        return False

def main_news_collection(
    bills: List[Dict[str, Any]],
    state: str,
    paths: Dict[str, Path],
    year: Optional[int] = None,
    max_bills: Optional[int] = None,
    fetch_full_text: bool = False,
    max_full_text_articles: Optional[int] = None
) -> bool:
    """
    Main function to perform the complete news collection workflow.
    
    Args:
        bills: List of bill dictionaries
        state: Two-letter state code
        paths: Project paths dictionary
        year: Optional year for output organization
        max_bills: Optional limit on number of bills to process
        fetch_full_text: Whether to fetch full article text
        max_full_text_articles: Maximum articles to fetch full text for (per bill)
        
    Returns:
        True if collection was successful, False otherwise
    """
    logger.info(f"Starting news collection pipeline for {state} ({len(bills)} bills)")
    
    # Ensure directories exist
    raw_news_dir = paths.get('raw_news')
    processed_dir = paths.get('processed')
    
    if not raw_news_dir or not processed_dir:
        logger.error("News or processed directory not configured in paths")
        return False
    
    if year:
        raw_news_dir = raw_news_dir / str(year)
    
    raw_news_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Collect news articles for bills
    collection_success = collect_news_for_bills(
        bills=bills,
        state=state,
        paths=paths,
        year=year,
        max_bills=max_bills
    )
    
    if not collection_success:
        logger.warning("News collection encountered significant failures")
    
    # Step 2: Process and enrich the collected data
    processing_success = process_and_enrich_news_data(
        raw_news_dir=raw_news_dir,
        processed_dir=processed_dir,
        fetch_full_text=fetch_full_text,
        max_full_text_articles=max_full_text_articles
    )
    
    if not processing_success:
        logger.warning("News data processing encountered failures")
    
    overall_success = collection_success and processing_success
    logger.info(f"News collection pipeline completed with overall success: {overall_success}")
    
    return overall_success 