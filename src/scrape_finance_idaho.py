#!/usr/bin/env python3
"""
Scrape campaign finance data from Idaho SOS Sunshine Portal.

This module implements Idaho-specific campaign finance data collection.
It is called directly by main.py rather than through data_collection.py's stub function,
as it contains state-specific scraping logic.

Dependencies:
- Requires processed legislators file from data_collection.py
- Uses shared path and logging configuration
- Uses shared utility functions from utils.py
"""

# Standard library imports
import argparse
import csv
import json
import logging
import re
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from urllib.parse import urljoin, urlparse
import io
import sys

# Third-party imports
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
# Add Playwright imports
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

# Local imports
from .config import (
    ID_FINANCE_BASE_URL,
    ID_FINANCE_DOWNLOAD_WAIT_SECONDS,
    FINANCE_SCRAPE_LOG_FILE,
    FINANCE_COMMITTEE_INDICATORS
)
from .utils import (
    setup_logging,
    save_json,
    convert_to_csv,
    fetch_page,
    setup_project_paths
)
from .data_collection import FINANCE_COLUMN_MAPS

# --- Custom Exceptions ---
class ScrapingStructureError(Exception):
    """Custom exception for unexpected website structure during scraping."""
    pass

# --- Configure Logging ---
logger = logging.getLogger(Path(FINANCE_SCRAPE_LOG_FILE).stem)

# --- Constants ---
CONTRIBUTION_COLUMN_MAP = FINANCE_COLUMN_MAPS['contributions']
EXPENDITURE_COLUMN_MAP = FINANCE_COLUMN_MAPS['expenditures']

# --- Helper Functions for Playwright ---
def safe_goto(page: Page, url: str, timeout_ms: int = 60000) -> bool:
    """Navigate to a URL with error handling."""
    try:
        logger.debug(f"Navigating to {url}")
        response = page.goto(url, wait_until='domcontentloaded', timeout=timeout_ms)
        if response and not response.ok:
            logger.error(f"Page load failed for {url}. Status: {response.status}")
            return False
        logger.debug(f"Successfully navigated to {url}")
        return True
    except PlaywrightTimeoutError:
        logger.error(f"Timeout loading page: {url}")
        return False
    except Exception as e:
        logger.error(f"Error navigating to {url}: {e}")
        return False

def save_debug_html(page: Page, filename_prefix: str, paths: Dict[str, Path]):
    """Saves the current page HTML for debugging."""
    artifacts_dir = paths['base'] / 'artifacts' if 'artifacts' not in paths else paths['artifacts']
    debug_path = artifacts_dir / 'debug'
    debug_path.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d%H%M%S")
    debug_file = debug_path / f"{filename_prefix}_{timestamp}.html"
    try:
        debug_file.write_text(page.content(), encoding='utf-8')
        logger.info(f"Saved debug HTML to: {debug_file}")
    except Exception as e:
        logger.error(f"Failed to save debug HTML: {e}")

# --- Refactored Search Function Using Playwright ---
def search_with_playwright(
    search_term: str,
    year: int,
    data_type: str,
    paths: Dict[str, Path]
) -> Optional[Tuple[str, pd.DataFrame]]:
    """
    Uses Playwright to search for finance data and return the export URL and possibly data.
    
    Args:
        search_term: Name of legislator or committee.
        year: Election year or reporting year.
        data_type: 'contributions' or 'expenditures'.
        paths: Project paths dictionary.
        
    Returns:
        Optional tuple of (export_url, data_frame).
        If export_url is None, no data could be found.
    """
    logger.info(f"Initiating finance search with Playwright: Term='{search_term}', Year={year}, Type={data_type}")
    
    # Configure selectors based on website inspection
    name_input_selector = '#panel-campaigns-content input[role="combobox"][id^="react-select-"]'
    date_input_selector = 'input[placeholder="Any Date"][type="tel"]'
    search_button_selector = 'button:has-text("Search")'
    results_grid_indicator_selector = 'div[role="columnheader"][class*="header-cell-label"], div.ag-header-cell-text'
    export_button_selector = 'button[title*="Export" i], button:has-text("Export to CSV"), a:has-text("Export")'
    
    # Calculate date range for the year
    start_date = f"01/01/{year}"
    end_date = f"12/31/{year}"
    
    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                accept_downloads=True,  # Important for handling file downloads
                viewport={'width': 1280, 'height': 1024}
            )
            page = context.new_page()
            
            # Navigate to search page
            if not safe_goto(page, ID_FINANCE_BASE_URL, timeout_ms=60000):
                logger.error(f"Failed to load search page: {ID_FINANCE_BASE_URL}")
                return None
            
            logger.info("Search page loaded. Waiting for dynamic content...")
            page.wait_for_timeout(3000)
            
            # --- Fill search form ---
            try:
                # Locate and fill name input
                logger.info(f"Looking for name search input field...")
                name_input = page.locator(name_input_selector).first
                name_input.wait_for(state='attached', timeout=15000)
                
                # Focus and fill name input
                logger.info(f"Filling search term: '{search_term}'")
                try:
                    name_input.focus(timeout=5000)
                    page.wait_for_timeout(500)
                except PlaywrightTimeoutError:
                    # Try JavaScript focus as a fallback
                    input_id = name_input.get_attribute('id')
                    if input_id:
                        page.evaluate(f"document.querySelector('#{input_id}').focus()")
                        page.wait_for_timeout(500)
                    else:
                        logger.error("Could not focus name input field")
                        return None
                
                # Type name and select first option
                name_input.fill(search_term, timeout=10000)
                page.wait_for_timeout(1000)
                
                # Try to click the dropdown option that appears
                option_selector = 'div[id*="react-select-"][class*="-option"]'
                first_option = page.locator(option_selector).first
                try:
                    first_option.wait_for(state='visible', timeout=5000)
                    first_option.click(timeout=5000)
                    page.wait_for_timeout(500)
                except PlaywrightTimeoutError:
                    logger.warning(f"No dropdown options appeared for '{search_term}'. Will try to proceed.")
                    # Press Enter as a fallback
                    name_input.press('Enter')
                    page.wait_for_timeout(500)
                
                # Fill date range inputs
                logger.info(f"Setting date range: {start_date} to {end_date}")
                date_inputs = page.locator(date_input_selector)
                if date_inputs.count() >= 2:
                    start_date_input = date_inputs.nth(0)
                    end_date_input = date_inputs.nth(1)
                    
                    start_date_input.fill(start_date, timeout=5000)
                    end_date_input.fill(end_date, timeout=5000)
                    page.wait_for_timeout(500)
                else:
                    logger.warning("Could not find both date inputs. Proceeding with default date range.")
                
                # Submit search
                logger.info("Submitting search...")
                search_button = page.locator(search_button_selector).first
                search_button.click(timeout=10000)
                
                # Wait for results to appear
                logger.info("Waiting for search results...")
                try:
                    page.locator(results_grid_indicator_selector).first.wait_for(
                        state='visible', timeout=45000
                    )
                    logger.info("Search results appeared.")
                    page.wait_for_timeout(2000)  # Let the grid stabilize
                    
                    # Check for "no results" message
                    no_results_selector = 'div:has-text("No records found"), div:has-text("No results")'
                    if page.locator(no_results_selector).count() > 0:
                        logger.info(f"Search successful but returned 'No Records Found' for '{search_term}', {year}.")
                        return None
                    
                    # Look for export button
                    logger.info("Looking for export button...")
                    export_button = page.locator(export_button_selector).first
                    try:
                        export_button.wait_for(state='visible', timeout=20000)
                        
                        # Setup download listener
                        logger.info("Setting up download listener...")
                        download_path = None
                        with page.expect_download(timeout=60000) as download_info:
                            logger.info("Clicking export button...")
                            export_button.click(timeout=10000)
                            
                            # Wait for download to start
                            download = download_info.value
                            logger.info(f"Download started: {download.suggested_filename}")
                            
                            # Define download location
                            raw_dir = paths['raw_campaign_finance'] / 'idaho' / str(year)
                            raw_dir.mkdir(parents=True, exist_ok=True)
                            safe_term = "".join(c if c.isalnum() else '_' for c in search_term)[:50].strip('_')
                            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                            download_path = raw_dir / f"{data_type}_{safe_term}_{year}_{timestamp}.csv"
                            
                            # Save the file
                            download.save_as(download_path)
                            logger.info(f"Downloaded file saved to: {download_path}")
                        
                        if download_path and download_path.exists():
                            # Success - read the data
                            logger.info(f"Reading downloaded data from {download_path}")
                            try:
                                df = pd.read_csv(download_path, low_memory=False)
                                if not df.empty:
                                    return (str(download_path), df)
                                else:
                                    logger.warning(f"Downloaded file is empty: {download_path}")
                                    return None
                            except Exception as e_read:
                                logger.error(f"Error reading downloaded file: {e_read}")
                                return None
                        else:
                            logger.error("Download failed or file not saved properly")
                            return None
                            
                    except PlaywrightTimeoutError:
                        logger.error("Timeout waiting for export button")
                        save_debug_html(page, f"export_button_timeout_{data_type}_{year}", paths)
                        return None
                    
                except PlaywrightTimeoutError:
                    logger.error("Timeout waiting for search results")
                    save_debug_html(page, f"results_timeout_{data_type}_{year}", paths)
                    return None
                
            except Exception as e:
                logger.error(f"Error during search form interaction: {e}")
                save_debug_html(page, f"search_error_{data_type}_{year}", paths)
                return None
                
        except Exception as e:
            logger.error(f"Unexpected error in Playwright search: {e}")
            return None
        finally:
            if browser:
                browser.close()
    
    return None

# --- Helper Functions ---
def standardize_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Standardizes DataFrame columns for a specific finance data type.
    
    Args:
        df: Input DataFrame to standardize
        data_type: Type of finance data ('contributions' or 'expenditures')
        
    Returns:
        DataFrame with standardized column names and all required columns present
        
    Raises:
        ValueError: If data_type is not recognized
    """
    # Get the appropriate column map based on data_type
    if data_type == 'contributions':
        column_map = CONTRIBUTION_COLUMN_MAP
    elif data_type == 'expenditures':
        column_map = EXPENDITURE_COLUMN_MAP
    else:
        raise ValueError(f"Unknown finance data type: {data_type}")
        
    # Ensure DataFrame is not empty
    if df.empty:
        logger.warning("Attempted to standardize columns on an empty DataFrame.")
        # Return DataFrame with standard columns but no data
        return pd.DataFrame(columns=list(column_map.values()))

    original_columns = df.columns.tolist() # Keep original for logging
    # Clean column names: lowercase, strip whitespace, remove trailing colons
    df.columns = df.columns.str.lower().str.strip().str.replace(r':$', '', regex=True)
    rename_dict = {}
    found_standard_names = set()

    for orig_col, std_col in column_map.items():
        if orig_col.lower() in df.columns:
            rename_dict[orig_col.lower()] = std_col
            found_standard_names.add(std_col)
            logger.debug(f"Mapping source column '{orig_col}' to standard '{std_col}'")

    # Log unmapped columns after cleaning
    current_cols_set = set(df.columns)
    mapped_source_cols_set = set(rename_dict.keys())
    unmapped_cols = list(current_cols_set - mapped_source_cols_set)
    if unmapped_cols:
         logger.warning(f"Unmapped columns found: {unmapped_cols}. Original columns: {original_columns}")

    # Perform renaming
    df = df.rename(columns=rename_dict)

    # Ensure all standard columns exist, add missing ones with pd.NA
    standard_columns_list = list(column_map.values())
    added_cols = []
    for standard_name in standard_columns_list:
        if standard_name not in df.columns:
            df[standard_name] = pd.NA # Use pandas NA type
            added_cols.append(standard_name)
    if added_cols:
         logger.debug(f"Added missing standard columns: {added_cols}")

    # Return DataFrame with only the standard columns in the defined order
    final_columns = [col for col in standard_columns_list if col in df.columns]
    return df[final_columns]

# --- Website Interaction & Parsing Functions ---

def get_hidden_form_fields(soup: BeautifulSoup) -> Dict[str, str]:
    """Extracts hidden input fields commonly used in ASP.NET forms."""
    fields = {}
    # Find the main form, often named 'aspnetForm' or starting with 'ctl' or 'form'
    main_form = soup.find('form', id=re.compile("aspnetForm|form", re.I))
    if not main_form:
         # Fallback to searching for any form tag
         main_form = soup.find('form')
         if not main_form:
              logger.error("CRITICAL: Could not find any <form> tag on the search page. Scraping cannot proceed.")
              # Consider raising ScrapingStructureError here if defined
              return {} # Cannot continue without a form

    hidden_inputs = main_form.find_all('input', {'type': 'hidden'})
    if not hidden_inputs:
        logger.warning("No hidden input fields found within the form.")

    for input_tag in hidden_inputs:
        name = input_tag.get('name')
        value = input_tag.get('value', '') # Default to empty string if value attribute is missing
        if name:
            # Prioritize known ASP.NET state fields
            if name in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
                 fields[name] = value
            # Be cautious about including other hidden fields automatically
            # else:
            #      logger.debug(f"Found other hidden field (potential): Name='{name}'")
            #      fields[name] = value # Include if necessary, but often not needed

    logger.debug(f"Extracted hidden fields: {list(fields.keys())}")
    # Make the missing ViewState check more critical
    if '__VIEWSTATE' not in fields:
        logger.error("CRITICAL: ViewState hidden field ('__VIEWSTATE') not found. Form submission will likely fail.")
        # Consider raising ScrapingStructureError here if defined
    return fields

def find_export_link(soup: BeautifulSoup, data_type: str, base_url: str) -> Optional[str]:
    """Finds the CSV/Excel export link/button in the search results HTML."""
    logger.debug(f"Searching results page for '{data_type}' export link/button...")
    export_link = None

    # --- Define Search Patterns (Prioritize specificity) ---
    # Pattern 1: Direct Link (<a> tag) - Prefer CSV
    # Use CSS selectors for more precise targeting if possible (inspect element)
    # Example specific IDs/Classes (ADJUST THESE BASED ON WEBSITE INSPECTION):
    # csv_link_selectors = ['a#ctl00_ContentPlaceHolder1_CsvExportLink', 'a.csv-export']
    # excel_link_selectors = ['a#ctl00_ContentPlaceHolder1_ExcelExportLink', 'a.excel-export']

    # More generic search if specific selectors fail
    possible_links = soup.select('a[id*="Export"], a[id*="Download"], a[title*="Export"], a[title*="Download"]')
    if not possible_links:
         # Fallback: Find links containing relevant text
         possible_links = soup.find_all('a', string=re.compile(r'\b(Export|Download|CSV|Excel)\b', re.I))

    csv_link = None
    excel_link = None
    for link in possible_links:
        href = link.get('href')
        if not href: continue # Skip links without href

        link_text = link.get_text(strip=True).lower()
        href_lower = href.lower()

        # Check for CSV indicators (more robust checks)
        if ('csv' in link_text or 'export to csv' in link_text or
            'format=csv' in href_lower or href_lower.endswith('.csv') or
            'csv' in link.get('id','').lower() or 'csv' in link.get('class','')):
             csv_link = href
             logger.info(f"Found potential CSV export link via <a> tag: {href}")
             break # Found preferred format (CSV), stop searching links

        # Check for Excel indicators (only if CSV not found yet)
        elif not csv_link and ('excel' in link_text or 'export to excel' in link_text or 'xls' in link_text or
                               'format=xls' in href_lower or href_lower.endswith(('.xls', '.xlsx')) or
                               'excel' in link.get('id','').lower() or 'xls' in link.get('id','').lower() or
                               'excel' in link.get('class','') or 'xls' in link.get('class','')):
             excel_link = href
             logger.info(f"Found potential Excel export link via <a> tag: {href}")
             # Continue searching in case a CSV link appears later

    export_link = csv_link or excel_link # Prioritize CSV

    # Pattern 2: Submit Button (<input type="submit"> or <button>) - Harder to handle
    if not export_link:
        # Example Selectors (ADJUST BASED ON INSPECTION):
        # button_selectors = ['input[type="submit"][name*="Export"]', 'button[id*="ExportButton"]']
        possible_buttons = soup.select('input[type="submit"][value*="Export"], input[type="submit"][value*="Download"], button[id*="Export"], button[id*="Download"]')
        if not possible_buttons:
             # Fallback: Find buttons containing relevant text
             possible_buttons = soup.find_all(['input', 'button'], string=re.compile(r'\b(Export|Download|CSV|Excel)\b', re.I))

        for button in possible_buttons:
             # Check if it's likely an export button
             button_name = button.get('name')
             button_value = button.get('value', button.get_text(strip=True)) # Button text as fallback value
             if button_name and ('export' in button_name.lower() or 'download' in button_name.lower() or
                                 'csv' in button_name.lower() or 'excel' in button_name.lower()):
                  logger.warning(f"Found potential export BUTTON: Name='{button_name}', Value/Text='{button_value}'.")
                  logger.warning("Download via button click usually requires another POST request with this button's name/value.")
                  logger.warning("This scraper is NOT configured to handle button-triggered downloads. It expects a direct link (<a> tag).")
                  # Cannot return a simple link here. Indicate failure.
                  export_link = None # Ensure export_link is None if only a button is found
                  break # Stop checking buttons

    # Pattern 3: JavaScript Triggers - Very difficult without browser automation
    if not export_link:
         # Find elements with onclick handlers mentioning export/download
         js_triggers = soup.find_all(True, onclick=re.compile(r'export|download|csv|excel', re.I))
         if js_triggers:
              onclick_attrs = [tag.get('onclick') for tag in js_triggers[:3] if tag.get('onclick')] # Get first few examples
              logger.warning(f"Found potential JavaScript export triggers (cannot handle with requests): {onclick_attrs}")
              export_link = None # Indicate failure

    # --- Final Processing ---
    if export_link:
        # Ensure the link is absolute
        absolute_link = urljoin(base_url, export_link)
        # Basic validation: Check if it still points to the same domain (or is relative)
        if urlparse(absolute_link).netloc != urlparse(base_url).netloc and not export_link.startswith('/'):
            logger.warning(f"Resolved export URL '{absolute_link}' points to a different domain than base '{base_url}'. Proceeding cautiously.")
        logger.info(f"Resolved export URL: {absolute_link}")
        return absolute_link
    else:
        logger.warning(f"Could not find a suitable download link (<a> tag) for '{data_type}' on results page. Check selectors/website structure.")
        return None


def search_for_finance_data_link(
    search_term: str,
    year: int,
    data_type: str,
    paths: Dict[str, Path]
    ) -> Optional[str]:
    """
    DEPRECATED. Searches the Idaho Sunshine Portal for a finance data download link.
    This function is kept for backward compatibility but will log a warning.
    
    Use search_with_playwright instead.
    """
    logger.warning("search_for_finance_data_link is deprecated. Use search_with_playwright instead.")
    # Call the new function if we want to maintain compatibility
    result = search_with_playwright(search_term, year, data_type, paths)
    if result:
        # Return just the file path as the "download link" for compatibility
        return result[0]
    return None

# --- Data Downloading and Processing ---
def download_and_extract_finance_data(
    download_path: str,
    data_df: Optional[pd.DataFrame],
    source_search_term: str, # e.g., legislator name used in search
    search_year: int,
    data_type: str, # 'contributions' or 'expenditures'
    paths: Dict[str, Path]
) -> Optional[pd.DataFrame]:
    """
    Processes, standardizes, and cleans finance data that was downloaded using Playwright.
    
    Args:
        download_path: Path to the downloaded file.
        data_df: DataFrame of already loaded data (if available).
        source_search_term: Search term used to find the data.
        search_year: Year of the data.
        data_type: 'contributions' or 'expenditures'.
        paths: Project paths dictionary.
        
    Returns:
        Standardized DataFrame or None if processing failed.
    """
    logger.info(f"Processing finance data: Type='{data_type}', Term='{source_search_term}', Year={search_year}")
    
    raw_path = Path(download_path)
    
    # If DataFrame was not passed, try to load from file
    if data_df is None:
        if not raw_path.exists():
            logger.error(f"Downloaded file does not exist: {raw_path}")
            return None
            
        try:
            # Try UTF-8 first
            data_df = pd.read_csv(raw_path, encoding='utf-8', low_memory=False)
            logger.debug(f"Successfully read CSV data using utf-8 from {raw_path.name}")
        except UnicodeDecodeError:
            try:
                # Try latin-1 as fallback
                data_df = pd.read_csv(raw_path, encoding='latin-1', low_memory=False)
                logger.debug(f"Successfully read CSV data using latin-1 from {raw_path.name}")
            except Exception as e:
                logger.error(f"Failed to read CSV data: {e}")
                return None
    
    # Check if DataFrame is empty
    if data_df is None or data_df.empty:
        logger.warning(f"No data available from {raw_path.name}")
        return None
    
    # --- Standardize Columns ---
    logger.debug(f"Standardizing {len(data_df)} rows for {data_type} from {raw_path.name}...")
    df_standardized = standardize_columns(data_df.copy(), data_type)
    
    # --- Add Metadata Columns ---
    df_standardized['source_search_term'] = source_search_term
    df_standardized['data_source_url'] = ID_FINANCE_BASE_URL
    df_standardized['scrape_year'] = search_year # Year used for the search
    df_standardized['raw_file_path'] = str(raw_path) # Path to the saved raw file
    df_standardized['scrape_timestamp'] = datetime.now().isoformat()
    df_standardized['data_type'] = data_type # Explicitly label record type

    # --- Data Cleaning Steps ---
    # Convert amount columns to numeric, handling '$', ',', '()'
    amount_col = 'contribution_amount' if data_type == 'contributions' else 'expenditure_amount'
    if amount_col in df_standardized.columns:
        # Ensure it's string first, replace symbols, handle parentheses for negatives
        # Fix: convert the column to Series first, then apply string operations
        amount_series = df_standardized[amount_col].astype(str)
        df_standardized[amount_col] = amount_series.str.replace(r'[$,]', '', regex=True).str.strip()
        
        # Handle potential parentheses for negative numbers, e.g., (100.00) -> -100.00
        neg_mask = df_standardized[amount_col].str.startswith('(') & df_standardized[amount_col].str.endswith(')')
        # Apply conversion for negative numbers
        df_standardized.loc[neg_mask, amount_col] = '-' + df_standardized.loc[neg_mask, amount_col].str.slice(1, -1)
        # Convert to numeric, coercing errors to NaN
        df_standardized[amount_col] = pd.to_numeric(df_standardized[amount_col], errors='coerce')
        # Log rows where conversion failed
        failed_amount_conversions = df_standardized[amount_col].isna().sum() - df_standardized[amount_col].isnull().sum()
        if failed_amount_conversions > 0:
             logger.warning(f"Could not convert {amount_col} to numeric for {failed_amount_conversions} rows in {raw_path.name}.")
    
    # Convert date columns to datetime objects, coercing errors
    date_col = 'contribution_date' if data_type == 'contributions' else 'expenditure_date'
    if date_col in df_standardized.columns:
        original_date_type = df_standardized[date_col].dtype
        df_standardized[date_col] = pd.to_datetime(df_standardized[date_col], errors='coerce')
        # Log failed conversions if the column wasn't already datetime-like
        if not pd.api.types.is_datetime64_any_dtype(original_date_type):
             failed_date_conversions = df_standardized[date_col].isna().sum() - df_standardized[date_col].isnull().sum()
             if failed_date_conversions > 0:
                  logger.warning(f"Could not convert {date_col} to datetime for {failed_date_conversions} rows in {raw_path.name}.")

    # Clean string columns
    string_columns = df_standardized.select_dtypes(include='object').columns
    for col in string_columns:
        if col not in ['raw_file_path', 'data_source_url']: # Avoid stripping paths/URLs
            df_standardized[col] = df_standardized[col].str.strip()

    logger.info(f"Successfully processed and cleaned {len(df_standardized)} {data_type} records for '{source_search_term}' ({search_year}).")
    return df_standardized


# --- Main Orchestration Function ---
def run_finance_scrape(start_year: Optional[int] = None, end_year: Optional[int] = None, data_dir: Optional[Union[str, Path]] = None) -> Optional[Path]:
    """Main function to orchestrate scraping Idaho campaign finance data."""
    # Setup paths using the provided base directory or default from config
    paths = setup_project_paths(data_dir)

    # Ensure logger is set up correctly for this module
    global logger
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'])

    # Determine year range
    current_year = datetime.now().year
    # Default to current year only if not specified
    if start_year is None: start_year = current_year
    if end_year is None: end_year = current_year
    if start_year > end_year:
        logger.error(f"Start year ({start_year}) cannot be after end year ({end_year}).")
        return None

    logger.info(f"=== Starting Idaho Campaign Finance Scraping with Playwright ===")
    logger.info(f"Data Source: Idaho Sunshine Portal ({ID_FINANCE_BASE_URL})")
    logger.info(f"Target Years: {start_year}-{end_year}")
    logger.info(f"Base Data Directory: {paths['base']}")

    # --- Load Legislator Names for Searching ---
    legislators_file = paths['processed'] / 'legislators_ID.csv'
    if not legislators_file.is_file():
        logger.error(f"Processed legislators file not found: {legislators_file}")
        logger.error("Ensure LegiScan data (including legislators) has been collected and processed first.")
        return None

    try:
        # Load only necessary columns
        legislators_df = pd.read_csv(legislators_file, usecols=['name'])
        # Drop rows with missing names, get unique names
        search_targets = legislators_df['name'].dropna().unique().tolist()
        logger.info(f"Loaded {len(search_targets)} unique legislator names to search for from {legislators_file.name}.")
        if not search_targets:
             logger.error("No valid legislator names found in the legislator file. Cannot proceed.")
             return None
    except KeyError as e:
        logger.error(f"Required column '{e}' not found in legislators file: {legislators_file}. Check the file format.")
        return None
    except Exception as e:
        logger.error(f"Error reading legislators file {legislators_file}: {e}", exc_info=True)
        return None

    # --- Create necessary directories ---
    # Ensure raw campaign finance directory exists
    if 'raw_campaign_finance' not in paths:
        paths['raw_campaign_finance'] = paths['raw'] / 'campaign_finance'
        paths['raw_campaign_finance'].mkdir(parents=True, exist_ok=True)

    # --- Iterate and Scrape ---
    all_finance_data_dfs: List[pd.DataFrame] = []
    search_attempts = 0
    download_successes = 0
    download_failures = 0

    years_to_process = list(range(start_year, end_year + 1))
    # Use nested tqdm for better progress visibility
    for year in tqdm(years_to_process, desc="Processing Years", unit="year", position=0):
        logger.info(f"--- Processing Year: {year} ---")
        # Add inner tqdm for targets within a year
        for target_name in tqdm(search_targets, desc=f"Searching Targets ({year})", unit="target", position=1, leave=False):
            # Search for both contributions and expenditures for each target in this year
            for data_type in ['contributions', 'expenditures']:
                search_attempts += 1
                logger.debug(f"Attempting search: Type={data_type}, Target='{target_name}', Year={year}")

                # Polite wait before initiating search
                time.sleep(random.uniform(0.6, ID_FINANCE_DOWNLOAD_WAIT_SECONDS + 0.5))

                try:
                    # Call the Playwright search function
                    search_result = search_with_playwright(target_name, year, data_type, paths)

                    if search_result:
                        download_path, data_df = search_result
                        logger.info(f"Successfully downloaded data for {data_type}, '{target_name}', {year}")
                        
                        # Process the downloaded data
                        df_processed = download_and_extract_finance_data(
                            download_path, data_df, target_name, year, data_type, paths
                        )

                        if df_processed is not None and not df_processed.empty:
                            all_finance_data_dfs.append(df_processed)
                            download_successes += 1
                        else:
                            logger.warning(f"Processing failed for {data_type}, '{target_name}', {year}")
                            download_failures += 1
                    else:
                         logger.debug(f"No data found for {data_type}, '{target_name}', {year}")
                         # Not a download failure, just no data reported

                except Exception as e_scrape_loop:
                     logger.error(f"Unhandled error during scrape loop for {data_type}, '{target_name}', {year}: {e_scrape_loop}", exc_info=True)
                     download_failures += 1
                
                # Small delay between searches
                time.sleep(random.uniform(0.2, 0.6))

            # Wait a bit longer between different legislators/committees
            time.sleep(random.uniform(0.5, 1.5))

    # --- Consolidate and Save Results ---
    logger.info(f"--- Idaho Finance Scraping Finished ({start_year}-{end_year}) ---")
    logger.info(f"Total search attempts (Target*Year*Type): {search_attempts}")
    logger.info(f"Successful data extractions (non-empty): {download_successes}")
    logger.info(f"Failed downloads or processing errors: {download_failures}")

    if not all_finance_data_dfs:
        logger.warning("No campaign finance data was successfully collected or extracted.")
        return None

    final_output_path: Optional[Path] = None
    try:
        # Concatenate all collected DataFrames
        logger.info(f"Consolidating {len(all_finance_data_dfs)} collected finance dataframes...")
        consolidated_df = pd.concat(all_finance_data_dfs, ignore_index=True, sort=False)
        total_records = len(consolidated_df)
        logger.info(f"Consolidated a total of {total_records} finance records.")

        if total_records == 0:
             logger.warning("Consolidation resulted in an empty DataFrame, although individual extractions were reported.")
             return None

        # Define output file path in the 'processed' directory
        output_filename = f'finance_ID_consolidated_{start_year}-{end_year}.csv'
        output_file = paths['processed'] / output_filename

        # Save the consolidated data
        records_list = consolidated_df.to_dict('records')

        # Define expected columns for the final CSV
        final_columns = sorted(list(
            set(CONTRIBUTION_COLUMN_MAP.values()) |
            set(EXPENDITURE_COLUMN_MAP.values()) |
            {'source_search_term', 'data_source_url', 'scrape_year', 'raw_file_path', 'scrape_timestamp', 'data_type'}
        ))

        num_saved = convert_to_csv(records_list, output_file, columns=final_columns)

        if num_saved == total_records:
            logger.info(f"Successfully saved {num_saved} consolidated finance records to: {output_file}")
            final_output_path = output_file
        else:
            logger.error(f"Mismatch saving consolidated data. Expected {total_records}, saved {num_saved}. Check logs and output file: {output_file}")
            final_output_path = output_file if output_file.exists() else None

    except pd.errors.InvalidIndexError as e_concat_cols:
        logger.error(f"Error during concatenation, likely due to duplicate column names: {e_concat_cols}", exc_info=True)
    except Exception as e_concat:
        logger.error(f"Error consolidating or saving final finance data: {e_concat}", exc_info=True)

    return final_output_path

# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape campaign finance data from Idaho Secretary of State website (Sunshine Portal) using Playwright.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--start-year', type=int, default=None,
                        help='Start year for data collection (default: current year)')
    parser.add_argument('--end-year', type=int, default=None,
                        help='End year for data collection (default: current year)')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data from config/utils)')
    parser.add_argument('--debug', action='store_true',
                        help='Run browser in headful mode for debugging')

    args = parser.parse_args()

    # --- Standalone Setup ---
    try:
        paths = setup_project_paths(args.data_dir)
    except SystemExit:
        sys.exit(1)

    # Setup logging
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'])

    # --- Run the main scraping logic ---
    final_output = None
    try:
        final_output = run_finance_scrape(
            start_year=args.start_year,
            end_year=args.end_year,
            data_dir=paths['base']
        )

        if final_output and final_output.exists():
            print(f"\nFinance scraping finished successfully.")
            print(f"Output file: {final_output}")
            exit_code = 0
        elif final_output:
            print(f"\nFinance scraping finished, but the expected output file was not found: {final_output}")
            exit_code = 1
        else:
            print("\nFinance scraping finished but produced no output or encountered errors.")
            exit_code = 1

    except Exception as e:
        logger.critical(f"Critical unhandled error during standalone finance scraping execution: {e}", exc_info=True)
        exit_code = 2
    finally:
        logging.shutdown()
        sys.exit(exit_code)
