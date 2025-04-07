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

# --- Helper Functions ---
def standardize_columns(df: pd.DataFrame, column_map: Dict[str, List[str]]) -> pd.DataFrame:
    """Standardizes DataFrame columns based on a mapping, ensuring all standard columns exist.
    
    Args:
        df: Input DataFrame to standardize
        column_map: Dictionary mapping standard column names to possible variations
        
    Returns:
        DataFrame with standardized column names and all required columns present
    """
    # Ensure DataFrame is not empty
    if df.empty:
        logger.warning("Attempted to standardize columns on an empty DataFrame.")
        # Return DataFrame with standard columns but no data
        return pd.DataFrame(columns=list(column_map.keys()))

    original_columns = df.columns.tolist() # Keep original for logging
    # Clean column names: lowercase, strip whitespace, remove trailing colons
    df.columns = df.columns.str.lower().str.strip().str.replace(r':$', '', regex=True)
    rename_dict = {}
    found_standard_names = set()

    for standard_name, variations in column_map.items():
        for var in variations:
            if var in df.columns:
                if standard_name not in found_standard_names:
                    # Only map if the standard name hasn't been mapped yet
                    rename_dict[var] = standard_name
                    found_standard_names.add(standard_name)
                    logger.debug(f"Mapping source column '{var}' to standard '{standard_name}'")
                    break # Use first matching variation found
            # else: logger.debug(f"Variation '{var}' not found in cleaned columns: {df.columns.tolist()}")

    # Log unmapped columns after cleaning
    current_cols_set = set(df.columns)
    mapped_source_cols_set = set(rename_dict.keys())
    unmapped_cols = list(current_cols_set - mapped_source_cols_set)
    if unmapped_cols:
         logger.warning(f"Unmapped columns found after cleaning/mapping: {unmapped_cols}. Check column_map or source data. Original columns: {original_columns}")

    # Perform renaming
    df = df.rename(columns=rename_dict)

    # Ensure all standard columns exist, add missing ones with pd.NA
    standard_columns_list = list(column_map.keys())
    added_cols = []
    for standard_name in standard_columns_list:
        if standard_name not in df.columns:
            df[standard_name] = pd.NA # Use pandas NA type
            added_cols.append(standard_name)
    if added_cols:
         logger.debug(f"Added missing standard columns: {added_cols}")

    # Return DataFrame with only the standard columns in the defined order
    # Filter final_columns to include only those that actually exist in df (should be all after adding missing)
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
    data_type: str, # 'contributions' or 'expenditures'
    paths: Dict[str, Path] # Pass paths dict for saving debug files
    ) -> Optional[str]:
    """
    Searches the Idaho Sunshine Portal for a finance data download link.
    Handles ASP.NET form submission with ViewState.

    Args:
        search_term: Name of legislator or committee.
        year: Election year or reporting year.
        data_type: 'contributions' or 'expenditures'.
        paths: Project paths dictionary for potential debug output.

    Returns:
        Absolute URL string for the download link, or None if not found or error.
    """
    # Use only the base URL as the search page entry point
    search_page_url = ID_FINANCE_BASE_URL 
    logger.info(f"Initiating finance search: Term='{search_term}', Year={year}, Type={data_type}, URL={search_page_url}")

    # Use a session object to handle cookies potentially set by the server
    session = requests.Session()
    # Set headers common for browser mimicry
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36', # Updated UA
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': ID_FINANCE_BASE_URL, # Set base referer
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin', # Usually 'same-origin' after first page load
        'Sec-Fetch-User': '?1',
    })

    try:
        # --- Step 1: Initial GET to load the search page and get form state ---
        logger.debug(f"Fetching initial search page to get form state: {search_page_url}")
        try:
            initial_response = session.get(search_page_url, timeout=45, allow_redirects=True)
            initial_response.raise_for_status() # Check for HTTP errors (4xx, 5xx)
            logger.debug(f"Initial GET successful (Status: {initial_response.status_code}, URL: {initial_response.url})")
        except requests.exceptions.RequestException as e_get:
            logger.error(f"Failed to fetch initial search page {search_page_url}: {e_get}")
            return None

        initial_soup = BeautifulSoup(initial_response.text, 'html.parser')
        hidden_fields = get_hidden_form_fields(initial_soup)
        # Check if critical fields were extracted (get_hidden_form_fields logs errors)
        if not hidden_fields or '__VIEWSTATE' not in hidden_fields:
            logger.error("Aborting search due to missing critical hidden form fields.")
            # Save initial page HTML for debugging
            debug_path = paths.get('artifacts', paths['base'] / 'artifacts') / 'debug'
            debug_path.mkdir(parents=True, exist_ok=True)
            debug_file = debug_path / f"initial_search_page_error_{time.strftime('%Y%m%d%H%M%S')}.html"
            try:
                 debug_file.write_text(initial_response.text, encoding='utf-8', errors='replace')
                 logger.info(f"Saved initial search page HTML (missing fields) for debugging to: {debug_file}")
            except Exception as e_save:
                 logger.error(f"Failed to save initial debug HTML: {e_save}")
            return None

        # --- Step 2: Construct the POST data payload ---
        # Field names require VERIFICATION by inspecting the actual form in a browser.
        # These are common patterns but may change.
        form_data = hidden_fields.copy() # Start with ViewState etc.

        # --- VERIFY THESE FIELD NAMES ---
        # Candidate/Committee Name Input: (e.g., ctl00$DefaultContent$CampaignSearch$txtName)
        form_data['ctl00$DefaultContent$CampaignSearch$txtName'] = search_term
        # Year Input: (e.g., ctl00$DefaultContent$CampaignSearch$txtYear or ...ddlYear)
        form_data['ctl00$DefaultContent$CampaignSearch$txtYear'] = str(year)
        # Search Button: (e.g., ctl00$DefaultContent$CampaignSearch$btnSearch) - Value is often important
        form_data['ctl00$DefaultContent$CampaignSearch$btnSearch'] = 'Search' # The 'value' attribute of the button

        # Other potential fields (Verify if they exist and are needed):
        # - Radio buttons for search type (Candidate/Committee)
        # - Dropdowns for office type
        # - Checkboxes for report types
        # form_data['ctl00$DefaultContent$CampaignSearch$rblSearchType'] = 'candCmte' # Example value for candidate/committee radio

        # Log crucial parts of form data, masking ViewState if too long
        log_form_data = {k: (v[:50] + '...' if isinstance(v, str) and len(v) > 100 else v)
                         for k, v in form_data.items()}
        logger.debug(f"Constructed POST data (sample): {log_form_data}")

        # --- Step 3: Make the POST request to submit the search ---
        logger.info(f"Submitting search POST request to {search_page_url}...")
        try:
            post_response = session.post(
                search_page_url,
                data=form_data,
                timeout=75, # Allow longer timeout for search processing
                allow_redirects=True,
                 # Update Referer to the page we are posting from
                 headers={'Referer': initial_response.url} # Use the URL we actually fetched
            )
            post_response.raise_for_status() # Check for HTTP errors on POST response
            logger.debug(f"Search POST request successful (Status: {post_response.status_code}, Final URL: {post_response.url})")

            # Optional: Check if final URL suggests an error page pattern
            if "error" in post_response.url.lower() or "login" in post_response.url.lower():
                 logger.warning(f"POST request resulted in a potential error/login page: {post_response.url}")

        except requests.exceptions.RequestException as e_post:
            logger.error(f"Search POST request failed for '{search_term}' ({year}): {e_post}")
            return None

        # --- Step 4: Parse the response HTML for error messages and the download link ---
        results_soup = BeautifulSoup(post_response.text, 'html.parser')

        # Check for explicit error messages on the results page
        # Look for common ASP.NET validation summary controls or specific error divs/spans
        # Example Selectors (ADJUST BASED ON INSPECTION):
        # error_selectors = ['#ctl00_ContentPlaceHolder1_ValidationSummary1', '.error-message', 'span.error']
        error_tags = results_soup.select('div[id*="ValidationSummary"], div[class*="error"], span[class*="error"]')
        error_text_found = None
        for tag in error_tags:
             text = tag.get_text(" ", strip=True)
             if text and len(text) > 5: # Ignore empty or very short tags
                  error_text_found = text
                  break # Use the first significant error message found

        if error_text_found:
            # Check for "no records found" specifically
            if "no records found" in error_text_found.lower() or "no results found" in error_text_found.lower():
                 logger.info(f"Search successful but returned 'No Records Found' for '{search_term}', {year}.")
                 # This is not an error, just no data for this search. Return None.
                 return None
            else:
                 # Log other errors as warnings, as sometimes pages show errors but still contain data links
                 logger.warning(f"Found potential error/validation message on results page: {error_text_found[:250]}...")

        # Attempt to find the specific export link (CSV preferred)
        export_url = find_export_link(results_soup, data_type, ID_FINANCE_BASE_URL)

        if export_url:
             logger.info(f"Successfully found export URL for '{search_term}', {year}, {data_type}")
             return export_url
        else:
             logger.warning(f"Could not find download link for '{search_term}', {year}, {data_type} on results page.")
             # Save the search results HTML for debugging if link not found
             # Use artifacts directory from paths dict
             debug_path = paths.get('artifacts', paths['base'] / 'artifacts') / 'debug'
             debug_path.mkdir(parents=True, exist_ok=True)
             # Sanitize search term for filename
             safe_term = "".join(c if c.isalnum() else '_' for c in search_term)[:50].strip('_')
             debug_file = debug_path / f"search_results_{safe_term}_{year}_{data_type}_{time.strftime('%Y%m%d%H%M%S')}.html"
             try:
                  debug_file.write_text(post_response.text, encoding='utf-8', errors='replace')
                  logger.warning(f"Saved search results HTML (link not found) for debugging to: {debug_file}")
             except Exception as e_save:
                  logger.error(f"Failed to save debug HTML: {e_save}")
             return None # Indicate link not found

    # Catch-all for unexpected errors during the process
    except Exception as e:
        logger.error(f"Unexpected error during finance search process for '{search_term}' ({year}, {data_type}): {e}", exc_info=True)
        return None

# --- Data Downloading and Processing ---
def download_and_extract_finance_data(
    download_url: str,
    source_search_term: str, # e.g., legislator name used in search
    search_year: int,
    data_type: str, # 'contributions' or 'expenditures'
    paths: Dict[str, Path]
) -> Optional[pd.DataFrame]:
    """Downloads, processes, standardizes, and cleans finance data (assumes CSV or detectable)."""
    logger.info(f"Attempting download: Type='{data_type}', Term='{source_search_term}', Year={search_year}")
    logger.debug(f"Download URL: {download_url}")

    # Use utils.fetch_page for robust GET request with retries
    # Request as bytes initially to better handle different encodings or non-text files
    file_content_bytes = fetch_page(download_url, timeout=120, return_bytes=True) # Longer timeout for downloads

    if file_content_bytes is None: # Check if fetch_page failed after retries
        logger.error(f"Failed to download content from {download_url} after retries.")
        return None
    if len(file_content_bytes) < 100: # Check for suspiciously small files (using bytes length)
        logger.warning(f"Downloaded file from {download_url} is very small ({len(file_content_bytes)} bytes). May be empty or an error page.")
        # Try decoding briefly to check if it's HTML
        try:
            decoded_start = file_content_bytes[:200].decode('utf-8', errors='ignore').strip().lower()
            if decoded_start.startswith(('<!doctype html', '<html', '<head', '<body')):
                logger.error(f"Downloaded content appears to be HTML, not data. URL: {download_url}")
                # Save HTML error page for debugging
                error_html_path = paths.get('artifacts', paths['base'] / 'artifacts') / 'debug' / f"download_error_{data_type}_{search_year}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
                error_html_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with open(error_html_path, 'wb') as f_err: f_err.write(file_content_bytes)
                    logger.info(f"Saved potential error HTML to: {error_html_path}")
                except Exception as e_save: logger.error(f"Failed to save error HTML: {e_save}")
                return None
        except Exception:
             pass # Ignore decoding errors for this check

    # --- Save Raw Content ---
    # Define raw file path (state assumed to be Idaho, add year subdirectory)
    raw_dir = paths['raw_campaign_finance'] / 'idaho' / str(search_year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    # Sanitize search term for filename
    safe_search_term = "".join(c if c.isalnum() else '_' for c in source_search_term)[:50].strip('_')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    # Try to guess extension, default to .raw
    file_ext = '.csv' # Assume CSV by default from Sunshine Portal links
    # Could add logic here to sniff content type if needed (e.g., check for Excel magic bytes)
    raw_filename = f"{data_type}_{safe_search_term}_{search_year}_{timestamp}{file_ext}"
    raw_path = raw_dir / raw_filename

    try:
        with open(raw_path, 'wb') as f_raw:
            f_raw.write(file_content_bytes)
        logger.info(f"Saved raw downloaded content ({len(file_content_bytes)} bytes) to {raw_path}")
    except Exception as e:
        logger.error(f"Error saving raw content to {raw_path}: {e}")
        # Decide whether to continue processing despite save failure
        # return None # Option: Fail if raw cannot be saved
        logger.warning("Continuing processing despite failure to save raw file.")

    # --- Attempt to Process Data (primarily as CSV) ---
    df = None
    detected_encoding = None
    try:
        # Use io.BytesIO to read the bytes content as a file-like object for pandas
        file_buffer = io.BytesIO(file_content_bytes)

        # Attempt 1: Try UTF-8
        try:
            df = pd.read_csv(
                file_buffer,
                encoding='utf-8',
                low_memory=False,
                on_bad_lines='warn', # Report lines that cause parsing issues
                # quoting=csv.QUOTE_MINIMAL, # Adjust quoting if necessary
                # escapechar='\\', # Adjust escape character if necessary
            )
            detected_encoding = 'utf-8'
            logger.debug(f"Successfully read CSV data using utf-8 for {raw_path.name}")
        except (UnicodeDecodeError, pd.errors.ParserError) as e_utf8:
            logger.warning(f"UTF-8 CSV parsing failed for {raw_path.name}: {str(e_utf8)[:200]}. Trying latin-1...")
            # Reset buffer position for next read attempt
            file_buffer.seek(0)
            # Attempt 2: Try Latin-1 (common fallback)
            try:
                df = pd.read_csv(file_buffer, encoding='latin-1', low_memory=False, on_bad_lines='warn')
                detected_encoding = 'latin-1'
                logger.debug(f"Successfully read CSV data using latin-1 for {raw_path.name}")
            except (pd.errors.ParserError, Exception) as e_latin1:
                logger.error(f"Latin-1 CSV parsing also failed for {raw_path.name}: {str(e_latin1)[:200]}")
                # --- Placeholder for Excel Reading ---
                # If CSV fails consistently and Excel is a possibility:
                # logger.info("Attempting Excel parsing...")
                # try:
                #     file_buffer.seek(0)
                #     df = pd.read_excel(file_buffer, engine='openpyxl') # Requires openpyxl installed
                #     detected_encoding = 'excel'
                #     logger.info(f"Successfully read Excel data for {raw_path.name}")
                # except Exception as e_excel:
                #     logger.error(f"Excel parsing also failed for {raw_path.name}: {e_excel}")
                #     return None # Give up if all attempts fail
                # --- End Excel Placeholder ---
                return None # Give up if CSV fails and Excel not implemented/failed

    except Exception as e_read:
         # Catch other potential errors during file reading setup (e.g., memory issues?)
         logger.error(f"Unexpected error during data processing setup for {raw_path.name}: {e_read}", exc_info=True)
         return None

    # Check if DataFrame is empty after successful read
    if df is None or df.empty:
        logger.warning(f"Parsing successful (encoding: {detected_encoding}), but resulted in an empty DataFrame for {raw_path.name}.")
        # Optionally return empty dataframe instead of None if needed downstream
        # return pd.DataFrame()
        return None # Treat as no data extracted

    # --- Standardize Columns ---
    logger.debug(f"Standardizing {len(df)} rows for {data_type} from {raw_path.name}...")
    if data_type == 'contributions':
        df_standardized = standardize_columns(df.copy(), CONTRIBUTION_COLUMN_MAP)
    elif data_type == 'expenditures':
        df_standardized = standardize_columns(df.copy(), EXPENDITURE_COLUMN_MAP)
    else:
        # This case should ideally not be reached if called correctly
        logger.error(f"Invalid data_type '{data_type}' provided for column standardization.")
        return None

    # --- Add Metadata Columns ---
    df_standardized['source_search_term'] = source_search_term
    df_standardized['data_source_url'] = download_url
    df_standardized['scrape_year'] = search_year # Year used for the search
    df_standardized['raw_file_path'] = str(raw_path) # Path to the saved raw file
    df_standardized['scrape_timestamp'] = datetime.now().isoformat()
    df_standardized['data_type'] = data_type # Explicitly label record type

    # --- Data Cleaning Steps ---
    # Convert amount columns to numeric, handling '$', ',', '()'
    amount_col = 'contribution_amount' if data_type == 'contributions' else 'expenditure_amount'
    if amount_col in df_standardized.columns:
        # Ensure it's string first, replace symbols, handle parentheses for negatives
        # Regex: Remove $, ,, strip whitespace. Handle (num) -> -num pattern separately if needed.
        df_standardized[amount_col] = df_standardized[amount_col].astype(str).str.replace(r'[$,]', '', regex=True).str.strip()
        # Handle potential parentheses for negative numbers, e.g., (100.00) -> -100.00
        neg_mask = df_standardized[amount_col].str.startswith('(') & df_standardized[amount_col].str.endswith(')')
        # Apply conversion for negative numbers
        df_standardized.loc[neg_mask, amount_col] = '-' + df_standardized.loc[neg_mask, amount_col].str.slice(1, -1)
        # Convert to numeric, coercing errors to NaN (which becomes <NA> for Int/Float dtypes)
        df_standardized[amount_col] = pd.to_numeric(df_standardized[amount_col], errors='coerce')
        # Optional: Log rows where conversion failed
        failed_amount_conversions = df_standardized[amount_col].isna().sum() - df_standardized[amount_col].isnull().sum() # Count NaNs introduced by coerce
        if failed_amount_conversions > 0:
             logger.warning(f"Could not convert {amount_col} to numeric for {failed_amount_conversions} rows in {raw_path.name}.")
        # Optional: Fill NaNs if appropriate (e.g., with 0, but NA is usually better)
        # df_standardized[amount_col] = df_standardized[amount_col].fillna(0)

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
        # Optional: Format date for consistency if needed (e.g., remove time part)
        # df_standardized[date_col] = df_standardized[date_col].dt.date

    # Add other cleaning steps:
    # - Trim whitespace from string columns
    # - Standardize state abbreviations (e.g., 'ID', 'Id', 'idaho' -> 'ID')
    # - Validate ZIP codes (basic format check)
    # - Clean/standardize contribution/expenditure types if needed
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
    # Ensure paths are set up before logging is fully configured if base_dir changes log path
    paths = setup_project_paths(data_dir)

    # Ensure logger is set up correctly for this module (might be redundant if main.py called setup_logging first)
    # This ensures it works standalone correctly. setup_logging handles multiple calls gracefully.
    global logger # Make sure we're using the module-level logger
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'])

    # Determine year range
    current_year = datetime.now().year
    # Default to current year only if not specified
    if start_year is None: start_year = current_year
    if end_year is None: end_year = current_year
    if start_year > end_year:
        logger.error(f"Start year ({start_year}) cannot be after end year ({end_year}).")
        return None

    logger.info(f"=== Starting Idaho Campaign Finance Scraping ===")
    logger.info(f"Data Source: Idaho Sunshine Portal ({ID_FINANCE_BASE_URL})")
    logger.info(f"Target Years: {start_year}-{end_year}")
    logger.info(f"Base Data Directory: {paths['base']}")
    logger.warning("Finance scraping depends heavily on website structure and form field names (e.g., 'ctl00$...') - verify these if scraping fails.")

    # --- Load Legislator Names for Searching ---
    # Use the processed legislators CSV which should be more stable
    legislators_file = paths['processed'] / 'legislators_ID.csv' # Assumes state is 'ID'
    if not legislators_file.is_file():
        logger.error(f"Processed legislators file not found: {legislators_file}")
        logger.error("Ensure LegiScan data (including legislators) has been collected and processed first.")
        return None

    try:
        # Load only necessary columns: 'name' (for searching), potentially 'legislator_id' (for linking later)
        legislators_df = pd.read_csv(legislators_file, usecols=['name']) # Add 'legislator_id' if needed
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

    # --- Iterate and Scrape ---
    all_finance_data_dfs: List[pd.DataFrame] = [] # Explicitly type list
    search_attempts = 0
    download_links_found = 0
    download_successes = 0
    download_failures = 0 # Includes failed downloads and empty/failed processing

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
                time.sleep(random.uniform(0.6, ID_FINANCE_DOWNLOAD_WAIT_SECONDS + 0.5)) # Slightly longer base wait

                try:
                    # Call the search function to get the download URL
                    # Pass the 'paths' dict needed for saving debug files if link not found
                    download_link = search_for_finance_data_link(target_name, year, data_type, paths)

                    if download_link:
                        download_links_found += 1
                        # Wait a bit more before hitting the download link itself
                        time.sleep(random.uniform(0.8, ID_FINANCE_DOWNLOAD_WAIT_SECONDS + 1.0))

                        # Download and process the data file
                        df_processed = download_and_extract_finance_data(
                            download_link, target_name, year, data_type, paths
                        )

                        if df_processed is not None and not df_processed.empty:
                            all_finance_data_dfs.append(df_processed)
                            download_successes += 1
                        elif df_processed is None:
                            # download_and_extract handles logging errors
                            logger.warning(f"Download/processing failed for {data_type}, '{target_name}', {year}. Link: {download_link}")
                            download_failures += 1
                        else: # df_processed is an empty DataFrame
                            logger.info(f"Processing resulted in empty data for {data_type}, '{target_name}', {year}. Link: {download_link}")
                            # Don't count as failure, but maybe track separately?
                            # download_failures += 1 # Option: count empty files as failures
                    else:
                         # search_for_finance_data_link handles logging reasons for no link
                         logger.debug(f"No download link found for {data_type}, '{target_name}', {year}.")
                         # Not a download failure, just no link found/no data reported

                except Exception as e_scrape_loop:
                     # Catch unexpected errors in the main loop iteration
                     logger.error(f"Unhandled error during scrape loop for {data_type}, '{target_name}', {year}: {e_scrape_loop}", exc_info=True)
                     download_failures += 1 # Count errors as failures

                # Small delay between contribution/expenditure searches for the same target/year
                # time.sleep(random.uniform(0.2, 0.6)) # Reduced slightly as waits are within loop

            # Wait a bit longer between different legislators/committees within the same year
            # time.sleep(random.uniform(0.5, 1.5)) # Reduced slightly

    # --- Consolidate and Save Results ---
    logger.info(f"--- Idaho Finance Scraping Finished ({start_year}-{end_year}) ---")
    logger.info(f"Total search attempts (Target*Year*Type): {search_attempts}")
    logger.info(f"Download links found: {download_links_found}")
    logger.info(f"Successful data extractions (non-empty): {download_successes}")
    logger.info(f"Failed/empty downloads or processing errors: {download_failures}")

    if not all_finance_data_dfs:
        logger.warning("No campaign finance data was successfully collected or extracted.")
        # Create empty placeholder file? Or just return None? Returning None seems cleaner.
        return None # Return None if nothing was collected

    final_output_path: Optional[Path] = None
    try:
        # Concatenate all collected DataFrames
        logger.info(f"Consolidating {len(all_finance_data_dfs)} collected finance dataframes...")
        # Use sort=False to prevent pandas from sorting columns alphabetically
        # Use join='outer' if schemas might slightly differ (though standardize_columns aims to prevent this)
        consolidated_df = pd.concat(all_finance_data_dfs, ignore_index=True, sort=False) #, join='outer' )
        total_records = len(consolidated_df)
        logger.info(f"Consolidated a total of {total_records} finance records.")

        if total_records == 0:
             logger.warning("Consolidation resulted in an empty DataFrame, although individual extractions were reported.")
             return None

        # Define output file path in the 'processed' directory
        # Include state and year range in filename for consistency with data_collection.py
        output_filename = f'finance_ID_consolidated_{start_year}-{end_year}.csv'
        output_file = paths['processed'] / output_filename

        # Save the consolidated data using utils.convert_to_csv
        # convert_to_csv expects a list of dicts
        # Convert pd.NA to None for broader compatibility if saving JSON later, but fine for CSV.
        # Using df.where(pd.notna(df), None) is safer than fillna(None)
        # records_list = consolidated_df.where(pd.notna(consolidated_df), None).to_dict('records')
        records_list = consolidated_df.to_dict('records') # Keep pd.NA for CSV saving consistency

        # Define expected columns for the final CSV based on combined maps + metadata
        final_columns = sorted(list(
            set(CONTRIBUTION_COLUMN_MAP.keys()) |
            set(EXPENDITURE_COLUMN_MAP.keys()) |
            {'source_search_term', 'data_source_url', 'scrape_year', 'raw_file_path', 'scrape_timestamp', 'data_type'}
        ))

        num_saved = convert_to_csv(records_list, output_file, columns=final_columns)

        if num_saved == total_records:
            logger.info(f"Successfully saved {num_saved} consolidated finance records to: {output_file}")
            final_output_path = output_file # Store path to return
        else:
            # convert_to_csv logs errors internally
            logger.error(f"Mismatch saving consolidated data via convert_to_csv. Expected {total_records}, reported save count {num_saved}. Check logs and output file: {output_file}")
            # Return path if file exists, otherwise None
            final_output_path = output_file if output_file.exists() else None

    except pd.errors.InvalidIndexError as e_concat_cols:
        logger.error(f"Error during concatenation, likely due to duplicate column names before standardization: {e_concat_cols}", exc_info=True)
    except Exception as e_concat:
        logger.error(f"Error consolidating or saving final finance data: {e_concat}", exc_info=True)

    return final_output_path


# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape campaign finance data from Idaho Secretary of State website (Sunshine Portal).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show defaults
    )
    parser.add_argument('--start-year', type=int, default=None, # Default handled in run_finance_scrape
                        help='Start year for data collection (default: current year)')
    parser.add_argument('--end-year', type=int, default=None, # Default handled in run_finance_scrape
                        help='End year for data collection (default: current year)')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data from config/utils)')

    args = parser.parse_args()

    # --- Standalone Setup ---
    # Setup paths early to ensure log directory exists if overridden
    # Use try-except for path setup as it can exit
    try:
        paths = setup_project_paths(args.data_dir)
    except SystemExit: # Catch sys.exit called by setup_project_paths on critical error
        # Error logged by setup_project_paths
        sys.exit(1) # Exit script

    # Setup logging specifically for this standalone run
    # Pass the configured paths to ensure logs go to the correct place
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'])

    # --- Run the main scraping logic ---
    final_output = None
    try:
        # Pass arguments directly to the main logic function
        final_output = run_finance_scrape(
            start_year=args.start_year,
            end_year=args.end_year,
            data_dir=paths['base'] # Pass the resolved base path
        )

        if final_output and final_output.exists():
            print(f"\nFinance scraping finished successfully.")
            print(f"Output file: {final_output}")
            exit_code = 0 # Success
        elif final_output:
            print(f"\nFinance scraping finished, but the expected output file was not found: {final_output}")
            exit_code = 1 # Failure state
        else:
            print("\nFinance scraping finished but produced no output or encountered errors.")
            exit_code = 1 # Failure state

    except Exception as e:
        logger.critical(f"Critical unhandled error during standalone finance scraping execution: {e}", exc_info=True)
        exit_code = 2 # Critical failure
    finally:
        logging.shutdown() # Ensure all logs are flushed before exiting
        sys.exit(exit_code)
