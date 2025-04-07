#!/usr/bin/env python3
"""
Test script for validating components of the Idaho finance scraper.

Includes functions to:
- Inspect form fields on the search page.
- Inspect the structure of the search results page.
- Run predefined search test cases.
"""
import argparse
import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
# Remove requests and BeautifulSoup imports if no longer needed after full refactor
# import requests
# from bs4 import BeautifulSoup 
from tqdm import tqdm
# Import Playwright
from playwright.sync_api import sync_playwright, expect, Page, TimeoutError as PlaywrightTimeoutError

# Local imports
from src.config import (
    ID_FINANCE_BASE_URL,
    DEFAULT_BASE_DATA_DIR,
    ID_FINANCE_DOWNLOAD_WAIT_SECONDS, # Keep for potential delays
    FINANCE_SCRAPE_LOG_FILE
)
# Import FINANCE_COLUMN_MAPS from data_collection, not config
from src.data_collection import FINANCE_COLUMN_MAPS
from src.utils import setup_logging, setup_project_paths
# Keep these imports for now, as test_search_functionality might still use them
# or could be refactored later to use Playwright as well
from src.scrape_finance_idaho import (
    search_for_finance_data_link,
    download_and_extract_finance_data,
    standardize_columns,
    # These might become obsolete with Playwright or need internal refactoring
    # get_hidden_form_fields, 
    # find_export_link
)

# --- Configure Logging ---
# Use __name__ for logger to reflect the module
logger = logging.getLogger(__name__)

# --- Helper: Safely interact with Playwright page ---
def safe_goto(page: Page, url: str):
    """Navigate to a URL with error handling."""
    try:
        logger.debug(f"Navigating to {url}")
        response = page.goto(url, wait_until='domcontentloaded', timeout=60000) # Wait for DOM load
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
    # Ensure paths['artifacts'] is a Path object if it comes from setup_project_paths
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

# --- Test Functions (Refactored for Playwright) ---

def inspect_form_fields(base_url: str, paths: Dict[str, Path]):
    """Uses Playwright to inspect form fields on the search page."""
    logger.info(f"Inspecting form fields at {base_url} using Playwright")
    found_fields = {}
    # The specific selector for the react-select input might change. Needs verification.
    # Use a more general approach first, then try the specific one if needed.
    # Example: Look for inputs inside divs with class containing 'select'
    general_input_selector = 'div[class*="select"] input[id^="react-select-"]'
    specific_input_selector = 'input#react-select-1106-input' # As previously identified

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch() # Consider headless=False for debugging
            page = browser.new_page()
            
            if not safe_goto(page, base_url):
                browser.close()
                return

            logger.info("Page loaded. Waiting for potential dynamic content...")
            page.wait_for_timeout(3000) # Give JS time to potentially load/render components

            # --- Attempt to interact with the React-Select component --- 
            logger.info(f"Looking for potential React-Select input using general selector: {general_input_selector}")
            input_element = None
            try:
                # Wait for the input field matching the general pattern
                input_elements = page.locator(general_input_selector)
                input_count = input_elements.count()
                logger.info(f"Found {input_count} potential React-Select inputs with general selector.")

                if input_count > 0:
                    input_element = input_elements.first # Assume first one is for name search
                    actual_id = input_element.get_attribute('id')
                    logger.info(f"Found input element with ID: {actual_id}")
                    
                    # Try focusing the element
                    logger.info(f"Attempting to focus input element (ID: {actual_id})...")
                    # Use JavaScript focus as it might work even if obscured
                    page.evaluate(f"document.querySelector('#{actual_id}').focus()")
                    page.wait_for_timeout(1000) 
                    logger.info("Focus attempted. Now inspecting surrounding elements.")

                    # Check visibility after focus attempt
                    is_visible = input_element.is_visible()
                    logger.info(f"Input element (ID: {actual_id}) visible after focus? {is_visible}")
                    found_fields[actual_id or 'react_select_input'] = {
                        'selector': f'#{actual_id}',
                        'visible_after_focus': is_visible,
                        'tag': 'input'
                    }

                    # Look for potential dropdown/options list
                    # Selector needs verification by inspecting the live site *after* focusing
                    options_list_selector = f'div[id^="react-select-"][class*="menu"], div[id^="react-select-"][class*="options"]'
                    options_container = page.locator(options_list_selector).first 
                    try:
                        options_container.wait_for(state='visible', timeout=5000) 
                        logger.info(f"Found potential options container matching: {options_list_selector}")
                        options = options_container.locator('div[class*="-option"]')
                        count = options.count()
                        logger.info(f"Found {count} potential options within the container.")
                        found_fields[actual_id + '_options' or 'react_select_options'] = {
                            'container_selector': options_list_selector,
                            'options_selector': 'div[class*="-option"]',
                            'count': count
                        }
                    except PlaywrightTimeoutError:
                        logger.info(f"Did not find a visible options container ({options_list_selector}) after focus.")
                else:
                    logger.warning(f"Could not find input matching general selector: {general_input_selector}. Trying specific: {specific_input_selector}")
                    # Try the specific selector if the general one failed
                    input_element = page.locator(specific_input_selector)
                    input_element.wait_for(state='attached', timeout=5000)
                    logger.info(f"Found input via specific selector: {specific_input_selector}")
                    # Repeat focus and check steps if needed... (omitted for brevity)

            except PlaywrightTimeoutError:
                logger.error(f"Timeout waiting for target input element using selectors.")
                save_debug_html(page, "inspect_form_timeout", paths)
            except Exception as e_interact:
                 logger.error(f"Error interacting with target input element: {e_interact}")
                 save_debug_html(page, "inspect_form_interact_error", paths)

            # --- Find other potential input fields (e.g., for Year) ---
            # Look for inputs related to 'year', 'date', common date range pickers
            logger.info("Searching for other potential input fields (Year, Date Range, Buttons)...")
            # Example selectors (GUESSES - NEED VERIFICATION)
            year_selectors = ['input[name*="year"]', 'input[placeholder*="Year"]', 'select[name*="year"]']
            date_range_selectors = ['input[class*="date"]', 'div[class*="daterange"]']
            button_selectors = ['button', 'input[type="submit"]', 'input[type="button"]']

            for field_type, selectors in [("Year", year_selectors), ("Date Range", date_range_selectors), ("Button", button_selectors)]:
                logger.debug(f"Checking for {field_type} fields using: {selectors}")
                elements = page.locator(", ".join(selectors))
                count = elements.count()
                if count > 0:
                    logger.info(f"Found {count} potential {field_type} elements.")
                    for i in range(min(count, 5)): # Log details for first few
                        element = elements.nth(i)
                        tag_name = element.evaluate('el => el.tagName.toLowerCase()')
                        el_id = element.get_attribute('id')
                        name = element.get_attribute('name')
                        placeholder = element.get_attribute('placeholder')
                        text = element.text_content(timeout=1000) # Timeout for text content
                        key = el_id or name or f"{tag_name}{i}_{field_type.replace(' ','')}"
                        details = f"Tag={tag_name}, ID={el_id}, Name={name}, Placeholder={placeholder}, Text={text[:50].strip()}"
                        logger.info(f"  - {key}: {details}")
                        if key not in found_fields:
                             found_fields[key] = {
                                'tag': tag_name,
                                'id': el_id,
                                'name': name,
                                'placeholder': placeholder,
                                'text': text[:100].strip() if text else None,
                                'type': field_type
                            }

            browser.close()

        except Exception as e:
            logger.error(f"Error during Playwright form inspection: {e}", exc_info=True)
            # Ensure browser is closed on error
            if 'browser' in locals() and browser.is_connected():
                browser.close()

    logger.info(f"Form Inspection Summary (Found {len(found_fields)} potential fields/components):")
    for field_key, details in found_fields.items():
        logger.info(f"  -> Key: '{field_key}', Details: {details}")
    if not found_fields:
        logger.warning("Inspection did not identify key search components. Manual inspection required.")


def inspect_search_results(base_url: str, paths: Dict[str, Path]):
    """Uses Playwright to submit a dummy search and inspect the results page."""
    logger.info(f"Inspecting search results page structure starting from {base_url}")

    # --- Search Parameters & Selectors (Updated from inspect_form_fields) --- 
    dummy_search_term = "Idaho Democratic Party" 
    dummy_year = 2022
    start_date = f"01/01/{dummy_year}"
    end_date = f"12/31/{dummy_year}"
    
    # --- Selectors (Refined Strategy) --- 
    # Directly target the name input within the correct accordion panel
    # Assumes the "Candidates & PACs" accordion content has id="panel-campaigns-content"
    name_input_selector = '#panel-campaigns-content input[role="combobox"][id^="react-select-"]'
    
    # Date inputs (from previous inspection or common patterns)
    # Using placeholder might be fragile, consider a more structural selector if needed
    date_input_selector = 'input[placeholder="Any Date"][type="tel"]' # More specific
    search_button_selector = 'button:has-text("Search")' 

    # Results elements (Keep as GUESSES - Verify after successful search)
    results_area_selector = 'div[class*="result"], div[id*="result"]' # Needs verification
    results_item_selector = 'div[class*="item"], div[role="row"]' # Needs verification
    export_link_selector = 'a:has-text("Export"), a:has-text("Download"), button:has-text("Export")' # Needs verification

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True) # Change to False to watch
            page = browser.new_page()

            if not safe_goto(page, base_url):
                browser.close()
                return

            logger.info("Page loaded. Waiting for potential dynamic content...")
            page.wait_for_timeout(3000) # Give JS time

            try:
                # --- Locate the name input directly ---
                logger.info(f"Looking for name search input directly using: {name_input_selector}")
                # Assuming the first match is the correct one for Candidates/PACs
                name_input = page.locator(name_input_selector).first 
                name_input.wait_for(state='attached', timeout=15000) # Wait longer for element to be in DOM
                input_id = name_input.get_attribute('id')
                logger.info(f"Found name search input directly (ID: {input_id}).")

                # --- FOCUS the input element ---
                logger.info("Attempting to focus the name search input...")
                try:
                    name_input.focus(timeout=5000)
                    logger.info("Focus command sent. Adding short delay.")
                    page.wait_for_timeout(1000) # Small delay to allow UI updates after focus
                except PlaywrightTimeoutError:
                    logger.warning("Timeout during focus(). Trying JS focus as fallback.")
                    # Use JS focus as a fallback, as suggested by user analysis
                    if input_id:
                        js_focus_script = f"document.querySelector('#{input_id}').focus()"
                        logger.info(f"Attempting JS focus: {js_focus_script}")
                        page.evaluate(js_focus_script)
                        page.wait_for_timeout(1000) # Delay after JS focus
                    else:
                        # This case should be less likely now we wait for 'attached'
                        logger.error("Cannot use JS focus fallback: Input has no ID or wasn't found properly.")
                        raise Exception("Could not reliably find or focus the name input.") # Fail fast

                # Check visibility *after* focus attempt
                is_visible = name_input.is_visible(timeout=1000) # Check visibility with a short timeout
                logger.info(f"Name input visible after focus attempt? {is_visible}")
                if not is_visible:
                    logger.warning("Name input still not visible after focus attempts. Proceeding anyway.")
                    # save_debug_html(page, "inspect_results_focus_fail", paths) # Optional: save state if focus didn't reveal

                # --- Interact with the name input ---
                logger.info(f"Typing search term '{dummy_search_term}' into the focused input.")
                # Use fill() which should handle obscured elements better if focused
                name_input.fill(dummy_search_term, timeout=10000)
                page.wait_for_timeout(500) # Small delay after typing

                # Click the first option (assuming it appears after typing)
                # This selector might need adjustment based on actual dropdown structure
                option_selector = 'div[id*="react-select-"][class*="-option"]'
                logger.info(f"Looking for dropdown option matching: {option_selector}")
                first_option = page.locator(option_selector).first
                try:
                    first_option.wait_for(state='visible', timeout=5000)
                    logger.info("Dropdown option found. Clicking it.")
                    first_option.click(timeout=5000)
                    page.wait_for_timeout(500) # Delay after click
                except PlaywrightTimeoutError:
                     logger.warning("Could not find or click dropdown option after typing. Proceeding without selection.")
                     # Maybe press Enter instead? Requires the input element.
                     # name_input.press('Enter')
                     # logger.info("Pressed Enter on the input field.")
                     # page.wait_for_timeout(500)


            except PlaywrightTimeoutError as e_timeout:
                logger.error(f"Timeout during form interaction: {e_timeout}")
                save_debug_html(page, "inspect_results_interact_timeout", paths)
                browser.close()
                return
            except Exception as e_interact:
                logger.error(f"Error during form interaction: {e_interact}", exc_info=True)
                save_debug_html(page, "inspect_results_interact_error", paths)
                browser.close()
                return

            # --- Fill date fields ---
            try:
                logger.info("Locating date input fields...")
                date_inputs = page.locator(date_input_selector)
                start_date_input = date_inputs.nth(0)
                end_date_input = date_inputs.nth(1)

                logger.info(f"Filling start date: {start_date}")
                start_date_input.fill(start_date, timeout=5000)
                logger.info(f"Filling end date: {end_date}")
                end_date_input.fill(end_date, timeout=5000)
                page.wait_for_timeout(500) # Delay after filling dates

            except PlaywrightTimeoutError as e_timeout:
                logger.error(f"Timeout filling date fields: {e_timeout}")
                save_debug_html(page, "inspect_results_date_timeout", paths)
                browser.close()
                return
            except Exception as e_date:
                logger.error(f"Error filling date fields: {e_date}", exc_info=True)
                save_debug_html(page, "inspect_results_date_error", paths)
                browser.close()
                return

            # --- Click Search ---
            try:
                logger.info(f"Locating and clicking search button: {search_button_selector}")
                search_button = page.locator(search_button_selector).first
                search_button.click(timeout=10000)
                logger.info("Search button clicked. Waiting for navigation or results...")
                # Wait for potential navigation or results area update
                # Option 1: Wait for navigation (if search triggers full page load)
                # page.wait_for_load_state('domcontentloaded', timeout=30000)
                # Option 2: Wait for results area to appear/update (if dynamic)
                page.locator(results_area_selector).first.wait_for(state='visible', timeout=30000)
                logger.info("Results area detected.")

            except PlaywrightTimeoutError as e_timeout:
                logger.error(f"Timeout clicking search or waiting for results: {e_timeout}")
                save_debug_html(page, "inspect_results_search_timeout", paths)
                browser.close()
                return
            except Exception as e_search:
                logger.error(f"Error clicking search or waiting for results: {e_search}", exc_info=True)
                save_debug_html(page, "inspect_results_search_error", paths)
                browser.close()
                return

            # --- Inspect Results ---
            logger.info("Inspecting search results area...")
            save_debug_html(page, "inspect_results_final_page", paths) # Save final state

            results_summary = {}
            try:
                # Check for results area
                results_container = page.locator(results_area_selector).first
                results_container.wait_for(state='visible', timeout=5000)
                logger.info(f"Found results area matching: {results_area_selector}")
                results_summary['results_area_found'] = True

                # Check for individual items within the results area
                items = results_container.locator(results_item_selector)
                item_count = items.count()
                logger.info(f"Found {item_count} potential result items using: {results_item_selector}")
                results_summary['results_item_count'] = item_count
                # Log text of first few items for context
                for i in range(min(item_count, 3)):
                    item_text = items.nth(i).text_content(timeout=1000)
                    logger.info(f"  - Item {i+1} text: {item_text[:100].strip()}...")

                # Check for export link
                export_link = page.locator(export_link_selector).first
                export_link.wait_for(state='visible', timeout=5000)
                logger.info(f"Found export link matching: {export_link_selector}")
                results_summary['export_link_found'] = True
                results_summary['export_link_text'] = export_link.text_content(timeout=1000)
                results_summary['export_link_tag'] = export_link.evaluate('el => el.tagName.toLowerCase()')
                results_summary['export_link_href'] = export_link.get_attribute('href')

            except PlaywrightTimeoutError:
                logger.warning("Timeout finding results elements (area, items, or export link). Search might have failed or structure changed.")
                results_summary['inspection_error'] = "Timeout finding expected elements"
            except Exception as e_inspect:
                 logger.error(f"Error inspecting results area: {e_inspect}", exc_info=True)
                 results_summary['inspection_error'] = str(e_inspect)

            browser.close()

        except Exception as e:
            logger.error(f"Error during Playwright results inspection: {e}", exc_info=True)
            if 'browser' in locals() and browser.is_connected():
                browser.close()
            return # Exit if setup failed

        logger.info("Search Results Inspection Summary:")
        for key, value in results_summary.items():
            logger.info(f"  -> {key}: {value}")
        if not results_summary.get('results_area_found') or not results_summary.get('export_link_found'):
            logger.warning("Could not verify key elements (results area, export link) on the results page.")

# --- Original test_search_functionality (Marked as needing refactor) ---

def test_search_functionality(test_cases: List[Dict[str, Any]], paths: Dict[str, Path]):
    """Tests the search functionality using predefined cases."""
    logger.info("--- Starting Finance Scraper Search Functionality Test ---")
    logger.warning("This test currently uses the existing (likely outdated) scrape_finance_idaho functions.")
    logger.warning("Refactor scrape_finance_idaho.search_for_finance_data_link to use Playwright based on inspection results.")
    results = []
    # base_url = ID_FINANCE_BASE_URL # Not needed if search_for_finance_data_link handles it

    for case in tqdm(test_cases, desc="Running Test Cases"):
        name = case['name']
        year = case['year'] # Year might not be used directly if date range is needed
        data_type = case['data_type'] # data_type might be less relevant now
        logger.info(f"Running case: Name='{name}', Year={year}, Type='{data_type}'")
        
        # --- This part needs to be updated once scrape_finance_idaho is refactored ---
        # Using the potentially outdated search function for now
        # It will likely fail or return incorrect results.
        try:
            download_link = search_for_finance_data_link(name, year, data_type, paths)
            error_msg = None
        except Exception as e:
            logger.error(f"Error running old search_for_finance_data_link for case {name}, {year}: {e}")
            download_link = None
            error_msg = str(e)
        # ----------------------------------------------------------------------------

        case_result = {
            'name': name,
            'year': year,
            'data_type': data_type,
            'download_link_found': bool(download_link),
            'download_link': download_link,
            'data_extracted': False, # Placeholder
            'error': error_msg 
        }
        
        if download_link:
            logger.info(f"  (Old Method) Download link found: {download_link}")
            # Placeholder: Add attempt to download/extract if needed for test
        else:
            logger.warning(f"  (Old Method) No download link found for case.")
            
        results.append(case_result)
        time.sleep(ID_FINANCE_DOWNLOAD_WAIT_SECONDS + random.uniform(0.1, 0.5)) # Use configured wait

    # Log summary
    successful_links = sum(1 for r in results if r['download_link_found'])
    logger.info("--- Test Summary (Using Old Scraper Logic) ---")
    logger.info(f"Total cases run: {len(results)}")
    logger.info(f"Download links found (old method): {successful_links}")
    for r in results:
        status = "Success (Link Found)" if r['download_link_found'] else "Failure (No Link)"
        logger.info(f"  - Case: {r['name']}, {r['year']}, {r['data_type']} -> {status}")
    logger.info("---------------------------------------------")


# --- Main Execution Logic --- #

def main() -> int:
    parser = argparse.ArgumentParser(description="Test and validate Idaho finance scraper components using Playwright.")
    parser.add_argument("--inspect-form", action="store_true", help="Inspect form fields on the search page using Playwright.")
    parser.add_argument("--inspect-results", action="store_true", help="Inspect the structure of the search results page using Playwright.")
    parser.add_argument("--test-search", action="store_true", help="Run predefined search test cases (uses OLD scraper logic - needs refactor)." )
    parser.add_argument("--test-case", nargs=3, metavar=('NAME', 'YEAR', 'TYPE'), help="Run a specific test case (uses OLD scraper logic - needs refactor).")
    parser.add_argument("--data-dir", type=str, default=None, help="Override base data directory.")

    args = parser.parse_args()

    # --- Setup --- #
    try:
        paths = setup_project_paths(args.data_dir)
    except SystemExit:
        return 1
    global logger # Use global logger setup by setup_logging
    # Setup logging using the specific log file name from config
    logger = setup_logging(FINANCE_SCRAPE_LOG_FILE, paths['log'], level=logging.INFO) 

    # --- Test Cases Definition --- # 
    # Using more generic names likely to yield results
    test_cases = [
        {'name': 'Idaho Democratic Party', 'year': 2022, 'data_type': 'contributions'},
        {'name': 'Idaho Republican Party', 'year': 2022, 'data_type': 'expenditures'},
        {'name': 'Melaleuca', 'year': 2023, 'data_type': 'contributions'}, # Example PAC/Org
        {'name': 'Micron Technology', 'year': 2023, 'data_type': 'contributions'},
        # Add more diverse cases if possible (e.g., candidate committees)
    ]
    if args.test_case:
         try:
             # Allow running a single specified case
             test_cases = [{'name': args.test_case[0], 'year': int(args.test_case[1]), 'data_type': args.test_case[2]}]
         except (IndexError, ValueError):
             logger.error("Invalid format for --test-case. Use: NAME YEAR TYPE (e.g., \"John Smith\" 2022 contributions)")
             return 1

    logger.info(f"Using Base URL: {ID_FINANCE_BASE_URL}")
    logger.info(f"Using Data directory: {paths['base']}")

    exit_code = 0
    try:
        if args.inspect_form:
            logger.info("Running form field inspection...")
            search_page_url = ID_FINANCE_BASE_URL
            inspect_form_fields(search_page_url, paths)
        elif args.inspect_results:
            logger.info("Running search results inspection...")
            search_page_url = ID_FINANCE_BASE_URL
            inspect_search_results(search_page_url, paths)
        elif args.test_search:
            test_search_functionality(test_cases, paths)
        elif args.test_case:
             # Allow running single case via --test-search logic
             test_search_functionality(test_cases, paths)
        else:
            logger.info("No action specified. Running default action: --inspect-form")
            search_page_url = ID_FINANCE_BASE_URL
            inspect_form_fields(search_page_url, paths)

    except Exception as e_main:
        logger.critical(f"Unhandled error in main execution: {e_main}", exc_info=True)
        exit_code = 1
    finally:
        # Final check or summary
        if exit_code != 0:
             logger.error("Validation script finished with errors.")
        else:
            logger.info("Validation script finished.")
        logging.shutdown() # Ensure logs are flushed

    return exit_code

if __name__ == "__main__":
    # Note: logger setup is now inside main() to use paths correctly
    sys.exit(main()) 