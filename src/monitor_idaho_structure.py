# monitor_idaho_structure.py
"""Monitor the structure of Idaho Legislature committee pages for changes."""

# Standard library imports
import argparse
import random
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin
import hashlib
import json

# Third-party imports
from bs4 import BeautifulSoup

# Local imports
from .config import (
    ID_HOUSE_COMMITTEES_URL,
    ID_SENATE_COMMITTEES_URL,
    MONITOR_LOG_FILE
)
from .utils import setup_logging, fetch_page, load_json, save_json, setup_project_paths

# --- Configure Logging ---
# Fix: Get paths and use log_dir
paths = setup_project_paths() # Define paths globally or pass to functions
logger = setup_logging(MONITOR_LOG_FILE, paths['log']) # Pass log_dir

# --- Configuration ---
# URLs to monitor (Matches data_collection.py's targets)
MONITOR_TARGETS: Dict[str, str] = {
    'House Committees': 'https://legislature.idaho.gov/house/committees/',
    'Senate Committees': 'https://legislature.idaho.gov/senate/committees/'
}

# Selectors based on the likely structure used by the scraper
EXPECTED_HEADING_SELECTORS: List[str] = ['h3', 'h4']  # Tags likely containing committee names
EXPECTED_CONTENT_SELECTORS: List[str] = ['ul', 'ol', 'p']  # Tags likely containing member lists

# Minimum number of potential committee headings expected per page
MIN_EXPECTED_HEADINGS: int = 5

# Network Configuration
DEFAULT_WAIT: float = 1.0  # Shorter wait is okay for monitoring

# --- Monitoring Logic ---
def check_page_structure(name: str, url: str) -> bool:
    """Performs structural checks on a single page.
    
    Args:
        name: Name of the page being checked (for logging)
        url: URL of the page to check
        
    Returns:
        bool: True if structure check passes, False otherwise
        
    Raises:
        requests.exceptions.RequestException: If page fetch fails
        ValueError: If HTML parsing fails
    """
    logger.info(f"--- Checking Structure: {name} ({url}) ---")
    html_content = fetch_page(url)

    if html_content is None:
        logger.error(f"Failed to fetch HTML content for {name}. Structure check failed.")
        return False

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        issues_found: List[str] = []

        # 1. Check for Presence of Heading Tags
        heading_tags = soup.find_all(EXPECTED_HEADING_SELECTORS)
        if not heading_tags:
            issues_found.append(f"No expected heading tags ({EXPECTED_HEADING_SELECTORS}) found.")
        else:
            logger.info(f"Found {len(heading_tags)} potential heading tags.")
            # 2. Check Minimum Heading Count
            if len(heading_tags) < MIN_EXPECTED_HEADINGS:
                issues_found.append(f"Found only {len(heading_tags)} headings, less than minimum expected ({MIN_EXPECTED_HEADINGS}).")

        # 3. Check for Presence of Content Tags
        content_tags = soup.find_all(EXPECTED_CONTENT_SELECTORS)
        if not content_tags:
            issues_found.append(f"No expected content tags ({EXPECTED_CONTENT_SELECTORS}) found.")
        else:
            logger.info(f"Found {len(content_tags)} potential content tags.")

        # 4. Check Basic Relationship (Content after Heading)
        if heading_tags and content_tags:
            first_heading_pos = html_content.find(str(heading_tags[0]))
            first_content_pos = html_content.find(str(content_tags[0]))
            found_content_after_heading = False
            if first_heading_pos != -1:
                for c_tag in content_tags:
                    c_pos = html_content.find(str(c_tag))
                    if c_pos != -1 and c_pos > first_heading_pos:
                        found_content_after_heading = True
                        break
            if not found_content_after_heading:
                pass  # Commenting out issue reporting for this simple check to reduce noise

        # --- Report Results ---
        if issues_found:
            logger.error(f"Structure Check FAILED for {name} ({url})")
            for issue in issues_found:
                logger.error(f"  - {issue}")
            return False
        else:
            logger.info(f"Structure Check PASSED for {name} ({url})")
            return True

    except Exception as e:
        logger.error(f"Error parsing or analyzing HTML for {name}: {e}", exc_info=True)
        return False

# --- Main Execution ---
def main(args: Optional[argparse.Namespace] = None) -> int:
    """Main function to monitor Idaho Legislature committee page structure.
    
    Args:
        args: Optional parsed arguments. If None, will parse command line arguments.
        
    Returns:
        int: 0 if all checks pass, 1 if any check fails
    """
    if args is None:
        parser = argparse.ArgumentParser(
            description="Monitor Idaho Legislature committee page structure for changes.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        args = parser.parse_args()

    logger.info("="*50)
    logger.info("Starting Idaho Legislature Structure Monitor")
    logger.info("="*50)

    # Check each target
    all_passed = True
    for name, url in MONITOR_TARGETS.items():
        if not check_page_structure(name, url):
            all_passed = False

    # Final status
    if all_passed:
        logger.info("All structure checks PASSED.")
        return 0
    else:
        logger.error("Some structure checks FAILED. Please review the logs.")
        return 1

if __name__ == "__main__":
    exit(main())