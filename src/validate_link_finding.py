#!/usr/bin/env python3
"""
Validate and refine link finding for Idaho SOS Sunshine Portal.

This script helps test different strategies for finding download links
on the search results page to ensure reliable data extraction.
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.config import (
    ID_FINANCE_BASE_URL,
    ID_FINANCE_SEARCH_PATH,
    ID_FINANCE_DOWNLOAD_WAIT_SECONDS
)
from src.utils import setup_logging, setup_project_paths
from src.scrape_finance_idaho import (
    get_hidden_form_fields,
    find_export_link,
    ScrapingStructureError
)

# --- Configure Logging ---
logger = logging.getLogger('validate_link_finding')

def inspect_page_structure(url: str, session: Optional[requests.Session] = None) -> Tuple[BeautifulSoup, Dict[str, str]]:
    """
    Inspect the structure of a page to help identify download links.
    
    Args:
        url: URL to inspect
        session: Optional session to use
        
    Returns:
        Tuple of (BeautifulSoup object, hidden form fields)
    """
    logger.info(f"Inspecting page structure at {url}")
    
    if session is None:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': ID_FINANCE_BASE_URL,
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
        })
    
    try:
        response = session.get(url, timeout=45)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        hidden_fields = get_hidden_form_fields(soup)
        
        return soup, hidden_fields
    
    except Exception as e:
        logger.error(f"Error inspecting page structure: {e}", exc_info=True)
        raise

def find_all_possible_links(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Find all possible download links on a page.
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        List of dictionaries with link information
    """
    logger.info("Finding all possible download links")
    
    possible_links = []
    
    # Check for direct links
    direct_links = soup.select('a[id*="Export"], a[id*="Download"], a[title*="Export"], a[title*="Download"]')
    if not direct_links:
        direct_links = soup.find_all('a', string=re.compile(r'\b(Export|Download|CSV|Excel)\b', re.I))
    
    for link in direct_links:
        href = link.get('href', '')
        link_text = link.get_text(strip=True)
        link_id = link.get('id', '')
        link_class = link.get('class', '')
        
        possible_links.append({
            'type': 'direct_link',
            'href': href,
            'text': link_text,
            'id': link_id,
            'class': link_class,
            'element': link
        })
    
    # Check for buttons
    buttons = soup.select('input[type="submit"][value*="Export"], input[type="submit"][value*="Download"], button[id*="Export"], button[id*="Download"]')
    if not buttons:
        buttons = soup.find_all(['input', 'button'], string=re.compile(r'\b(Export|Download|CSV|Excel)\b', re.I))
    
    for button in buttons:
        button_name = button.get('name', '')
        button_value = button.get('value', button.get_text(strip=True))
        button_id = button.get('id', '')
        button_type = button.get('type', '')
        
        possible_links.append({
            'type': 'button',
            'name': button_name,
            'value': button_value,
            'id': button_id,
            'type': button_type,
            'element': button
        })
    
    # Check for JavaScript triggers
    js_triggers = soup.find_all(True, onclick=re.compile(r'export|download|csv|excel', re.I))
    
    for trigger in js_triggers:
        onclick = trigger.get('onclick', '')
        tag_name = trigger.name
        tag_id = trigger.get('id', '')
        
        possible_links.append({
            'type': 'js_trigger',
            'tag_name': tag_name,
            'id': tag_id,
            'onclick': onclick,
            'element': trigger
        })
    
    # Log findings
    logger.info(f"Found {len(possible_links)} possible download links:")
    for i, link in enumerate(possible_links):
        logger.info(f"Link {i+1}: {link}")
    
    return possible_links

def test_link_finding(url: str, form_data: Dict[str, str], session: Optional[requests.Session] = None) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """
    Test link finding on a page.
    
    Args:
        url: URL to submit the form
        form_data: Form data to submit
        session: Optional session to use
        
    Returns:
        Tuple of (download link, all possible links)
    """
    logger.info(f"Testing link finding at {url}")
    
    if session is None:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': ID_FINANCE_BASE_URL,
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
        })
    
    try:
        # First get the page to get the form fields
        initial_response = session.get(url, timeout=45)
        initial_response.raise_for_status()
        
        initial_soup = BeautifulSoup(initial_response.text, 'html.parser')
        hidden_fields = get_hidden_form_fields(initial_soup)
        
        # Combine hidden fields with form data
        full_form_data = hidden_fields.copy()
        full_form_data.update(form_data)
        
        # Submit the form
        post_response = session.post(
            url,
            data=full_form_data,
            timeout=75,
            allow_redirects=True,
            headers={'Referer': initial_response.url}
        )
        post_response.raise_for_status()
        
        # Parse the response
        results_soup = BeautifulSoup(post_response.text, 'html.parser')
        
        # Find all possible links
        all_links = find_all_possible_links(results_soup)
        
        # Try to find the export link
        download_link = find_export_link(results_soup)
        
        return download_link, all_links
    
    except Exception as e:
        logger.error(f"Error testing link finding: {e}", exc_info=True)
        return None, []

def save_debug_info(url: str, html: str, paths: Dict[str, Path]) -> Path:
    """
    Save debug information for further inspection.
    
    Args:
        url: URL that was accessed
        html: HTML content
        paths: Project paths dictionary
        
    Returns:
        Path to the saved debug file
    """
    debug_path = paths['artifacts'] / 'debug'
    debug_path.mkdir(exist_ok=True)
    
    timestamp = time.strftime('%Y%m%d%H%M%S')
    debug_file = debug_path / f"link_finding_debug_{timestamp}.html"
    
    with open(debug_file, 'w', encoding='utf-8') as f:
        f.write(f"<!-- URL: {url} -->\n")
        f.write(html)
    
    logger.info(f"Saved debug information to {debug_file}")
    
    return debug_file

def main() -> int:
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Validate and refine link finding for Idaho SOS Sunshine Portal.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--name', type=str, required=True,
                        help='Name to search for')
    parser.add_argument('--year', type=int, required=True,
                        help='Year to search for')
    parser.add_argument('--data-type', type=str, choices=['contributions', 'expenditures'], default='contributions',
                        help='Type of data to search for')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data from config/utils)')
    
    args = parser.parse_args()
    
    # Setup paths
    try:
        paths = setup_project_paths(args.data_dir)
    except SystemExit:
        sys.exit(1)
    
    # Setup logging
    logger = setup_logging('validate_link_finding.log', paths['log'])
    
    # Construct the search URL
    search_url = urljoin(ID_FINANCE_BASE_URL, ID_FINANCE_SEARCH_PATH)
    
    # Construct the form data
    form_data = {
        'ctl00$DefaultContent$CampaignSearch$txtName': args.name,
        'ctl00$DefaultContent$CampaignSearch$txtYear': str(args.year),
        'ctl00$DefaultContent$CampaignSearch$btnSearch': 'Search'
    }
    
    # Test link finding
    download_link, all_links = test_link_finding(search_url, form_data)
    
    if download_link:
        logger.info(f"✅ Found download link: {download_link}")
    else:
        logger.warning("⚠️ No download link found")
    
    # Save debug information
    if all_links:
        debug_file = save_debug_info(search_url, all_links[0]['element'].parent.parent.prettify(), paths)
        logger.info(f"Saved debug information to {debug_file}")
    
    return 0

if __name__ == "__main__":
    import re
    
    sys.exit(main()) 