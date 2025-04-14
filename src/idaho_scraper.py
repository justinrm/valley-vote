# Standard library imports
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
import re
from datetime import datetime

# Third-party imports
import pandas as pd
from bs4 import BeautifulSoup
from thefuzz import process, fuzz
from tqdm import tqdm

# Local imports
from .config import (
    COMMITTEE_MEMBER_MATCH_THRESHOLD,
    ID_HOUSE_COMMITTEES_URL,
    ID_SENATE_COMMITTEES_URL,
    ID_COMMITTEE_HEADING_SELECTORS,
    ID_COMMITTEE_CONTENT_SELECTORS,
)
from .utils import (
    fetch_page,
    save_json,
    convert_to_csv,
    load_json,
    clean_name
)

logger = logging.getLogger(__name__)

# --- Custom Exceptions ---
class ScrapingStructureError(Exception):
    """Custom exception for unexpected website structure during scraping."""
    pass

# --- Idaho Committee Web Scraping ---

def parse_idaho_committee_page(committee_url: str, chamber: str) -> List[Dict[str, Any]]:
    """
    Parse an Idaho committee page to extract member information.
    
    Args:
        committee_url: URL of the committee page
        chamber: 'house' or 'senate'
        
    Returns:
        List of dictionaries with member information
    
    Raises:
        ScrapingStructureError: If the page structure doesn't match expectations
    """
    logger.info(f"Parsing {chamber.title()} committee page: {committee_url}")
    soup = fetch_page(committee_url)
    if not soup:
        logger.error(f"Failed to fetch/parse {committee_url}")
        return []
    
    # Get committee name from page
    heading_selectors = ID_COMMITTEE_HEADING_SELECTORS.get(chamber, [])
    committee_name = None
    
    for selector in heading_selectors:
        heading_elem = soup.select_one(selector)
        if heading_elem:
            committee_name = heading_elem.get_text(strip=True)
            break
    
    if not committee_name:
        committee_name = f"Unknown {chamber.title()} Committee"
        logger.warning(f"Could not extract committee name from {committee_url}")
    
    # Get committee members
    content_selectors = ID_COMMITTEE_CONTENT_SELECTORS.get(chamber, [])
    content_elem = None
    
    for selector in content_selectors:
        content_elem = soup.select_one(selector)
        if content_elem:
            break
    
    if not content_elem:
        error_msg = f"Could not find committee content using selectors in {committee_url}"
        logger.error(error_msg)
        raise ScrapingStructureError(error_msg)
    
    members = []
    current_year = datetime.now().year
    
    # Parse different based on chamber
    if chamber.lower() == 'house':
        # House format usually has members in paragraphs or specific format
        try:
            member_elements = content_elem.find_all(['p', 'div'], class_=lambda c: c and ('member' in c.lower() or 'legislator' in c.lower()))
            
            if not member_elements:
                # Fallback parsing based on text structure
                member_elements = content_elem.find_all(['p'])
            
            for elem in member_elements:
                text = elem.get_text(strip=True)
                if not text or len(text) < 5:
                    continue
                
                # Extract position if present (e.g., "Chair:", "Vice Chair:")
                position = ""
                if "chair" in text.lower():
                    if "vice chair" in text.lower():
                        position = "Vice Chair"
                        name_part = re.sub(r'vice\s+chair[:\s]*', '', text, flags=re.IGNORECASE)
                    else:
                        position = "Chair"
                        name_part = re.sub(r'chair[:\s]*', '', text, flags=re.IGNORECASE)
                else:
                    name_part = text
                
                name = clean_name(name_part)
                if name:
                    members.append({
                        'name': name,
                        'position': position,
                        'committee': committee_name,
                        'chamber': chamber,
                        'year': current_year,
                        'url': committee_url
                    })
        except Exception as e:
            logger.error(f"Error parsing house members: {e}", exc_info=True)
            return []
    
    elif chamber.lower() == 'senate':
        # Senate format usually has members in list items or tables
        try:
            # First try to find members in lists
            member_elements = content_elem.find_all(['li'])
            
            if not member_elements:
                # Next try table rows
                member_elements = content_elem.find_all(['tr'])
            
            if not member_elements:
                # Fallback to paragraphs
                member_elements = content_elem.find_all(['p'])
            
            for elem in member_elements:
                text = elem.get_text(strip=True)
                if not text or len(text) < 5:
                    continue
                
                # Extract position if present
                position = ""
                if "chair" in text.lower():
                    if "vice chair" in text.lower():
                        position = "Vice Chair"
                        name_part = re.sub(r'vice\s+chair[:\s]*', '', text, flags=re.IGNORECASE)
                    else:
                        position = "Chair"
                        name_part = re.sub(r'chair[:\s]*', '', text, flags=re.IGNORECASE)
                else:
                    name_part = text
                
                name = clean_name(name_part)
                if name:
                    members.append({
                        'name': name,
                        'position': position,
                        'committee': committee_name,
                        'chamber': chamber,
                        'year': current_year,
                        'url': committee_url
                    })
        except Exception as e:
            logger.error(f"Error parsing senate members: {e}", exc_info=True)
            return []
    
    logger.info(f"Extracted {len(members)} members from {committee_name}")
    return members


def scrape_committee_memberships(year: int, paths: Dict[str, Path]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Scrape Idaho committee membership for the specified year.
    
    Args:
        year: The year to scrape for
        paths: Project path dictionary
        
    Returns:
        Tuple of (members list, committee URLs that were scraped)
    """
    logger.info(f"Scraping Idaho committee memberships for {year}...")
    
    year_dir = paths['raw_scrape'] / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    
    house_committees_json = year_dir / f"house_committees_{year}_ID.json"
    senate_committees_json = year_dir / f"senate_committees_{year}_ID.json"
    
    all_members = []
    committee_urls = []
    
    # House committees
    try:
        logger.info(f"Fetching House committees from {ID_HOUSE_COMMITTEES_URL}")
        house_soup = fetch_page(ID_HOUSE_COMMITTEES_URL)
        if house_soup:
            house_committee_links = house_soup.find_all('a', href=lambda href: href and 'committees/hcom' in href.lower())
            house_committee_urls = [link['href'] for link in house_committee_links if 'href' in link.attrs]
            house_committee_urls = list(set(house_committee_urls))  # Remove duplicates
            
            logger.info(f"Found {len(house_committee_urls)} House committee URLs")
            house_members = []
            
            for url in tqdm(house_committee_urls, desc=f"Scraping House committees ({year})", unit="committee"):
                try:
                    committee_urls.append(url)
                    members = parse_idaho_committee_page(url, 'house')
                    house_members.extend(members)
                except ScrapingStructureError as e:
                    logger.warning(f"Skipping committee URL due to structure error: {url}. Error: {e}")
                except Exception as e:
                    logger.error(f"Error scraping committee {url}: {e}", exc_info=True)
            
            all_members.extend(house_members)
            save_json(house_members, house_committees_json)
            logger.info(f"Saved {len(house_members)} House committee members to {house_committees_json}")
        else:
            logger.error(f"Failed to fetch House committee index from {ID_HOUSE_COMMITTEES_URL}")
    except Exception as e:
        logger.error(f"Error processing House committees: {e}", exc_info=True)
    
    # Senate committees
    try:
        logger.info(f"Fetching Senate committees from {ID_SENATE_COMMITTEES_URL}")
        senate_soup = fetch_page(ID_SENATE_COMMITTEES_URL)
        if senate_soup:
            senate_committee_links = senate_soup.find_all('a', href=lambda href: href and 'committees/scom' in href.lower())
            senate_committee_urls = [link['href'] for link in senate_committee_links if 'href' in link.attrs]
            senate_committee_urls = list(set(senate_committee_urls))  # Remove duplicates
            
            logger.info(f"Found {len(senate_committee_urls)} Senate committee URLs")
            senate_members = []
            
            for url in tqdm(senate_committee_urls, desc=f"Scraping Senate committees ({year})", unit="committee"):
                try:
                    committee_urls.append(url)
                    members = parse_idaho_committee_page(url, 'senate')
                    senate_members.extend(members)
                except ScrapingStructureError as e:
                    logger.warning(f"Skipping committee URL due to structure error: {url}. Error: {e}")
                except Exception as e:
                    logger.error(f"Error scraping committee {url}: {e}", exc_info=True)
            
            all_members.extend(senate_members)
            save_json(senate_members, senate_committees_json)
            logger.info(f"Saved {len(senate_members)} Senate committee members to {senate_committees_json}")
        else:
            logger.error(f"Failed to fetch Senate committee index from {ID_SENATE_COMMITTEES_URL}")
    except Exception as e:
        logger.error(f"Error processing Senate committees: {e}", exc_info=True)
    
    logger.info(f"Completed scraping {len(all_members)} committee members from {len(committee_urls)} committees")
    return all_members, committee_urls


def match_scraped_legislators(
    scraped_members: List[Dict[str, Any]],
    legislators_df: pd.DataFrame,
    threshold: int = COMMITTEE_MEMBER_MATCH_THRESHOLD
) -> List[Dict[str, Any]]:
    """
    Match scraped committee members to known legislators using fuzzy matching.
    
    Args:
        scraped_members: List of scraped committee member dictionaries
        legislators_df: DataFrame of known legislators
        threshold: Fuzzy match threshold (0-100)
        
    Returns:
        List of matched member dictionaries with legislator_id added
    """
    if not scraped_members:
        logger.warning("No scraped members provided for matching")
        return []
    
    if legislators_df.empty:
        logger.warning("Empty legislators DataFrame. Cannot match members.")
        return scraped_members
    
    logger.info(f"Matching {len(scraped_members)} scraped committee members to {len(legislators_df)} legislators")
    
    # Create a list of known legislator names for matching
    legislator_names = legislators_df['name'].tolist()
    name_to_id_map = dict(zip(legislators_df['name'], legislators_df['legislator_id']))
    
    matched_members = []
    match_count = 0
    
    for member in tqdm(scraped_members, desc="Matching members", unit="member"):
        scraped_name = member.get('name', '')
        if not scraped_name:
            matched_members.append(member)
            continue
        
        best_match, score = process.extractOne(
            scraped_name, 
            legislator_names,
            scorer=fuzz.token_sort_ratio
        )
        
        if score >= threshold:
            match_count += 1
            matched_member = member.copy()
            matched_member['legislator_id'] = name_to_id_map.get(best_match)
            matched_member['match_name'] = best_match
            matched_member['match_score'] = score
            matched_members.append(matched_member)
        else:
            # Keep the member but mark as unmatched
            unmatched_member = member.copy()
            unmatched_member['legislator_id'] = None
            unmatched_member['match_name'] = best_match if best_match else None
            unmatched_member['match_score'] = score
            matched_members.append(unmatched_member)
    
    logger.info(f"Matched {match_count} of {len(scraped_members)} members ({match_count/len(scraped_members)*100:.1f}%)")
    return matched_members


def consolidate_membership_data(years: List[int], state_abbr: str, paths: Dict[str, Path]):
    """
    Consolidate committee membership data from multiple years into a single CSV and JSON.
    
    Args:
        years: List of years to consolidate
        state_abbr: State abbreviation (e.g., 'ID')
        paths: Project path dictionary
    """
    logger.info(f"Consolidating committee membership data for {state_abbr}, years {min(years)}-{max(years)}...")
    
    raw_scrape_dir = paths.get('raw_scrape')
    processed_dir = paths.get('processed')
    
    if not raw_scrape_dir or not processed_dir:
        logger.error(f"Missing required paths for consolidation: raw_scrape={raw_scrape_dir}, processed={processed_dir}")
        return
    
    all_members = []
    
    for year in tqdm(years, desc=f"Loading membership data ({state_abbr})", unit="year"):
        year_dir = raw_scrape_dir / str(year)
        if not year_dir.is_dir():
            logger.debug(f"No directory for year {year}: {year_dir}")
            continue
        
        # Load house and senate files
        house_file = year_dir / f"house_committees_{year}_{state_abbr}.json"
        senate_file = year_dir / f"senate_committees_{year}_{state_abbr}.json"
        
        if house_file.exists():
            try:
                house_data = load_json(house_file)
                if isinstance(house_data, list):
                    all_members.extend(house_data)
                    logger.debug(f"Added {len(house_data)} house members from {house_file}")
                else:
                    logger.warning(f"Expected list in {house_file}, got {type(house_data)}")
            except Exception as e:
                logger.error(f"Error reading {house_file}: {e}")
        
        if senate_file.exists():
            try:
                senate_data = load_json(senate_file)
                if isinstance(senate_data, list):
                    all_members.extend(senate_data)
                    logger.debug(f"Added {len(senate_data)} senate members from {senate_file}")
                else:
                    logger.warning(f"Expected list in {senate_file}, got {type(senate_data)}")
            except Exception as e:
                logger.error(f"Error reading {senate_file}: {e}")
    
    if not all_members:
        logger.warning(f"No committee membership data found for {state_abbr} in years {years}")
        processed_csv = processed_dir / f"committee_members_{state_abbr}.csv"
        columns = ['name', 'position', 'committee', 'chamber', 'year', 'url', 'legislator_id', 'match_name', 'match_score']
        convert_to_csv([], processed_csv, columns=columns)
        return
    
    # Deduplicate members based on name, committee, and year
    seen_keys = set()
    unique_members = []
    
    for member in all_members:
        if not isinstance(member, dict):
            continue
            
        key = (
            member.get('name', ''), 
            member.get('committee', ''),
            member.get('year', ''),
            member.get('chamber', '')
        )
        
        if key not in seen_keys:
            seen_keys.add(key)
            unique_members.append(member)
    
    logger.info(f"Consolidated {len(all_members)} members to {len(unique_members)} unique entries")
    
    # Save consolidated data
    processed_json = processed_dir / f"committee_members_{state_abbr}.json"
    processed_csv = processed_dir / f"committee_members_{state_abbr}.csv"
    
    save_json(unique_members, processed_json)
    
    columns = ['name', 'position', 'committee', 'chamber', 'year', 'url', 'legislator_id', 'match_name', 'match_score']
    convert_to_csv(unique_members, processed_csv, columns=columns)
    
    logger.info(f"Saved consolidated data to {processed_csv} and {processed_json}") 