"""Match finance data to legislators using fuzzy matching."""
import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from thefuzz import process, fuzz
from tqdm import tqdm

from src.utils import setup_logging, setup_project_paths, clean_name

# --- Configure Logging ---
paths = setup_project_paths()
logger = setup_logging('match_finance_to_leg.log', paths['log'])

# --- Configuration ---
DEFAULT_MATCH_THRESHOLD = 88

# Terms indicating a committee name structure
COMMITTEE_INDICATORS = [
    'committee', 'campaign', 'friends of', 'citizens for', 'pac',
    'for senate', 'for house', 'for governor', 'for congress', 'election',
    'victory fund', 'leadership pac', 'party', 'caucus'
]

def parse_committee_name(committee_name: str) -> Optional[str]:
    """
    Extract potential candidate name from committee name.
    
    Args:
        committee_name: Raw committee name string
    
    Returns:
        Extracted candidate name or None if no valid name found
    """
    if not committee_name or pd.isna(committee_name):
        return None

    name_lower = str(committee_name).lower()
    contains_indicator = any(indicator in name_lower for indicator in COMMITTEE_INDICATORS)

    if not contains_indicator:
        return None

    # Clean up the name
    extracted_name = str(committee_name)
    
    # Remove committee prefixes first
    extracted_name = re.sub(
        r'^(committee to elect|committee for|friends of|citizens for|elect)\s+',
        '',
        extracted_name,
        flags=re.IGNORECASE
    ).strip()

    # Then remove other indicators
    for indicator in COMMITTEE_INDICATORS:
        extracted_name = re.sub(
            rf'\b{indicator}\b',
            '',
            extracted_name,
            flags=re.IGNORECASE
        ).strip()

    # Remove trailing office indicators
    extracted_name = re.sub(
        r'\s+(for\s+(senate|house|governor|mayor|congress|judge|district\s*\d*))$',
        '',
        extracted_name,
        flags=re.IGNORECASE
    ).strip()

    # Clean up punctuation
    extracted_name = extracted_name.replace(' - ', ' ').replace(':', '').strip(' ,-')
    cleaned_name_from_util = clean_name(extracted_name)

    # Validate the result
    if cleaned_name_from_util:
        name_parts = cleaned_name_from_util.split()
        if len(name_parts) >= 2 and len(cleaned_name_from_util) > 4 and not cleaned_name_from_util.isdigit():
            logger.debug(f"Parsed '{committee_name}' -> '{cleaned_name_from_util}'")
            return cleaned_name_from_util
    
    return None

def match_finance_to_legislators(
    finance_file: Path,
    legislators_file: Path,
    output_file: Path,
    threshold: int = DEFAULT_MATCH_THRESHOLD
) -> None:
    """
    Match finance records to legislators using fuzzy matching.
    
    Args:
        finance_file: Path to finance data CSV
        legislators_file: Path to legislators CSV
        output_file: Path for output matched CSV
        threshold: Matching score threshold (0-100)
    """
    logger.info(f"Starting finance to legislator matching")
    logger.info(f"Finance file: {finance_file}")
    logger.info(f"Legislators file: {legislators_file}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Match threshold: {threshold}")

    # Load data
    try:
        finance_df = pd.read_csv(finance_file)
        legislators_df = pd.read_csv(legislators_file)
    except Exception as e:
        logger.error(f"Error loading data files: {e}")
        return

    # Prepare legislator names for matching
    legislator_names = legislators_df['name'].tolist()
    name_to_id = legislators_df.set_index('name')['legislator_id'].to_dict()

    # Process each finance record
    results = []
    for _, row in tqdm(finance_df.iterrows(), total=len(finance_df), desc="Matching records"):
        result = row.to_dict()
        result['matched_legislator_id'] = None
        result['matched_name'] = None
        result['match_score'] = 0

        # Try direct name match first
        if 'name' in row and pd.notna(row['name']):
            name = str(row['name']).strip()
            match = process.extractOne(
                name,
                legislator_names,
                scorer=fuzz.WRatio,
                score_cutoff=threshold
            )
            if match:
                matched_name, score = match
                result['matched_legislator_id'] = name_to_id[matched_name]
                result['matched_name'] = matched_name
                result['match_score'] = score
                logger.debug(f"Matched '{name}' to '{matched_name}' (score: {score})")

        # Try committee name if no direct match
        elif 'committee_name' in row and pd.notna(row['committee_name']):
            committee_name = str(row['committee_name']).strip()
            extracted_name = parse_committee_name(committee_name)
            if extracted_name:
                match = process.extractOne(
                    extracted_name,
                    legislator_names,
                    scorer=fuzz.WRatio,
                    score_cutoff=threshold
                )
                if match:
                    matched_name, score = match
                    result['matched_legislator_id'] = name_to_id[matched_name]
                    result['matched_name'] = matched_name
                    result['match_score'] = score
                    logger.debug(f"Matched committee '{committee_name}' to '{matched_name}' (score: {score})")

        results.append(result)

    # Create output DataFrame with required columns
    output_df = pd.DataFrame(results)
    
    # Ensure required columns exist
    required_columns = ['matched_legislator_id', 'matched_name', 'match_score']
    for col in required_columns:
        if col not in output_df.columns:
            output_df[col] = None

    # Save results
    output_df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(output_df)} matched records to {output_file}")
    
    # Report statistics
    matched_count = output_df['matched_legislator_id'].notna().sum()
    if len(output_df) > 0:
        match_percentage = matched_count / len(output_df) * 100
        logger.info(f"Matching complete: {matched_count}/{len(output_df)} records matched ({match_percentage:.1f}%)")
    else:
        logger.info("No records to match")

def main() -> int:
    """Main function for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Match campaign finance records to legislators using fuzzy matching."
    )
    parser.add_argument('finance_file', type=Path, help='Path to finance data CSV')
    parser.add_argument('legislators_file', type=Path, help='Path to legislators CSV')
    parser.add_argument('output_file', type=Path, help='Path for output matched CSV')
    parser.add_argument('--threshold', type=int, default=DEFAULT_MATCH_THRESHOLD,
                      help='Fuzzy matching score threshold (0-100)')
    
    args = parser.parse_args()
    
    try:
        match_finance_to_legislators(
            args.finance_file,
            args.legislators_file,
            args.output_file,
            args.threshold
        )
        return 0
    except Exception as e:
        logger.error(f"Error during matching: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())