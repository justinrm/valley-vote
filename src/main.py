#!/usr/bin/env python3
"""Main entry point for Valley Vote data collection and processing."""
import argparse
from datetime import datetime
from pathlib import Path

from src.utils import setup_logging, setup_project_paths
import src.data_collection as data_collection
import src.scrape_finance_idaho as scrape_finance_idaho
import src.match_finance_to_leg as match_finance_to_leg
import src.monitor_idaho_structure as monitor_idaho_structure

logger = setup_logging('valley_vote.log')

def main():
    parser = argparse.ArgumentParser(
        description="Valley Vote - Legislative Data Collection and Processing Platform",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Global options
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Override base data directory (default: ./data)')
    parser.add_argument('--state', type=str.upper, default='ID',
                        help='State abbreviation (e.g., ID, CA, TX)')
    parser.add_argument('--start-year', type=int, default=2023,
                        help='Start year for data collection')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                        help='End year for data collection')
    
    # Action selection
    parser.add_argument('--skip-api', action='store_true',
                        help='Skip LegiScan API data collection')
    parser.add_argument('--skip-finance', action='store_true',
                        help='Skip campaign finance data collection')
    parser.add_argument('--skip-matching', action='store_true',
                        help='Skip matching finance data to legislators')
    parser.add_argument('--monitor-only', action='store_true',
                        help='Only run website structure monitoring')
    
    args = parser.parse_args()
    
    # Setup project paths
    paths = setup_project_paths(args.data_dir)
    
    logger.info("=== Starting Valley Vote Data Collection ===")
    logger.info(f"State: {args.state}")
    logger.info(f"Years: {args.start_year}-{args.end_year}")
    logger.info(f"Data Directory: {paths['base']}")
    
    try:
        # 1. Monitor website structure (if requested or as pre-check)
        if args.monitor_only:
            return monitor_idaho_structure.main(args)
        
        # 2. Collect legislative data from LegiScan API
        if not args.skip_api:
            logger.info("=== Collecting Legislative Data ===")
            # Get session list for the specified years
            sessions = data_collection.get_session_list(args.state, range(args.start_year, args.end_year + 1))
            
            # Collect legislators
            data_collection.collect_legislators(args.state, sessions)
            
            # Collect committee definitions, bills, votes, and sponsors for each session
            for session in sessions:
                data_collection.collect_committee_definitions(session)
                data_collection.collect_bills_votes_sponsors(session)
            
            # Consolidate yearly data
            data_collection.consolidate_yearly_data('legislators', range(args.start_year, args.end_year + 1), 
                                                  ['legislator_id', 'first_name', 'last_name', 'party', 'role', 'district'], 
                                                  args.state)
            data_collection.consolidate_yearly_data('bills', range(args.start_year, args.end_year + 1), 
                                                  ['bill_id', 'session_id', 'bill_number', 'title', 'description', 'type', 'status', 'url'], 
                                                  args.state)
            data_collection.consolidate_yearly_data('votes', range(args.start_year, args.end_year + 1), 
                                                  ['vote_id', 'bill_id', 'legislator_id', 'vote_value', 'vote_text'], 
                                                  args.state)
        
        # 3. Collect campaign finance data
        if not args.skip_finance:
            logger.info("=== Collecting Campaign Finance Data ===")
            finance_file = scrape_finance_idaho.main(
                start_year=args.start_year,
                end_year=args.end_year,
                data_dir=paths['base']
            )
        
        # 4. Match finance data to legislators
        if not args.skip_matching and not args.skip_finance:
            logger.info("=== Matching Finance Data to Legislators ===")
            legislators_file = paths['processed'] / f'legislators_{args.state}.csv'
            if finance_file and legislators_file.exists():
                output_file = paths['processed'] / f'finance_{args.state}_matched.csv'
                match_finance_to_leg.match_finance_to_legislators(
                    finance_file,
                    legislators_file,
                    output_file
                )
        
        logger.info("=== Data Collection Complete ===")
        return 0
        
    except Exception as e:
        logger.error(f"Error during data collection: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main()) 