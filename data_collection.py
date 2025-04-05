import os
import json
import time
import random
import logging
import requests
import pandas as pd
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('data_collection')

# Configuration
API_KEY = 'f526c63ee48472002b1a2356ef05fcae'  # Replace with your LegiScan API key
BASE_URL = 'https://api.legiscan.com/'
STATE = 'ID'  # Idaho
YEARS = range(2010, 2025)
RAW_DIR = 'data/raw/'
PROCESSED_DIR = 'data/processed/'
MAX_RETRIES = 5
DEFAULT_WAIT = 1  # Default wait time between API calls in seconds

# Data directories structure
DATA_DIRS = {
    'legislators': f'{RAW_DIR}legislators/',
    'bills': f'{RAW_DIR}bills/',
    'votes': f'{RAW_DIR}votes/',
    'committees': f'{RAW_DIR}committees/',
    'committee_memberships': f'{RAW_DIR}committee_memberships/',
    'sponsors': f'{RAW_DIR}sponsors/',
    'processed': PROCESSED_DIR
}

# Create all necessary directories
for directory in DATA_DIRS.values():
    os.makedirs(directory, exist_ok=True)

class APIRateLimitError(Exception):
    """Custom exception for API rate limiting"""
    pass

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((requests.exceptions.RequestException, APIRateLimitError)),
    before_sleep=lambda retry_state: logger.warning(f"Retry attempt {retry_state.attempt_number} after error")
)
def fetch_data(operation, params, wait_time=None):
    """
    Fetch data from LegiScan API with retry logic.
    
    Args:
        operation: API operation name (for logging)
        params: API parameters
        wait_time: Optional override for wait time
    
    Returns:
        JSON response data or None on failure
    """
    params['key'] = API_KEY
    
    # Smart wait to prevent rate limiting
    time.sleep(wait_time if wait_time is not None else DEFAULT_WAIT + random.uniform(0.1, 0.5))
    
    try:
        response = requests.get(BASE_URL, params=params)
        
        # Handle rate limiting
        if response.status_code == 429:
            logger.warning("Rate limit hit. Backing off...")
            raise APIRateLimitError("Rate limit exceeded")
            
        if response.status_code != 200:
            logger.error(f"Error fetching {operation}: HTTP {response.status_code}")
            return None
            
        data = response.json()
        
        # Check API-specific errors
        if data.get('status') != 'OK':
            error_msg = data.get('alert', {}).get('message', 'Unknown API error')
            logger.error(f"API error in {operation}: {error_msg}")
            return None
            
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception in {operation}: {str(e)}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response from {operation}")
        return None

def save_json(data, path):
    """Save data as JSON file."""
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON to {path}: {str(e)}")
        return False

def convert_to_csv(data, csv_path, columns):
    """Convert JSON data to CSV with specified columns."""
    try:
        df = pd.DataFrame(data)
        if not df.empty:
            # Ensure all necessary columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            
            df[columns].to_csv(csv_path, index=False)
            return len(df)
        else:
            logger.warning(f"No data to save at {csv_path}")
            return 0
    except Exception as e:
        logger.error(f"Error creating CSV {csv_path}: {str(e)}")
        return 0

def get_session_list():
    """Get list of sessions for Idaho over the specified years."""
    logger.info("Fetching session list...")
    params = {'op': 'getSessionList', 'state': STATE}
    data = fetch_data('getSessionList', params)
    
    if data and data['status'] == 'OK':
        sessions = []
        for session in data['sessions']:
            # Check if session falls within our year range
            year_start = int(session['year_start'])
            year_end = int(session['year_end']) if session['year_end'] else year_start
            
            if any(year in YEARS for year in range(year_start, year_end + 1)):
                sessions.append(session)
                
        logger.info(f"Found {len(sessions)} relevant sessions")
        return sessions
    
    logger.error("Failed to retrieve session list")
    return []

def collect_legislators():
    """Fetch and save legislator data using getPeopleList for completeness."""
    logger.info("Collecting legislator data...")
    params = {'op': 'getPeopleList', 'state': STATE}
    data = fetch_data('getPeopleList', params)
    
    legislators = []
    if data and data['status'] == 'OK':
        for legislator_id, person in data['people'].items():
            # Get detailed information for each legislator
            person_params = {'op': 'getPerson', 'id': legislator_id}
            person_data = fetch_data('getPerson', person_params)
            
            if person_data and person_data['status'] == 'OK':
                person_detail = person_data['person']
                legislator = {
                    'legislator_id': legislator_id,
                    'name': person_detail['name'],
                    'first_name': person_detail.get('first_name', ''),
                    'last_name': person_detail.get('last_name', ''),
                    'party': person_detail.get('party', ''),
                    'role': person_detail.get('role', ''),
                    'district': person_detail.get('district', ''),
                    'state': STATE,
                    'active': person_detail.get('active', 0)
                }
                
                # Capture demographic data if available
                if 'person_demographics' in person_detail:
                    demographics = person_detail['person_demographics']
                    legislator.update({
                        'gender': demographics.get('gender', ''),
                        'ethnicity': demographics.get('ethnicity', ''),
                        'religion': demographics.get('religion', ''),
                        'age': demographics.get('date_of_birth', '')
                    })
                
                legislators.append(legislator)
                
                # Save individual legislator data
                save_json(person_detail, f"{DATA_DIRS['legislators']}{legislator_id}.json")
    
    # Save consolidated legislator data
    if legislators:
        logger.info(f"Collected {len(legislators)} legislators")
        save_json(legislators, f"{DATA_DIRS['legislators']}all_legislators.json")
        convert_to_csv(
            legislators, 
            f"{DATA_DIRS['processed']}legislators.csv", 
            ['legislator_id', 'name', 'first_name', 'last_name', 'party', 'role', 'district', 'state', 'active', 
             'gender', 'ethnicity', 'religion', 'age']
        )
    else:
        logger.warning("No legislator data collected")

def collect_committees(session):
    """Fetch and save committee data for a session."""
    year = session['year_start']
    session_id = session['session_id']
    
    # Create year-specific directory
    year_dir = f"{DATA_DIRS['committees']}{year}/"
    os.makedirs(year_dir, exist_ok=True)
    
    logger.info(f"Collecting committees for {year} (session {session_id})...")
    
    params = {'op': 'getCommitteeList', 'id': session_id}
    data = fetch_data('getCommitteeList', params)
    
    committees = []
    if data and data['status'] == 'OK':
        for committee_id, committee in data['committees'].items():
            # Get detailed committee info
            committee_params = {'op': 'getCommittee', 'id': committee_id}
            committee_data = fetch_data('getCommittee', committee_params)
            
            if committee_data and committee_data['status'] == 'OK':
                committee_detail = committee_data['committee']
                committees.append({
                    'committee_id': committee_id,
                    'name': committee_detail['name'],
                    'chamber': committee_detail.get('chamber', ''),
                    'session_id': session_id,
                    'year': year
                })
                
                # Save detailed committee data
                save_json(committee_detail, f"{year_dir}{committee_id}.json")
                
                # Process committee members
                if 'committee_members' in committee_detail:
                    member_list = []
                    for member_id, member in committee_detail['committee_members'].items():
                        member_list.append({
                            'committee_id': committee_id,
                            'legislator_id': member['people_id'],
                            'role': member.get('role', 'Member'),
                            'committee_name': committee_detail['name'],
                            'session_id': session_id,
                            'year': year
                        })
                    
                    if member_list:
                        # Save committee membership data
                        membership_dir = f"{DATA_DIRS['committee_memberships']}{year}/"
                        os.makedirs(membership_dir, exist_ok=True)
                        save_json(member_list, f"{membership_dir}committee_{committee_id}_members.json")
                        convert_to_csv(
                            member_list,
                            f"{membership_dir}committee_{committee_id}_members.csv",
                            ['committee_id', 'legislator_id', 'role', 'committee_name', 'session_id', 'year']
                        )
    
    # Save consolidated committee data
    if committees:
        logger.info(f"Collected {len(committees)} committees for {year}")
        save_json(committees, f"{year_dir}all_committees.json")
        convert_to_csv(
            committees,
            f"{DATA_DIRS['processed']}committees_{year}.csv",
            ['committee_id', 'name', 'chamber', 'session_id', 'year']
        )
    else:
        logger.warning(f"No committee data collected for {year}")

def collect_bills_and_votes(session):
    """Fetch and save bills and votes for a session with detailed data."""
    year = session['year_start']
    session_id = session['session_id']
    
    # Create year-specific directories
    bills_year_dir = f"{DATA_DIRS['bills']}{year}/"
    votes_year_dir = f"{DATA_DIRS['votes']}{year}/"
    sponsors_year_dir = f"{DATA_DIRS['sponsors']}{year}/"
    
    os.makedirs(bills_year_dir, exist_ok=True)
    os.makedirs(votes_year_dir, exist_ok=True)
    os.makedirs(sponsors_year_dir, exist_ok=True)
    
    logger.info(f"Collecting bills and votes for {year} (session {session_id})...")
    
    # Get bill list for the session
    params = {'op': 'getMasterList', 'id': session_id}
    data = fetch_data('getMasterList', params)
    
    if not (data and data['status'] == 'OK'):
        logger.error(f"Failed to retrieve master bill list for session {session_id}")
        return
    
    bills = []
    all_votes = []
    all_sponsors = []
    vote_count = 0
    
    # Process each bill
    for bill_id, bill_info in tqdm(data['masterlist'].items(), desc=f"Processing bills for {year}"):
        if not isinstance(bill_info, dict):
            continue
        
        # Fetch detailed bill data
        bill_params = {'op': 'getBill', 'id': bill_id}
        bill_data = fetch_data('getBill', bill_params)
        
        if not (bill_data and bill_data['status'] == 'OK'):
            logger.warning(f"Failed to retrieve data for bill {bill_id}")
            continue
        
        bill = bill_data['bill']
        
        # Extract bill data
        bill_record = {
            'bill_id': bill['bill_id'],
            'number': bill.get('bill_number', ''),
            'title': bill.get('title', ''),
            'description': bill.get('description', ''),
            'status': bill.get('status', ''),
            'status_date': bill.get('status_date', ''),
            'session_id': session_id,
            'year': year,
            'url': bill.get('state_link', '')
        }
        
        # Extract bill subjects/topics if available
        if 'subjects' in bill:
            subjects = ';'.join([subject.get('subject_name', '') for subject in bill['subjects']])
            bill_record['subjects'] = subjects
        
        bills.append(bill_record)
        save_json(bill, f"{bills_year_dir}{bill_id}.json")
        
        # Extract bill sponsors
        if 'sponsors' in bill:
            for sponsor in bill['sponsors']:
                sponsor_record = {
                    'bill_id': bill['bill_id'],
                    'legislator_id': sponsor.get('people_id', ''),
                    'type': sponsor.get('sponsor_type', ''),
                    'position': sponsor.get('sponsor_order', 0),
                    'session_id': session_id,
                    'year': year
                }
                all_sponsors.append(sponsor_record)
        
        # Process votes
        if 'votes' in bill:
            for vote_info in bill['votes']:
                vote_id = vote_info.get('roll_call_id')
                
                if not vote_id:
                    continue
                
                # Fetch detailed vote data
                roll_params = {'op': 'getRollCall', 'id': vote_id}
                roll_data = fetch_data('getRollCall', roll_params)
                
                if not (roll_data and roll_data['status'] == 'OK'):
                    logger.warning(f"Failed to retrieve roll call data for vote {vote_id}")
                    continue
                
                roll_call = roll_data['roll_call']
                save_json(roll_call, f"{votes_year_dir}vote_{vote_id}.json")
                
                # Record vote metadata
                vote_metadata = {
                    'vote_id': vote_id,
                    'bill_id': bill['bill_id'],
                    'date': roll_call.get('date', ''),
                    'desc': roll_call.get('desc', ''),
                    'yes_count': roll_call.get('yea', 0),
                    'no_count': roll_call.get('nay', 0),
                    'passed': 1 if roll_call.get('passed', 0) == 1 else 0,
                    'session_id': session_id,
                    'year': year
                }
                
                # Process individual legislator votes
                if 'votes' in roll_call:
                    for vote in roll_call['votes']:
                        vote_record = {
                            'vote_id': vote_id,
                            'bill_id': bill['bill_id'],
                            'legislator_id': vote.get('people_id', ''),
                            'vote_text': vote.get('vote_text', ''),
                            'vote_value': map_vote_value(vote.get('vote_text', '')),
                            'date': roll_call.get('date', ''),
                            'session_id': session_id,
                            'year': year
                        }
                        all_votes.append(vote_record)
                        vote_count += 1
    
    # Save consolidated data
    if bills:
        logger.info(f"Collected {len(bills)} bills with {vote_count} votes for {year}")
        
        # Save bills
        save_json(bills, f"{bills_year_dir}all_bills.json")
        convert_to_csv(
            bills,
            f"{DATA_DIRS['processed']}bills_{year}.csv",
            ['bill_id', 'number', 'title', 'description', 'status', 'status_date', 'session_id', 'year', 'url', 'subjects']
        )
        
        # Save votes
        if all_votes:
            save_json(all_votes, f"{votes_year_dir}all_votes.json")
            convert_to_csv(
                all_votes,
                f"{DATA_DIRS['processed']}votes_{year}.csv",
                ['vote_id', 'bill_id', 'legislator_id', 'vote_text', 'vote_value', 'date', 'session_id', 'year']
            )
        
        # Save sponsors
        if all_sponsors:
            save_json(all_sponsors, f"{sponsors_year_dir}all_sponsors.json")
            convert_to_csv(
                all_sponsors,
                f"{DATA_DIRS['processed']}sponsors_{year}.csv",
                ['bill_id', 'legislator_id', 'type', 'position', 'session_id', 'year']
            )
    else:
        logger.warning(f"No bill data collected for {year}")

def map_vote_value(vote_text):
    """Map vote text to numeric values for modeling."""
    vote_text = vote_text.lower() if vote_text else ''
    if vote_text in ['yea', 'aye', 'yes']:
        return 1
    elif vote_text in ['nay', 'no']:
        return 0
    elif vote_text in ['abstain', 'present']:
        return -1
    elif vote_text in ['absent', 'not voting']:
        return -2
    else:
        return -9  # Unknown

def consolidate_committee_memberships():
    """Consolidate committee memberships from all years."""
    logger.info("Consolidating committee memberships...")
    all_memberships = []
    
    for year in YEARS:
        year_dir = f"{DATA_DIRS['committee_memberships']}{year}/"
        if not os.path.exists(year_dir):
            continue
            
        for filename in os.listdir(year_dir):
            if filename.endswith('.json') and 'committee_' in filename:
                try:
                    with open(os.path.join(year_dir, filename), 'r') as f:
                        memberships = json.load(f)
                        all_memberships.extend(memberships)
                except Exception as e:
                    logger.error(f"Error loading {filename}: {str(e)}")
    
    if all_memberships:
        logger.info(f"Consolidated {len(all_memberships)} committee memberships")
        save_json(all_memberships, f"{DATA_DIRS['committee_memberships']}all_memberships.json")
        convert_to_csv(
            all_memberships,
            f"{DATA_DIRS['processed']}committee_memberships.csv",
            ['committee_id', 'legislator_id', 'role', 'committee_name', 'session_id', 'year']
        )
    else:
        logger.warning("No committee memberships to consolidate")

def generate_feature_matrix():
    """Generate preliminary feature matrix for model training."""
    logger.info("Generating feature matrix...")
    
    # Load legislators
    try:
        legislators_df = pd.read_csv(f"{DATA_DIRS['processed']}legislators.csv")
        logger.info(f"Loaded {len(legislators_df)} legislators")
    except Exception as e:
        logger.error(f"Error loading legislators: {str(e)}")
        return
    
    # Load votes from all years
    all_votes = []
    for year in YEARS:
        vote_file = f"{DATA_DIRS['processed']}votes_{year}.csv"
        if os.path.exists(vote_file):
            try:
                year_votes = pd.read_csv(vote_file)
                all_votes.append(year_votes)
                logger.info(f"Loaded {len(year_votes)} votes from {year}")
            except Exception as e:
                logger.error(f"Error loading votes from {year}: {str(e)}")
    
    if not all_votes:
        logger.error("No vote data available")
        return
    
    # Combine votes
    votes_df = pd.concat(all_votes, ignore_index=True)
    logger.info(f"Combined {len(votes_df)} votes from all years")
    
    # Filter to valid votes (yea/nay only for now)
    valid_votes = votes_df[votes_df['vote_value'].isin([0, 1])]
    logger.info(f"Filtered to {len(valid_votes)} valid yea/nay votes")
    
    # Basic feature engineering
    # Join with legislator data
    feature_df = valid_votes.merge(
        legislators_df[['legislator_id', 'party', 'district', 'role']], 
        on='legislator_id', 
        how='left'
    )
    
    # Calculate party voting patterns
    party_vote_patterns = feature_df.groupby(['bill_id', 'party'])['vote_value'].mean().reset_index()
    party_vote_patterns = party_vote_patterns.rename(columns={'vote_value': 'party_vote_avg'})
    
    # Join party patterns back to feature dataset
    feature_df = feature_df.merge(
        party_vote_patterns, 
        on=['bill_id', 'party'], 
        how='left'
    )
    
    # Calculate historical voting alignment for each legislator
    feature_df['alignment_with_party'] = (
        feature_df['vote_value'] == (feature_df['party_vote_avg'] > 0.5).astype(int)
    ).astype(int)
    
    # Save feature matrix
    feature_df.to_csv(f"{DATA_DIRS['processed']}vote_features.csv", index=False)
    logger.info(f"Generated feature matrix with {len(feature_df)} rows and {len(feature_df.columns)} columns")
    
    # Create data summary
    summary = {
        'total_legislators': len(legislators_df),
        'total_votes': len(votes_df),
        'valid_votes': len(valid_votes),
        'feature_rows': len(feature_df),
        'vote_distribution': {
            'yay_votes': int(feature_df['vote_value'].sum()),
            'nay_votes': int(len(feature_df) - feature_df['vote_value'].sum())
        },
        'party_distribution': feature_df['party'].value_counts().to_dict()
    }
    
    save_json(summary, f"{DATA_DIRS['processed']}data_summary.json")
    logger.info("Feature matrix and data summary generated")

def main():
    """Main execution function with timing and progress tracking."""
    start_time = datetime.now()
    logger.info(f"Starting data collection for years {min(YEARS)}-{max(YEARS)}...")
    
    # Ensure all directories exist
    for directory in DATA_DIRS.values():
        os.makedirs(directory, exist_ok=True)
    
    try:
        # Get sessions for specified years
        sessions = get_session_list()
        if not sessions:
            logger.error("No sessions found for the specified years. Exiting.")
            return
        
        # Collect legislators (single operation for all years)
        collect_legislators()
        
        # Process each session for bills, votes, and committees
        for session in sessions:
            logger.info(f"Processing session {session['session_name']} ({session['year_start']})")
            collect_committees(session)
            collect_bills_and_votes(session)
        
        # Consolidate committee memberships across years
        consolidate_committee_memberships()
        
        # Generate feature matrix for model training
        generate_feature_matrix()
        
        # Report completion
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Data collection complete. Total time: {duration}")
        
    except Exception as e:
        logger.error(f"Unhandled exception in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
