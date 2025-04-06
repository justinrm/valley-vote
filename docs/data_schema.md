# Valley Vote Data Schema

This document describes the schema for key data files in the Valley Vote project.

## Raw Data Files

### legislators.csv
Contains information about legislators from LegiScan API.

| Column | Type | Description |
|--------|------|-------------|
| legislator_id | int | Unique identifier from LegiScan |
| person_hash | str | LegiScan's hash for tracking person across sessions |
| name | str | Full name of legislator |
| first_name | str | First name |
| middle_name | str | Middle name (if available) |
| last_name | str | Last name |
| suffix | str | Name suffix (if any) |
| nickname | str | Nickname (if any) |
| party_id | int | Party identifier |
| party | str | Party abbreviation (D, R, I, etc.) |
| role_id | int | Role identifier (1=Rep, 2=Sen, etc.) |
| role | str | Role description |
| district | str | Legislative district |
| state_id | int | State identifier |
| state | str | State abbreviation |
| active | int | Whether legislator is active (1) or not (0) |
| committee_sponsor | int | Whether can sponsor as committee (1) or not (0) |
| committee_id | int | Committee ID if committee sponsor |
| ftm_eid | str | FollowTheMoney ID |
| votesmart_id | str | VoteSmart ID |
| opensecrets_id | str | OpenSecrets ID |
| knowwho_pid | str | KnowWho ID |
| ballotpedia | str | Ballotpedia URL slug |
| state_link | str | State legislature profile URL |
| legiscan_url | str | LegiScan profile URL |

### bills.csv
Contains information about bills from LegiScan API.

| Column | Type | Description |
|--------|------|-------------|
| bill_id | int | Unique identifier from LegiScan |
| change_hash | str | Hash indicating last change |
| session_id | int | Legislative session ID |
| year | int | Year of the bill |
| state | str | State abbreviation |
| state_id | int | State identifier |
| url | str | LegiScan URL |
| state_link | str | State legislature URL |
| number | str | Bill number |
| type | str | Bill type (B=Bill, R=Resolution, etc.) |
| type_id | int | Bill type identifier |
| body | str | Originating body (H=House, S=Senate) |
| body_id | int | Body identifier |
| current_body | str | Current body |
| current_body_id | int | Current body identifier |
| title | str | Bill title |
| description | str | Bill description |
| status | int | Status code |
| status_desc | str | Status description |
| status_date | str | Date of last status change |
| pending_committee_id | int | Committee ID if in committee |
| subjects | str | Semicolon-separated list of subjects |
| subject_ids | str | Semicolon-separated list of subject IDs |
| sast_relations | json | Related bills (Same As/Similar To) |
| text_stubs | json | Available bill text versions |
| amendment_stubs | json | Available amendments |
| supplement_stubs | json | Available supplements |

### votes.csv
Contains individual votes on bills.

| Column | Type | Description |
|--------|------|-------------|
| vote_id | int | Unique identifier for the roll call |
| bill_id | int | Bill being voted on |
| legislator_id | int | Legislator casting vote |
| vote_id_type | int | Vote type identifier |
| vote_text | str | Raw vote text (Yea, Nay, etc.) |
| vote_value | int | Standardized vote value (1=Yes, 0=No, -1=Present/NV, -2=Absent) |
| date | str | Date of vote |
| description | str | Vote description |
| yea | int | Total Yea votes |
| nay | int | Total Nay votes |
| nv | int | Total Not Voting |
| absent | int | Total Absent |
| total | int | Total possible votes |
| passed | int | Whether vote passed (1) or failed (0) |
| chamber | str | Chamber where vote occurred |
| chamber_id | int | Chamber identifier |
| session_id | int | Legislative session ID |
| year | int | Year of vote |

### committee_memberships.csv
Contains committee membership data scraped from state legislature website.

| Column | Type | Description |
|--------|------|-------------|
| committee_id_scraped | str | Generated identifier for committee |
| committee_name_scraped | str | Committee name from website |
| chamber | str | Chamber (House/Senate) |
| year | int | Year of membership |
| legislator_name_scraped | str | Legislator name from website |
| role_scraped | str | Role in committee (Chair, Vice Chair, Member) |
| legislator_id | int | Matched LegiScan legislator ID |
| matched_api_name | str | Name from LegiScan that matched |
| match_score | float | Fuzzy match score (0-100) |

### finance_contributions.csv
Contains campaign contribution data from state campaign finance portal.

| Column | Type | Description |
|--------|------|-------------|
| donor_name | str | Name of contributor |
| contribution_date | date | Date of contribution |
| contribution_amount | float | Amount contributed |
| donor_address | str | Contributor's address |
| donor_city | str | Contributor's city |
| donor_state | str | Contributor's state |
| donor_zip | str | Contributor's ZIP code |
| donor_employer | str | Contributor's employer |
| donor_occupation | str | Contributor's occupation |
| contribution_type | str | Type of contribution |
| committee_name | str | Recipient committee name |
| legislator_id_source | int | Matched LegiScan legislator ID |
| data_source_url | str | URL where data was obtained |
| scrape_year | int | Year data was scraped |
| raw_file_path | str | Path to raw data file |

## Processed Data Files

### voting_data.csv
Final feature matrix for vote prediction model.

| Column | Type | Description |
|--------|------|-------------|
| vote_id | int | Unique identifier for vote |
| legislator_id | int | Legislator casting vote |
| bill_id | int | Bill being voted on |
| vote_value | int | Target variable (1=Yes, 0=No) |
| party | str | Legislator's party |
| district_number | int | Legislative district number |
| seniority | int | Years served |
| party_loyalty_score | float | % votes with party majority |
| influence_score | float | Computed influence metric |
| bill_subject_vector | json | TF-IDF vector of bill subjects |
| donor_industry_vector | json | Vector of donation amounts by industry |
| district_demographics | json | Key demographic metrics for district |
| ... | ... | Additional engineered features | 