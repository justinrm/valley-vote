# Valley Vote Data Schema

This document outlines the structure (schema) for key data files generated and used within the Valley Vote project. It includes schemas for data collected from external sources (like the LegiScan API and web scraping) and planned schemas for processed data intended for analysis and modeling.

**Note:** Schemas marked as **Planned** or **Provisional** are subject to change based on future development phases or the specific format of acquired data (especially manually obtained datasets).

Data types are listed generically (int, str, float, date, json). For dates, ISO 8601 format (`YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`) is recommended where applicable. Notes indicate potential primary keys (PK) and foreign keys (FK) for future relational database implementation.

## Core Data Schemas (Generated Primarily by `data_collection.py`)

These represent data directly obtained or minimally processed from sources like the LegiScan API or web scraping.

### `legislators.csv`

*   **Source:** LegiScan API (`getSessionPeople` endpoint, consolidated).
*   **Description:** Contains information about individual legislators.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| legislator_id | int | Unique LegiScan identifier for the person | PK |
| person_hash | str | LegiScan's hash for tracking person across sessions | |
| name | str | Full name of legislator | |
| first_name | str | First name | |
| middle_name | str | Middle name (if available) | |
| last_name | str | Last name | |
| suffix | str | Name suffix (if any) | |
| nickname | str | Nickname (if any) | |
| party_id | int | Party identifier (e.g., 1=D, 2=R) | |
| party | str | Party abbreviation (D, R, I, etc.) | |
| role_id | int | Role identifier (e.g., 1=Rep, 2=Sen) | |
| role | str | Role description (Representative, Senator) | |
| district | str | Legislative district identifier (e.g., "HD-1", "SD-15") | |
| state_id | int | LegiScan state identifier | |
| state | str | State abbreviation (e.g., ID) | |
| active | int | Whether legislator is currently active (1) or not (0) | |
| committee_sponsor | int | Whether can sponsor as committee (1) or not (0) | |
| committee_id | int | Committee ID if committee sponsor (usually 0 for people) | FK (committees) |
| ftm_eid | str | FollowTheMoney ID | Index? |
| votesmart_id | str | VoteSmart ID | Index? |
| opensecrets_id | str | OpenSecrets ID | Index? |
| knowwho_pid | str | KnowWho ID | |
| ballotpedia | str | Ballotpedia URL slug or name | Index? |
| state_link | str | URL to legislator profile on state legislature site | |
| legiscan_url | str | URL to legislator profile on LegiScan site | |
| *year* | *int* | *(Added during consolidation)* Year this record pertains to | Part of composite key? |

### `bills.csv`

*   **Source:** LegiScan API (`getMasterListRaw`, `getBill` endpoints, consolidated).
*   **Description:** Contains information about individual bills.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| bill_id | int | Unique LegiScan identifier for the bill | PK |
| change_hash | str | Hash indicating last change (used for efficient updates) | Index |
| session_id | int | LegiScan legislative session ID | FK (sessions), Index |
| year | int | Year the bill was introduced/active (derived from session) | Index |
| state | str | State abbreviation | |
| state_id | int | LegiScan state identifier | |
| url | str | LegiScan URL for the bill | |
| state_link | str | URL to the bill on the state legislature site | |
| number | str | Bill number (e.g., "HB 101") | Index |
| type | str | Bill type abbreviation (B=Bill, R=Resolution, etc.) | |
| type_id | int | Bill type identifier | |
| body | str | Originating body abbreviation (H=House, S=Senate, J=Joint) | |
| body_id | int | Body identifier | |
| current_body | str | Current body abbreviation | |
| current_body_id | int | Current body identifier | |
| title | str | Bill title | |
| description | str | Bill description or synopsis | |
| status | int | Numeric status code (see `STATUS_CODES` in `config.py`) | Index |
| status_desc | str | Text description of the status | |
| status_date | date | Date of last status change (YYYY-MM-DD) | |
| pending_committee_id | int | Committee ID if currently pending in committee (0 otherwise) | FK (committees) |
| subjects | str | Semicolon-separated list of subjects | |
| subject_ids | str | Semicolon-separated list of subject IDs | |
| sast_relations | json | JSON string representing related bills (Same As/Similar To) | Needs parsing/normalization | |
| texts | json | JSON string listing available bill text document versions (stubs) | FK (texts) | |
| amendments | json | JSON string listing available amendments (stubs) | FK (amendments) | |
| supplements | json | JSON string listing available supplements (e.g., fiscal notes) (stubs) | FK (supplements) | |

### `votes.csv`

*   **Source:** LegiScan API (`getRollCall` endpoint, processed).
*   **Description:** Contains individual legislator votes on specific roll calls.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| vote_id | int | Unique LegiScan identifier for the roll call | Part of PK, FK (roll_calls table?) |
| bill_id | int | Bill being voted on | FK (bills), Index |
| legislator_id | int | Legislator casting the vote | Part of PK, FK (legislators), Index |
| vote_id_type | int | LegiScan's numeric vote type (1=Yea, 2=Nay, 3=NV, 4=Absent) | |
| vote_text | str | Raw vote text from API (e.g., "Yea", "Nay", "Not Voting") | |
| vote_value | int | Standardized vote value (1=Yea, 0=Nay, -1=NV/Present, -2=Absent/Excused) | Target variable (after filtering) |
| date | date | Date of the roll call vote (YYYY-MM-DD) | |
| description | str | Description of the vote/action (e.g., "Third Reading", "Motion to Table") | |
| yea | int | Summary count: Total Yea votes on this roll call | |
| nay | int | Summary count: Total Nay votes on this roll call | |
| nv | int | Summary count: Total Not Voting on this roll call | |
| absent | int | Summary count: Total Absent on this roll call | |
| total | int | Total members voting or eligible on this roll call | |
| passed | int | Whether the measure passed on this roll call (1=Yes, 0=No) | |
| chamber | str | Chamber where vote occurred (H=House, S=Senate) | |
| chamber_id | int | Chamber identifier | |
| session_id | int | Legislative session ID | FK (sessions) |
| year | int | Year of vote | Index |

### `sponsors.csv`

*   **Source:** LegiScan API (`getBill` endpoint, extracted from bill details).
*   **Description:** Links bills to their sponsors (legislators or committees).

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| bill_id | int | Bill being sponsored | Part of PK, FK (bills) |
| legislator_id | int | Sponsoring legislator ID (0 if committee sponsor) | Part of PK (if not committee), FK (legislators) |
| committee_id | int | Sponsoring committee ID (0 if legislator sponsor) | Part of PK (if committee), FK (committees) |
| sponsor_type_id | int | Type of sponsorship (1=Primary, 2=Cosponsor) | Part of PK? |
| sponsor_type | str | Text description of sponsor type | |
| sponsor_order | int | Order of sponsorship | |
| committee_sponsor | int | Flag indicating if sponsor is a committee (1) or person (0) | |
| session_id | int | Legislative session ID | FK (sessions) |
| year | int | Year of sponsorship | Index |

### `committees.csv`

*   **Source:** LegiScan API (`getDataset` with `list=committee` or `getSessionCommittees` endpoint).
*   **Description:** Defines legislative committees for a session.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| committee_id | int | Unique LegiScan identifier for the committee | PK |
| committee_name | str | Name of the committee | |
| chamber | str | Chamber abbreviation (H, S, J) | |
| chamber_id | int | Chamber identifier | |
| session_id | int | Legislative session ID | FK (sessions) |
| year | int | Year of session | Index |

### `committee_memberships.csv`

*   **Source:** Web Scraped from State Legislature Website (e.g., Idaho) + Matching logic.
*   **Description:** Links legislators to committees based on scraped data.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| committee_id_scraped | str | Generated identifier for the scraped committee concept | |
| committee_name_scraped | str | Committee name as it appeared on the website | |
| chamber | str | Chamber (House/Senate/Joint) inferred from source | |
| year | int | Year the membership data pertains to | Index |
| legislator_name_scraped | str | Legislator name as it appeared on the website | |
| role_scraped | str | Role in committee (e.g., Chair, Vice Chair, Member) | |
| legislator_id | int | Matched LegiScan legislator ID (via fuzzy matching) | FK (legislators), Index |
| matched_api_name | str | Name from LegiScan (`legislators.csv`) that produced the match | |
| match_score | float | Fuzzy match score (0-100) indicating confidence | |
| source_url | str | URL or description of the scraped source page | |

## Planned / Provisional Schemas

### `finance_contributions_processed.csv` (Provisional)

*   **Source:** Manually acquired data (e.g., CSV from records request), then parsed and matched.
*   **Description:** Processed campaign contribution data, linked to legislators. **Schema is provisional and depends heavily on the format of the acquired data.**

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| contribution_id | str | Unique identifier for the contribution (if available, or generated) | PK |
| legislator_id | int | Matched LegiScan legislator ID of the recipient/filer | FK (legislators), Index |
| filer_name | str | Name of the committee/candidate filing the report | |
| contribution_date | date | Date of contribution | Index |
| contribution_amount | float | Amount contributed | |
| contributor_name | str | Name of contributor | |
| contributor_address | str | Contributor's full address | |
| contributor_city | str | Contributor's city | |
| contributor_state | str | Contributor's state | |
| contributor_zip | str | Contributor's ZIP code | |
| contributor_employer | str | Contributor's employer (often self-reported) | |
| contributor_occupation | str | Contributor's occupation (often self-reported) | |
| contribution_type | str | Type of contribution (e.g., Monetary, In-Kind) | |
| source_file | str | Identifier for the source file/batch of manual data | |
| acquisition_date | date | Date the manual data was acquired/processed | |
| match_method | str | Method used to match contribution to legislator (e.g., fuzzy_name, committee_id) | Added during matching | |
| match_confidence | float | Score or confidence level of the match | Added during matching | |
| *...other fields...* | *...* | Additional fields present in the raw source data | |

### `texts.csv` (or similar, if full texts are stored separately)

*   **Source:** LegiScan API (`getText` endpoint, if `--fetch-texts` used).
*   **Description:** Stores the full content of bill text documents.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| doc_id | int | Unique LegiScan document identifier | PK |
| bill_id | int | Bill this document belongs to | FK (bills), Index |
| date | date | Date of the document version (YYYY-MM-DD) | |
| type | str | Document type (e.g., "Introduced", "Enrolled") | |
| mime | str | MIME type of the content (e.g., "text/html") | |
| text_size | int | Size of the text content in bytes | |
| text_content | str | Full text content (can be large) | |
| url | str | LegiScan URL for this specific document | |
| state_link | str | URL to the document on the state legislature site | |

### `amendments.csv` (or similar)

*   **Source:** LegiScan API (`getAmendment` endpoint, if `--fetch-amendments` used).
*   **Description:** Stores the full content of bill amendment documents.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| amendment_id | int | Unique LegiScan amendment identifier | PK |
| bill_id | int | Bill this amendment belongs to | FK (bills), Index |
| adopted | int | Whether the amendment was adopted (1=Yes, 0=No) | |
| chamber | str | Chamber where amendment was considered (H, S) | |
| chamber_id | int | Chamber identifier | |
| date | date | Date of the amendment (YYYY-MM-DD) | |
| title | str | Amendment title/description | |
| text_content | str | Full text content (can be large) | |
| url | str | LegiScan URL for this specific amendment | |
| state_link | str | URL to the amendment on the state legislature site | |

### `supplements.csv` (or similar)

*   **Source:** LegiScan API (`getSupplement` endpoint, if `--fetch-supplements` used).
*   **Description:** Stores the full content of bill supplement documents (e.g., fiscal notes).

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| supplement_id | int | Unique LegiScan supplement identifier | PK |
| bill_id | int | Bill this supplement belongs to | FK (bills), Index |
| date | date | Date of the supplement (YYYY-MM-DD) | |
| type | str | Type of supplement (e.g., "Fiscal Note", "Analysis") | |
| type_id | int | Supplement type identifier | |
| title | str | Supplement title/description | |
| text_content | str | Full text content (can be large) | |
| url | str | LegiScan URL for this specific supplement | |
| state_link | str | URL to the supplement on the state legislature site | |

### `voting_data.csv` (Conceptual / Planned)

*   **Source:** Generated by `data_preprocessing.py` (Phase 2).
*   **Description:** Conceptual schema for the final feature matrix used for vote prediction modeling. Specific features will be determined during feature engineering.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| vote_record_id | int | Unique identifier for this specific vote instance | PK |
| vote_id | int | Identifier for the roll call vote | FK (votes/roll_calls) |
| legislator_id | int | Legislator casting the vote | FK (legislators) |
| bill_id | int | Bill being voted on | FK (bills) |
| vote_value | int | Target variable: Standardized vote (e.g., 1=Yea, 0=Nay) | Target |
| legislator_feature_1 | float | Example: Legislator seniority | Feature |
| legislator_feature_2 | str | Example: Legislator party | Feature |
| bill_feature_1 | float | Example: Bill complexity score | Feature |
| bill_feature_2 | str | Example: Bill primary subject category | Feature |
| context_feature_1 | float | Example: District median income | Feature |
| context_feature_2 | float | Example: Recent campaign donations from industry X | Feature |
| *...* | *...* | *Additional engineered features* | Features | 