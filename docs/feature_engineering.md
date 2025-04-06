# Feature Engineering Documentation

This document describes the key features engineered for the vote prediction model and their calculation methods.

## Legislator Features

### Seniority Score
Measures a legislator's experience based on time served.

```python
def calculate_seniority(legislator_sessions):
    """
    Calculate seniority based on unique sessions served.
    
    Args:
        legislator_sessions: DataFrame of legislator's session history
        
    Returns:
        float: Seniority score (years served)
    """
    unique_years = legislator_sessions['year'].nunique()
    return unique_years
```

### Party Loyalty Score
Measures how often a legislator votes with their party's majority.

```python
def calculate_party_loyalty(legislator_votes, party_majority_votes):
    """
    Calculate percentage of votes aligned with party majority.
    
    Args:
        legislator_votes: DataFrame of legislator's votes
        party_majority_votes: DataFrame of party majority positions
        
    Returns:
        float: Party loyalty score (0-100)
    """
    aligned_votes = (legislator_votes['vote_value'] == party_majority_votes['majority_vote']).sum()
    total_votes = len(legislator_votes)
    return (aligned_votes / total_votes) * 100 if total_votes > 0 else 0
```

### Influence Score
Composite metric based on leadership positions, bill success, and committee roles.

```python
def calculate_influence(legislator_data, bills_sponsored, committee_roles):
    """
    Calculate legislator influence score.
    
    Components:
    - Leadership positions (weight: 0.3)
    - Bill success rate (weight: 0.4)
    - Committee leadership roles (weight: 0.3)
    
    Args:
        legislator_data: Basic legislator info
        bills_sponsored: Bills sponsored by legislator
        committee_roles: Committee memberships
        
    Returns:
        float: Influence score (0-100)
    """
    # Leadership score (0-100)
    leadership_score = 100 if legislator_data['role'] in ['Speaker', 'Majority Leader', 'Minority Leader'] else \
                      50 if legislator_data['role'] in ['Whip', 'Caucus Chair'] else 0
    
    # Bill success (0-100)
    if len(bills_sponsored) > 0:
        passed_bills = bills_sponsored['status'].isin(['Passed', 'Chaptered']).sum()
        bill_score = (passed_bills / len(bills_sponsored)) * 100
    else:
        bill_score = 0
    
    # Committee leadership (0-100)
    committee_score = 100 if 'Chair' in committee_roles['role'].values else \
                     50 if 'Vice Chair' in committee_roles['role'].values else \
                     25 if len(committee_roles) > 0 else 0
    
    # Weighted average
    influence_score = (0.3 * leadership_score) + (0.4 * bill_score) + (0.3 * committee_score)
    return influence_score
```

## Bill Features

### Subject Vector
TF-IDF representation of bill subjects for similarity comparison.

```python
from sklearn.feature_extraction.text import TfidfVectorizer

def create_subject_vectors(bills):
    """
    Create TF-IDF vectors from bill subjects.
    
    Args:
        bills: DataFrame containing bill data
        
    Returns:
        sparse matrix: TF-IDF vectors for each bill
    """
    # Combine subjects into space-separated strings
    subject_texts = bills['subjects'].str.replace(';', ' ')
    
    # Create and fit TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words='english',
        max_features=1000
    )
    return vectorizer.fit_transform(subject_texts)
```

### Bill Complexity Score
Measures bill complexity based on length, amendments, and subjects.

```python
def calculate_complexity(bill_data):
    """
    Calculate bill complexity score.
    
    Components:
    - Text length (weight: 0.4)
    - Number of amendments (weight: 0.3)
    - Number of subjects (weight: 0.3)
    
    Args:
        bill_data: Dictionary containing bill information
        
    Returns:
        float: Complexity score (0-100)
    """
    # Text length score (0-100)
    text_length = len(bill_data.get('text', ''))
    length_score = min(100, (text_length / 5000) * 100)  # Cap at 5000 chars
    
    # Amendment score (0-100)
    num_amendments = len(bill_data.get('amendments', []))
    amendment_score = min(100, num_amendments * 20)  # 5+ amendments = 100
    
    # Subject score (0-100)
    num_subjects = len(bill_data.get('subjects', '').split(';'))
    subject_score = min(100, num_subjects * 25)  # 4+ subjects = 100
    
    # Weighted average
    complexity_score = (0.4 * length_score) + (0.3 * amendment_score) + (0.3 * subject_score)
    return complexity_score
```

## Campaign Finance Features

### Industry Contribution Vector
Aggregates contributions by donor industry.

```python
def create_industry_vectors(contributions, industry_mapping):
    """
    Create vectors of contribution amounts by industry.
    
    Args:
        contributions: DataFrame of campaign contributions
        industry_mapping: Dictionary mapping occupations to industries
        
    Returns:
        DataFrame: Contribution amounts by industry for each legislator
    """
    # Map occupations to industries
    contributions['industry'] = contributions['donor_occupation'].map(industry_mapping)
    
    # Aggregate by legislator and industry
    industry_totals = contributions.pivot_table(
        index='legislator_id',
        columns='industry',
        values='contribution_amount',
        aggfunc='sum',
        fill_value=0
    )
    
    # Normalize by total contributions
    totals = industry_totals.sum(axis=1)
    return industry_totals.div(totals, axis=0) * 100
```

## District Features

### Demographics Vector
Standardized demographic metrics for each district.

```python
def create_demographics_vector(district_data):
    """
    Create standardized demographic features.
    
    Args:
        district_data: DataFrame of district Census data
        
    Returns:
        DataFrame: Standardized demographic features
    """
    # Select key metrics
    features = [
        'median_income',
        'pct_college_degree',
        'pct_urban',
        'median_age',
        'pct_minority'
    ]
    
    # Standardize features
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    return pd.DataFrame(
        scaler.fit_transform(district_data[features]),
        columns=features,
        index=district_data.index
    )
```

## Feature Selection

Features are selected based on:
1. Domain knowledge of legislative process
2. Correlation analysis with voting patterns
3. Feature importance from preliminary models
4. Availability and reliability of data

## Usage in Model

The engineered features are combined into the final feature matrix (`voting_data.csv`) with appropriate scaling and encoding for the XGBoost model. 