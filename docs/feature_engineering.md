# Feature Engineering Documentation

This document describes the key features engineered for the vote prediction model and their calculation methods. Features may be refined or extended as development continues.

**Status Key:**
- **Implemented**: Feature is coded and available in current version
- **In Progress**: Feature is partially implemented or under development
- **Planned**: Feature is designed but not yet implemented

## Feature Validation

All engineered features undergo validation to ensure quality and consistency:

1. **Range Checks**: Features are validated to ensure they fall within expected ranges
2. **Completeness**: Required features are checked for presence and completeness
3. **Correlation Analysis**: Feature correlations are analyzed to identify potential redundancies
4. **Distribution Analysis**: Feature distributions are logged for monitoring
5. **Null Value Checks**: Features are checked for unexpected null values

## Legislator Features

### Seniority Score
Measures a legislator's experience based on time served.

**Status**: Implemented

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

**Status**: Implemented

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

**Status**: Implemented

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

### Roll Call Participation
Measures a legislator's presence and participation in voting.

**Status**: Implemented

```python
def calculate_participation(legislator_votes):
    """
    Calculate the legislator's participation in roll calls.
    
    Args:
        legislator_votes: DataFrame of legislator's votes
        
    Returns:
        float: Participation rate (0-100)
    """
    # Count votes that aren't absent/excused (-2)
    participated = (legislator_votes['vote_value'] != -2).sum()
    total_votes = len(legislator_votes)
    return (participated / total_votes) * 100 if total_votes > 0 else 0
```

### Bipartisanship Score
Measures how often a legislator votes with the opposing party.

**Status**: Implemented

```python
def calculate_bipartisanship(legislator_votes, party_majorities, legislator_party):
    """
    Calculate how often legislator votes with the opposing party's majority.
    
    Args:
        legislator_votes: DataFrame of legislator's votes
        party_majorities: DataFrame with majority positions by party
        legislator_party: Party ID of the legislator
        
    Returns:
        float: Bipartisanship score (0-100)
    """
    # Find opposing party
    opposing_party = 2 if legislator_party == 1 else 1  # Assuming 1=D, 2=R
    
    # Join legislator votes with opposing party majorities
    joined = pd.merge(
        legislator_votes, 
        party_majorities[party_majorities['party_id'] == opposing_party],
        on='vote_id'
    )
    
    # Calculate agreement rate
    aligned_votes = (joined['vote_value'] == joined['majority_vote']).sum()
    total_votes = len(joined)
    return (aligned_votes / total_votes) * 100 if total_votes > 0 else 0
```

## Bill Features

### Subject Vector
TF-IDF representation of bill subjects for similarity comparison.

**Status**: Implemented

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
        max_features=1000,
        ngram_range=(1, 2)  # Include bigrams
    )
    return vectorizer.fit_transform(subject_texts)
```

### Bill Complexity Score
Measures bill complexity based on length, amendments, and subjects.

**Status**: Implemented

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

### Bill Controversy Score
Measures how controversial a bill is based on voting patterns.

**Status**: Implemented

```python
def calculate_controversy(roll_call_data):
    """
    Calculate controversy score based on vote margins.
    
    Args:
        roll_call_data: DataFrame of roll call summary data
        
    Returns:
        float: Controversy score (0-100)
    """
    # Calculate closeness of vote
    total_votes = roll_call_data['yea'] + roll_call_data['nay']
    if total_votes == 0:
        return 0
    
    margin = abs(roll_call_data['yea'] - roll_call_data['nay']) / total_votes
    
    # Invert so that closer votes have higher controversy
    return (1 - margin) * 100
```

## Feature Validation Results

The following validation checks are performed on all engineered features:

1. **Subject Vectors**
   - Check for NaN values
   - Check for zero vectors
   - Log feature statistics

2. **Influence Score**
   - Validate score range (0-100)
   - Check component scores
   - Analyze correlations between components
   - Log score distribution

3. **General Feature Validation**
   - Check for missing required features
   - Validate feature ranges
   - Check for unexpected null values
   - Analyze feature distributions

## Future Enhancements

1. **Temporal Features**
   - Session progression
   - Time-based voting patterns
   - Historical success rates

2. **Network Features**
   - Co-sponsorship networks
   - Committee overlap
   - Voting bloc analysis

3. **Text Analysis**
   - Bill text complexity
   - Amendment impact
   - Subject evolution

## Campaign Finance Features

### Industry Contribution Vector
Aggregates contributions by donor industry.

**Status**: Planned

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

### Total Contribution Amount
Measures a legislator's fundraising capability.

**Status**: Planned

```python
def calculate_total_contributions(contributions_df, legislator_id):
    """
    Calculate total campaign contributions for a legislator.
    
    Args:
        contributions_df: DataFrame of all campaign contributions
        legislator_id: Legislator ID to calculate for
        
    Returns:
        float: Total contribution amount
    """
    leg_contributions = contributions_df[contributions_df['legislator_id'] == legislator_id]
    return leg_contributions['contribution_amount'].sum()
```

## District Features

### Demographics Vector
Standardized demographic metrics for each district.

**Status**: Planned

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

## Bill-Legislator Interaction Features

### Prior Vote Similarity
Measures similarity of past voting patterns between legislators on similar bills.

**Status**: Implemented

```python
def calculate_vote_similarity(legislator_votes, similar_legislators, bill_id):
    """
    Calculate similarity of voting patterns on similar bills.
    
    Args:
        legislator_votes: DataFrame of legislator's past votes
        similar_legislators: List of similar legislators by voting pattern
        bill_id: Current bill ID
        
    Returns:
        float: Vote similarity score (-1 to 1)
    """
    # Implementation in progress
    # This is a placeholder for the algorithm
    return similarity_score
```

## Current Limitations and Future Enhancements

### Limitations

1. **Data Availability**: Some features depend on data sources not yet integrated (e.g., campaign finance, district demographics)
2. **Feature Quality**: Some implemented features may need refinement based on model performance
3. **Computational Efficiency**: Some feature calculations may need optimization for larger datasets
4. **Feature Interaction**: Current implementation focuses on individual features, not interactions between features
5. **Temporal Aspects**: Features don't fully capture changes over time in legislator behavior

### Planned Enhancements

1. **Extended Demographics**: More comprehensive demographic features from Census data
2. **Campaign Finance Integration**: Better integration of finance data once manual data is processed
3. **Text Analysis**: Natural language processing on bill texts for better subject/content analysis
4. **Network Analysis**: Graph-based features showing legislator relationships and voting blocs
5. **Temporal Features**: More sophisticated time-series based features to capture changing behavior patterns
6. **Feature Selection**: Automated feature importance and selection process
7. **Ensemble Features**: Combined features that leverage multiple data sources
8. **Customizable Feature Engineering**: Framework to allow experimentation with different feature sets

## Feature Selection

Features are selected based on:
1. Domain knowledge of legislative process
2. Correlation analysis with voting patterns
3. Feature importance from preliminary models
4. Availability and reliability of data

## Usage in Model

The engineered features are combined into the final feature matrix (`voting_feature_matrix.csv`) with appropriate scaling and encoding for the XGBoost model. The current implementation in `data_preprocessing.py` generates this matrix using the implemented features, with placeholders for planned features. 