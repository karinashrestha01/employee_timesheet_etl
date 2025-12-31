# ETL/silver_utils.py
"""
Silver Layer Utilities - Data cleaning and transformation functions.
Contains reusable utility functions for cleaning null values, dates, comments, etc.
"""

import pandas as pd

# Placeholder values that should be treated as null
NULL_PLACEHOLDERS = [
    '[NULL]', '[null]', 'NULL', 'null', 'None', 'none', 
    'N/A', 'n/a', 'NA', 'na', 'NaN', 'nan', 
    '', ' ', '  ', '-', '--', '.', 'undefined'
]

# Sentinel date for "no end date" - industry standard for SCD2
# Using 2262-01-01 (max pandas timestamp is ~2262-04-11 due to nanosecond precision)
SENTINEL_END_DATE = pd.to_datetime("2222-12-31")


def clean_string_column(series: pd.Series, default_value: str = None) -> pd.Series:
    """
    Clean a string column by handling null/empty/placeholder values.
    
    Args:
        series: Pandas Series to clean
        default_value: Value to use for nulls (None = keep as null, "" = empty string)
    
    Returns:
        Cleaned Series with null placeholders replaced
    """
    # Convert to string, strip whitespace and quotes
    result = series.astype(str).str.strip().str.strip('"').str.strip("'")
    
    # Replace null placeholders with pd.NA
    for placeholder in NULL_PLACEHOLDERS:
        result = result.replace(placeholder, pd.NA)
    
    # If default value provided, fill nulls
    if default_value is not None:
        result = result.fillna(default_value)
    
    return result


def clean_numeric_column(series: pd.Series, default_value: float = 0.0) -> pd.Series:
    """
    Clean a numeric column by handling null/empty/placeholder values.
    
    Args:
        series: Pandas Series to clean
        default_value: Value to use for nulls (default 0.0)
    
    Returns:
        Cleaned Series converted to numeric with nulls filled
    """
    # First clean as string to handle placeholders
    cleaned = clean_string_column(series, default_value=None)
    
    # Convert to numeric
    result = pd.to_numeric(cleaned, errors='coerce')
    
    # Fill remaining nulls with default
    return result.fillna(default_value)


def clean_date_column(series: pd.Series) -> pd.Series:
    """
    Clean date column by handling null/empty values properly.
    
    Handles:
    - Empty strings -> NaT
    - Whitespace-only strings -> NaT
    - "[NULL]" string -> NaT
    - None/NaN values -> NaT
    - Quoted dates (e.g., "2024-10-28") -> proper datetime
    - Valid dates -> proper datetime
    """
    # Strip quotes from string values
    series = series.astype(str).str.strip().str.strip('"').str.strip("'")
    
    # First, replace empty strings and whitespace-only values with NaN
    series = series.replace(r'^\s*$', pd.NA, regex=True)
    series = series.replace('nan', pd.NA)
    series = series.replace('None', pd.NA)
    
    # Replace [NULL] string placeholder
    series = series.replace('[NULL]', pd.NA)
    series = series.replace('[null]', pd.NA)
    
    # Convert to datetime
    return pd.to_datetime(series, errors="coerce")


def clean_date_column_with_sentinel(series: pd.Series) -> pd.Series:
    """
    Clean date column and replace null values with sentinel date (2262-01-01).
    Use this for termination_date, end_date columns.
    """
    result = clean_date_column(series)
    # Fill null dates with sentinel date
    return result.fillna(SENTINEL_END_DATE)


# STANDARD COMMENT CATEGORIES
STANDARD_COMMENT_CATEGORIES = {
    # Early Out - employee left earlier than scheduled
    "EARLY OUT": ["EARLY_OUT", "EARLY OUT", "EARLYOUT", "EARLY", "LEFT_EARLY", "LEFT EARLY"],
    
    # Late Out - employee stayed later than scheduled  
    "LATE OUT": ["LATE_OUT", "LATE OUT", "LATEOUT", "VERY_LATE_OUT", "VERY LATE OUT", "VERY_LATE"],
    
    # Late In - employee arrived later than scheduled
    "LATE IN": ["LATE_IN", "LATE IN", "LATEIN", "LATE", "VERY_LATE", "ARRIVED_LATE", "ARRIVED LATE"],
    
    # Missed Punch - employee forgot to punch in/out
    "MISSED PUNCH": ["MISSED_PUNCH", "MISSED PUNCH", "MISSEDPUNCH", "MISSING_PUNCH", 
                     "FORGOT_PUNCH", "FORGOT PUNCH", "NO_PUNCH", "NO PUNCH", "IN_CHAIN", "IN CHAIN"],
    
    # PTO - Paid Time Off
    "PTO": ["PTO", "PAID_TIME_OFF", "PAID TIME OFF", "VACATION", "PERSONAL_DAY", "PERSONAL DAY",
            "SICK", "SICK_DAY", "SICK DAY", "HOLIDAY", "LEAVE", "TIME_OFF", "TIME OFF"],
    
    # Unscheduled - work done outside normal schedule
    "UNSCHEDULED": ["UNSCHEDULED", "UN_SCHEDULED", "NOT_SCHEDULED", "NOT SCHEDULED", 
                   "EXTRA_SHIFT", "EXTRA SHIFT", "OVERTIME", "OT"],
    
    # Meal Issue - meal break related
    "MEAL ISSUE": ["MEAL_NOT_TAKEN", "MEAL NOT TAKEN", "MEAL_ISSUE", "MEAL ISSUE",
                   "NO_MEAL", "NO MEAL", "MISSED_MEAL", "MISSED MEAL"],
    
    # Short Shift - worked less than scheduled hours
    "SHORT SHIFT": ["SHORT_SHIFT", "SHORT SHIFT", "SHORTSHIFT", "PARTIAL_SHIFT", "PARTIAL SHIFT"],
    
    # Cancelled Deduction
    "CANCELLED DEDUCTION": ["CANCELLED_DEDUCTION", "CANCELLED DEDUCTION", "CANCELED_DEDUCT",
                           "CANCELED DEDUCTION", "DEDUCTION_CANCELLED", "DEDUCTION CANCELLED"],
}


def categorize_comment(text) -> str:
    """
    Categorize a punch comment into a standard category.
    
    Returns one of: Early Out, Late Out, Late In, Missed Punch, PTO, 
                    Unscheduled, Meal Issue, Short Shift, Cancelled Deduction, Other, or 'NA'
    """
    # Handle None, NaN, NA, and other null-like values
    if text is None:
        return "NA"
    
    # Handle pandas/numpy null types
    try:
        if pd.isna(text):
            return "NA"
    except (TypeError, ValueError):
        pass
    
    # Basic cleanup
    text = str(text).strip().strip('"').strip("'").upper()
    
    # Handle null placeholders - return "NA"
    if text in ["[NULL]", "NULL", "NONE", "N/A", "NA", "NAN", "", " ", "-", "--", "."]:
        return "NA"
    
    # Check each category for matches
    for category, keywords in STANDARD_COMMENT_CATEGORIES.items():
        # Check for exact match
        if text in keywords:
            return category
        
        # Check if text contains any keyword
        for keyword in keywords:
            if keyword in text:
                return category
    
    # Additional partial word matching for common patterns not in exact keywords
    # Check for "MISSED" anywhere (covers "Employee Missed In Punch", etc.)
    if "MISSED" in text and ("PUNCH" in text or "IN" in text or "OUT" in text):
        return "MISSED PUNCH"
    
    # Check for "MEAL" patterns
    if "MEAL" in text and ("NOT" in text or "TAKEN" in text or "SKIP" in text or "MISSED" in text):
        return "MEAL ISSUE"
    
    # Check for "LATE" as an isolated category indicator
    if text == "LATE":
        return "LATE IN"
    
    # If contains pipe separator, try to categorize each part
    if "|" in text:
        parts = text.split("|")
        categories_found = set()
        for part in parts:
            part = part.strip()
            if part:
                for category, keywords in STANDARD_COMMENT_CATEGORIES.items():
                    if part in keywords or any(kw in part for kw in keywords):
                        categories_found.add(category)
                        break
                # Also check partial patterns for pipe-separated values
                if not categories_found:
                    if "MISSED" in part:
                        categories_found.add("MISSED PUNCH")
                    elif "MEAL" in part:
                        categories_found.add("MEAL ISSUE")
        
        if categories_found:
            return ", ".join(sorted(categories_found))
    
    # If no category matched and text has content, return "OTHER"
    if text and len(text) > 0:
        return "OTHER"
    
    return "NA"


def clean_comment(text: str) -> str:
    """
    Standardize punch comments into predefined categories.
    
    Returns one of the standard categories:
    - EARLY OUT, LATE OUT, LATE IN, MISSED PUNCH, PTO
    - UNSCHEDULED, MEAL ISSUE, SHORT SHIFT, CANCELLED DEDUCTION
    - OTHER (for unrecognized values)
    - NA (for null/empty values)
    """
    return categorize_comment(text)

