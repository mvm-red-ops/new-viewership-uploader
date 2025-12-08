"""
Logo Detection Utility

Auto-detects and removes logo/banner rows from uploaded files.
Used for files like Pluto revenue reports that have logo headers.
"""

import pandas as pd
from typing import Tuple, Optional
import re


def detect_header_row(df: pd.DataFrame, max_rows_to_check: int = 10) -> int:
    """
    Detect the first row that contains the actual header (column names).

    This function looks for rows that:
    1. Have most cells filled (>60% non-empty)
    2. Contain typical header keywords
    3. Have reasonable string lengths for headers

    Args:
        df: DataFrame to analyze (should be read with header=None initially)
        max_rows_to_check: Maximum number of rows to check from the top

    Returns:
        Row index (0-based) of the detected header row, or 0 if detection fails
    """

    # Common header keywords to look for
    header_keywords = [
        'partner', 'series', 'episode', 'season', 'channel', 'revenue',
        'content', 'name', 'title', 'date', 'views', 'hours', 'minutes',
        'impressions', 'territory', 'platform', 'clip', 'viewership'
    ]

    best_row = 0
    best_score = 0

    for row_idx in range(min(max_rows_to_check, len(df))):
        row = df.iloc[row_idx]

        # Calculate fill rate (percentage of non-empty cells)
        non_empty = row.notna().sum()
        total_cols = len(row)
        fill_rate = non_empty / total_cols if total_cols > 0 else 0

        # Skip rows that are mostly empty
        if fill_rate < 0.4:
            continue

        # Convert row values to strings and check for keywords
        row_text = ' '.join([str(val).lower() for val in row if pd.notna(val)])

        # Count keyword matches
        keyword_matches = sum(1 for keyword in header_keywords if keyword in row_text)

        # Check average string length (headers are typically short)
        non_empty_vals = [str(val) for val in row if pd.notna(val)]
        avg_length = sum(len(val) for val in non_empty_vals) / len(non_empty_vals) if non_empty_vals else 0

        # Score this row
        # Higher score = more likely to be a header
        score = 0

        # Boost score for keyword matches
        score += keyword_matches * 3

        # Boost score for good fill rate
        if fill_rate > 0.6:
            score += 2

        # Penalize if average length is too long (data rows have longer values)
        if avg_length > 50:
            score -= 2

        # Boost if row contains typical header punctuation
        if '#' in row_text or '/' in row_text:
            score += 1

        if score > best_score:
            best_score = score
            best_row = row_idx

    # Only return detected row if we have reasonable confidence
    # Require at least score of 3 (e.g., 1 keyword match)
    if best_score >= 3:
        return best_row

    # Default to first row if detection fails
    return 0


def remove_logo_rows(file_path: str, file_type: str = 'csv') -> pd.DataFrame:
    """
    Read a file and automatically remove logo/banner rows.

    Args:
        file_path: Path to the file to read
        file_type: Type of file ('csv', 'xlsx', 'xls')

    Returns:
        DataFrame with logo rows removed and proper headers set
    """

    # Read file without assuming first row is header
    if file_type == 'csv':
        df_temp = pd.read_csv(file_path, header=None, nrows=15)
    else:
        df_temp = pd.read_excel(file_path, header=None, nrows=15)

    # Detect the header row
    header_row_idx = detect_header_row(df_temp)

    # Now read the full file with correct header
    if file_type == 'csv':
        df = pd.read_csv(file_path, header=header_row_idx)
    else:
        df = pd.read_excel(file_path, header=header_row_idx)

    # Clean up column names (remove extra whitespace)
    df.columns = df.columns.str.strip()

    return df


def preview_logo_detection(file_obj, file_type: str = 'csv', preview_rows: int = 10) -> Tuple[int, pd.DataFrame]:
    """
    Preview logo detection without modifying the file object.
    Shows first N rows with detected header highlighted.

    Args:
        file_obj: File object (from Streamlit uploader)
        file_type: Type of file ('csv', 'xlsx', 'xls')
        preview_rows: Number of rows to show in preview

    Returns:
        Tuple of (detected_header_row_index, preview_dataframe)
    """

    # Save current position
    file_obj.seek(0)

    # Read preview without header
    if file_type == 'csv':
        df_preview = pd.read_csv(file_obj, header=None, nrows=preview_rows)
    else:
        df_preview = pd.read_excel(file_obj, header=None, nrows=preview_rows)

    # Reset file position
    file_obj.seek(0)

    # Detect header row
    header_row_idx = detect_header_row(df_preview, max_rows_to_check=preview_rows)

    return header_row_idx, df_preview
