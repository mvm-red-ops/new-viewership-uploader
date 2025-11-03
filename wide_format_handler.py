"""
Wide Format Handler
Detects and transforms wide-format viewership data (dates as columns) to long format (dates as rows)
"""

import pandas as pd
import re
from datetime import datetime
from typing import Tuple, Optional, List, Dict
import io

# Global to track filtered rows
_last_filtered_count = 0


def is_wide_format(df: pd.DataFrame) -> Tuple[bool, Optional[Dict]]:
    """
    Detect if a dataframe is in wide format (dates as columns).

    Returns:
        Tuple of (is_wide_format: bool, metadata: dict)
        metadata contains: date_columns, metric_columns, content_columns
    """
    if df.empty or len(df.columns) < 5:
        return False, None

    # Pattern to match date columns (YYYY-MM-DD format) including pandas-renamed duplicates (.1, .2, etc.)
    date_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})(?:\.(\d+))?$')

    # Find columns that look like dates (including .1, .2 suffixes from pandas)
    date_columns = []
    base_dates = set()

    for col in df.columns:
        col_str = str(col).strip()
        match = date_pattern.match(col_str)

        if match:
            base_date = match.group(1)  # Extract base date without suffix
            # Verify it's a valid date
            try:
                datetime.strptime(base_date, '%Y-%m-%d')
                date_columns.append(col_str)
                base_dates.add(base_date)
            except ValueError:
                continue

    # Check if we have multiple date columns (minimum 2 unique base dates for wide format)
    if len(base_dates) < 2:
        return False, None

    # Look at the header row structure
    # In wide format, the second row often contains metric names that repeat
    # Example: [Date Dim, Movie/Series Title, ..., 2025-07-01, 2025-07-01, 2025-07-01, ...]
    #          [blank,    Content Title,       ..., Stream Starts, PlayThrough, All Starts, ...]

    # Check if this looks like a two-row header (first row as dates, second row as metrics)
    # by looking at the first row of data
    if len(df) > 0:
        first_row = df.iloc[0]

        # Check if first row contains metric-like names
        metric_keywords = ['stream', 'play', 'hour', 'view', 'watch', 'starts', 'sum']
        metric_count = 0

        for col in date_columns:
            val = str(first_row[col]).lower()
            if any(keyword in val for keyword in metric_keywords):
                metric_count += 1

        # If many date columns have metric names in first row, this is likely wide format
        if metric_count > len(date_columns) * 0.3:  # At least 30% match
            return True, {
                'has_header_row': True,
                'date_columns': date_columns,
                'header_row_index': 0
            }

    # Alternative check: If we have many consecutive date columns (5+), likely wide format
    if len(date_columns) >= 5:
        # Find content/metadata columns (non-date columns)
        content_columns = [col for col in df.columns if col not in date_columns]

        # Typical content columns
        content_keywords = ['title', 'series', 'season', 'episode', 'content', 'provider', 'id', 'movie']
        content_matches = sum(1 for col in content_columns
                            if any(keyword in str(col).lower() for keyword in content_keywords))

        # If we have both content columns and many date columns, it's wide format
        if content_matches >= 2:
            return True, {
                'has_header_row': False,
                'date_columns': date_columns,
                'content_columns': content_columns
            }

    return False, None


def transform_wide_to_long(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
    """
    Transform wide format dataframe to long format.

    Args:
        df: Wide format dataframe with dates as columns
        metadata: Detection metadata from is_wide_format()

    Returns:
        Long format dataframe with one row per content per date
    """

    # Handle two-row header (row 0 = dates, row 1 = metric names)
    if metadata.get('has_header_row'):
        # First row contains metric names
        metric_row = df.iloc[0]

        # Skip the metric name row for data
        df_data = df.iloc[1:].reset_index(drop=True)

        # Get ALL columns and identify date columns (including pandas-renamed ones like 2025-07-01.1, .2, etc.)
        all_columns = list(df.columns)

        # Pattern to match date columns and their pandas suffixes
        date_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})(?:\.(\d+))?$')

        # Group columns by base date
        date_groups = {}  # {base_date: [(col_name, metric_name), ...]}
        content_columns = []

        for col in all_columns:
            col_str = str(col).strip()
            match = date_pattern.match(col_str)

            if match:
                base_date = match.group(1)  # Extract base date (e.g., "2025-07-01")
                metric_name = str(metric_row[col]).strip()

                if base_date not in date_groups:
                    date_groups[base_date] = []

                date_groups[base_date].append((col, metric_name))
            else:
                content_columns.append(col)

        # Create long format rows
        long_rows = []

        for idx, row in df_data.iterrows():
            # Skip rows where content columns are all NA
            content_values = [row[col] for col in content_columns]
            if all(pd.isna(v) or str(v).strip() == '' for v in content_values):
                continue

            # For each date, create a row with all metrics for that date
            for base_date, columns_and_metrics in date_groups.items():
                long_row = {}

                # Copy content columns
                for col in content_columns:
                    long_row[col] = row[col]

                # Add date
                long_row['Date'] = base_date

                # Add metrics for this date
                has_data = False
                for col_name, metric_name in columns_and_metrics:
                    value = row[col_name]

                    # Skip if metric name is empty or NaN
                    if pd.isna(metric_name) or metric_name.strip() == '':
                        continue

                    # Clean up metric name
                    clean_metric = metric_name.replace(' ', '_').replace('-', '_').upper()
                    long_row[clean_metric] = value

                    # Check if this metric has actual data
                    if pd.notna(value) and str(value).strip() != '':
                        has_data = True

                # Only add row if it has some data
                if has_data:
                    long_rows.append(long_row)

        # Create dataframe
        df_result = pd.DataFrame(long_rows)

        # Convert metric columns to numeric, handling commas
        for col in df_result.columns:
            if col not in content_columns and col != 'Date':
                # Try to convert to numeric, removing commas
                try:
                    df_result[col] = pd.to_numeric(
                        df_result[col].astype(str).str.replace(',', ''),
                        errors='coerce'
                    )
                except:
                    pass

        # Filter out rows with no content identification
        # Look for typical content column names
        title_columns = [col for col in df_result.columns
                        if any(keyword in col.lower() for keyword in ['title', 'content', 'series', 'name', 'movie'])]

        if title_columns:
            # Keep rows that have at least one non-null content identifier
            before_count = len(df_result)
            mask = df_result[title_columns].notna().any(axis=1)
            df_result = df_result[mask].reset_index(drop=True)
            after_count = len(df_result)

            if before_count > after_count:
                # Store filtered count to return to caller
                global _last_filtered_count
                _last_filtered_count = before_count - after_count
                print(f"⚠️ Filtered out {_last_filtered_count} rows with no content identification (blank title/series)")

        return df_result

    else:
        # Standard wide format: each date column contains one metric
        # This is simpler - just unpivot
        date_columns = metadata['date_columns']
        content_columns = metadata.get('content_columns',
                                      [col for col in df.columns if col not in date_columns])

        # Use pandas melt to unpivot
        df_long = df.melt(
            id_vars=content_columns,
            value_vars=date_columns,
            var_name='Date',
            value_name='Value'
        )

        # Remove rows with no value
        df_long = df_long[df_long['Value'].notna()]

        return df_long


def read_wide_format_with_multiheader(file_path_or_buffer) -> pd.DataFrame:
    """
    Read a wide format file that has multi-row headers (dates + metrics).

    Args:
        file_path_or_buffer: File path or file-like object

    Returns:
        DataFrame with properly parsed multi-level column headers
    """
    # Try reading with multi-level header first
    try:
        df = pd.read_csv(file_path_or_buffer, header=[0, 1]) if isinstance(file_path_or_buffer, str) and file_path_or_buffer.endswith('.csv') else pd.read_excel(file_path_or_buffer, header=[0, 1])

        # Check if this created a MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            return df
    except:
        pass

    # Fallback: read normally
    if isinstance(file_path_or_buffer, str):
        if file_path_or_buffer.endswith('.csv'):
            return pd.read_csv(file_path_or_buffer)
        else:
            return pd.read_excel(file_path_or_buffer)
    else:
        # For file-like objects, try CSV first
        try:
            return pd.read_csv(file_path_or_buffer)
        except:
            file_path_or_buffer.seek(0)
            return pd.read_excel(file_path_or_buffer)


def detect_and_transform(df: pd.DataFrame, file_buffer=None) -> Tuple[pd.DataFrame, bool, int]:
    """
    Detect if dataframe is wide format and transform if needed.

    Args:
        df: Input dataframe
        file_buffer: Optional file buffer to re-read with proper headers

    Returns:
        Tuple of (transformed_df, was_transformed, filtered_count)
    """
    global _last_filtered_count
    _last_filtered_count = 0  # Reset for this transformation

    # First, check if this looks like a 2-row header that was incorrectly read
    # Signs: Many "Unnamed: X" columns, and first row contains field names
    unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed:')]

    if len(unnamed_cols) >= 3 and len(df) > 0:
        # Check if first row contains field-like names
        first_row = df.iloc[0]
        field_keywords = ['title', 'content', 'season', 'episode', 'provider', 'stream', 'starts', 'hour']
        field_matches = sum(1 for val in first_row.values
                          if isinstance(val, str) and any(keyword in val.lower() for keyword in field_keywords))

        if field_matches >= 3:
            print(f"🔍 Detected 2-row header structure - reconstructing column names...")
            # Reconstruct proper column names
            new_columns = []
            header_row = df.iloc[0]  # Field names / metric names row

            for i, col in enumerate(df.columns):
                if str(col).startswith('Unnamed:'):
                    # Use the value from first row as column name (content columns)
                    field_name = str(header_row.iloc[i]) if pd.notna(header_row.iloc[i]) else f"Column_{i}"
                    new_columns.append(field_name)
                else:
                    # This is a date column - keep the date as column name
                    # The metric names in row 0 will be used during transformation
                    new_columns.append(col)

            # Apply new column names (but DON'T drop row 0 yet - we need it for transformation)
            df.columns = new_columns

            # Drop trailing empty columns (all NaN)
            df = df.dropna(axis=1, how='all')

            # Drop first column if it's just an index (Column_0 with sequential numbers)
            if 'Column_0' in df.columns:
                # Check if it's just sequential numbers
                try:
                    col_values = pd.to_numeric(df['Column_0'], errors='coerce')
                    if col_values.notna().sum() > len(df) * 0.8:  # If 80%+ are numbers
                        # Check if sequential
                        non_na = col_values.dropna()
                        if len(non_na) > 0 and (non_na == range(1, len(non_na) + 1)).all():
                            df = df.drop('Column_0', axis=1)
                            print(f"🗑️ Dropped Column_0 (sequential index)")
                except:
                    pass

            print(f"📋 Reconstructed columns (first 15): {list(df.columns[:15])}")
            print(f"📋 Total columns after cleanup: {len(df.columns)}")
            # Row 0 still contains metric names for date columns - keep it for transformation

    is_wide, metadata = is_wide_format(df)

    if is_wide:
        print(f"📊 Wide format detected: {len(metadata.get('date_columns', []))} date columns found")
        print(f"📋 Original columns (first 10): {list(df.columns[:10])}")
        transformed_df = transform_wide_to_long(df, metadata)
        print(f"✅ Transformed to long format: {len(df)} rows → {len(transformed_df)} rows")
        print(f"📋 Transformed columns: {list(transformed_df.columns)}")
        return transformed_df, True, _last_filtered_count
    else:
        return df, False, 0
