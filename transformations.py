"""
Data transformation utilities for column mapping
Handles field-level transformations like parsing time, extracting values, etc.
"""

import pandas as pd
import re
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional


class FieldTransformation:
    """Base class for field transformations"""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    def apply(self, value: Any) -> Any:
        """Apply transformation to a single value"""
        raise NotImplementedError

    def apply_series(self, series: pd.Series) -> pd.Series:
        """Apply transformation to entire pandas Series"""
        return series.apply(self.apply)


class TimeFormatTransformation(FieldTransformation):
    """Parse time formats like 'hh:mm:ss' or 'hh:mm' to decimal hours or minutes"""

    def __init__(self, output_unit: str = 'hours'):
        """
        Args:
            output_unit: 'hours' or 'minutes'
        """
        super().__init__(
            name="parse_time_format",
            description=f"Parse time format (hh:mm:ss) to {output_unit}"
        )
        self.output_unit = output_unit

    def apply(self, value: Any) -> float:
        if pd.isna(value) or value == '':
            return 0.0

        try:
            # Convert to string and strip whitespace
            time_str = str(value).strip()

            # Handle various time formats
            parts = time_str.split(':')

            if len(parts) == 3:  # hh:mm:ss
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
            elif len(parts) == 2:  # hh:mm or mm:ss
                # Assume hh:mm if first part > 24, otherwise mm:ss
                if int(parts[0]) > 24:
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    seconds = 0
                else:
                    hours = 0
                    minutes = int(parts[0])
                    seconds = int(parts[1])
            else:
                # Just a number - assume it's already in correct format
                return float(value)

            # Calculate total time
            if self.output_unit == 'hours':
                return hours + (minutes / 60) + (seconds / 3600)
            else:  # minutes
                return (hours * 60) + minutes + (seconds / 60)

        except (ValueError, AttributeError):
            return 0.0


class RegexExtractTransformation(FieldTransformation):
    """Extract value using regex pattern"""

    def __init__(self, pattern: str, group: int = 1, default: str = ''):
        """
        Args:
            pattern: Regex pattern with capture groups
            group: Which capture group to extract (default 1)
            default: Default value if no match
        """
        super().__init__(
            name="regex_extract",
            description=f"Extract using pattern: {pattern}"
        )
        self.pattern = re.compile(pattern)
        self.group = group
        self.default = default

    def apply(self, value: Any) -> str:
        if pd.isna(value) or value == '':
            return self.default

        try:
            match = self.pattern.search(str(value))
            if match:
                return match.group(self.group).strip()
            return self.default
        except (AttributeError, IndexError):
            return self.default


class SplitExtractTransformation(FieldTransformation):
    """Extract value by splitting on delimiter"""

    def __init__(self, delimiter: str, index: int = 0, strip_prefix: str = ''):
        """
        Args:
            delimiter: Character to split on (e.g., ',', '|')
            index: Which part to extract after split (0-based)
            strip_prefix: Prefix to remove (e.g., 'Channel: ')
        """
        super().__init__(
            name="split_extract",
            description=f"Split on '{delimiter}', extract part {index}"
        )
        self.delimiter = delimiter
        self.index = index
        self.strip_prefix = strip_prefix

    def apply(self, value: Any) -> str:
        if pd.isna(value) or value == '':
            return ''

        try:
            parts = str(value).split(self.delimiter)
            if len(parts) > self.index:
                result = parts[self.index].strip()

                # Remove prefix if specified
                if self.strip_prefix and result.startswith(self.strip_prefix):
                    result = result[len(self.strip_prefix):].strip()

                return result
            return ''
        except (AttributeError, IndexError):
            return ''


class CleanNumericTransformation(FieldTransformation):
    """Clean numeric values (remove commas, currency symbols, etc.)"""

    def __init__(self, decimal_places: Optional[int] = None):
        """
        Args:
            decimal_places: Round to this many decimal places (None = no rounding)
        """
        super().__init__(
            name="clean_numeric",
            description="Remove non-numeric characters and convert to number"
        )
        self.decimal_places = decimal_places

    def apply(self, value: Any) -> float:
        if pd.isna(value) or value == '':
            return 0.0

        try:
            # Remove common non-numeric characters
            cleaned = str(value).strip()
            cleaned = cleaned.replace(',', '')
            cleaned = cleaned.replace('$', '')
            cleaned = cleaned.replace('%', '')
            cleaned = cleaned.replace(' ', '')

            # Convert to float
            result = float(cleaned)

            # Round if specified
            if self.decimal_places is not None:
                result = round(result, self.decimal_places)

            return result
        except (ValueError, AttributeError):
            return 0.0


class DateFormatTransformation(FieldTransformation):
    """Parse date in various formats"""

    def __init__(self, input_format: Optional[str] = None, output_format: str = '%Y-%m-%d'):
        """
        Args:
            input_format: strptime format string (None = auto-detect)
            output_format: strftime format string for output
        """
        super().__init__(
            name="parse_date",
            description=f"Parse date to {output_format}"
        )
        self.input_format = input_format
        self.output_format = output_format

    def apply(self, value: Any) -> str:
        if pd.isna(value) or value == '':
            return ''

        try:
            date_str = str(value).strip()

            if self.input_format:
                # Use specified format
                dt = datetime.strptime(date_str, self.input_format)
            else:
                # Try common formats
                formats = [
                    '%Y-%m-%d',
                    '%m/%d/%Y',
                    '%d/%m/%Y',
                    '%Y/%m/%d',
                    '%m-%d-%Y',
                    '%d-%m-%Y',
                ]

                dt = None
                for fmt in formats:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue

                if dt is None:
                    return date_str  # Return as-is if can't parse

            return dt.strftime(self.output_format)

        except (ValueError, AttributeError):
            return str(value)


class ChainTransformation(FieldTransformation):
    """Chain multiple transformations together"""

    def __init__(self, transformations: List[FieldTransformation]):
        super().__init__(
            name="chain",
            description="Apply multiple transformations in sequence"
        )
        self.transformations = transformations

    def apply(self, value: Any) -> Any:
        result = value
        for transform in self.transformations:
            result = transform.apply(result)
        return result


class TransformationBuilder:
    """Helper to build common transformation patterns"""

    @staticmethod
    def parse_combined_field(field_mappings: Dict[str, str]) -> Dict[str, RegexExtractTransformation]:
        """
        Parse a combined field like "Channel: ABC, Partner: XYZ, Territory: US"

        Args:
            field_mappings: Dict of {target_field: pattern}
                Example: {
                    'channel': r'Channel:\s*([^,]+)',
                    'partner': r'Partner:\s*([^,]+)',
                    'territory': r'Territory:\s*([^,]+)'
                }

        Returns:
            Dict of transformations for each target field
        """
        return {
            field: RegexExtractTransformation(pattern)
            for field, pattern in field_mappings.items()
        }

    @staticmethod
    def parse_time_to_hours(time_str: str) -> float:
        """Quick helper to parse time string to hours"""
        transform = TimeFormatTransformation(output_unit='hours')
        return transform.apply(time_str)

    @staticmethod
    def parse_time_to_minutes(time_str: str) -> float:
        """Quick helper to parse time string to minutes"""
        transform = TimeFormatTransformation(output_unit='minutes')
        return transform.apply(time_str)


def apply_conditional_transformation(value: Any, condition_config: Dict) -> Any:
    """
    Apply transformation based on conditions

    Args:
        value: Input value
        condition_config: {
            'conditions': [
                {
                    'type': 'contains' | 'starts_with' | 'ends_with' | 'equals' | 'regex_match',
                    'value': 'text to check',
                    'steps': [list of transformation steps]
                },
                ...
            ],
            'else_steps': [list of transformation steps if no condition matches]
        }
    """
    conditions = condition_config.get('conditions', [])
    else_steps = condition_config.get('else_steps', [])

    value_str = str(value) if pd.notna(value) else ''

    # Check each condition
    for condition in conditions:
        condition_type = condition.get('type')
        check_value = condition.get('value', '')
        steps = condition.get('steps', [])

        matched = False

        if condition_type == 'contains':
            matched = check_value in value_str
        elif condition_type == 'starts_with':
            matched = value_str.startswith(check_value)
        elif condition_type == 'ends_with':
            matched = value_str.endswith(check_value)
        elif condition_type == 'equals':
            matched = value_str == check_value
        elif condition_type == 'regex_match':
            import re
            matched = bool(re.search(check_value, value_str))

        if matched:
            # Apply the steps for this condition
            result = value
            for step in steps:
                result = preview_transformation_step(result, step)
            return result

    # No condition matched, apply else steps
    if else_steps:
        result = value
        for step in else_steps:
            result = preview_transformation_step(result, step)
        return result

    # No else steps, return original value
    return value


def apply_transformation(data: pd.Series, transformation_config: Dict) -> pd.Series:
    """
    Apply transformation to a pandas Series based on config

    Args:
        data: Input pandas Series
        transformation_config: Dict with transformation settings
            Single step:
            {
                'type': 'parse_time' | 'regex_extract' | 'split_extract' | 'clean_numeric' | 'parse_date',
                'params': {...}  # Type-specific parameters
            }

            Multiple steps (chain):
            {
                'type': 'chain',
                'steps': [
                    {'type': 'split_extract', 'params': {...}},
                    {'type': 'split_extract', 'params': {...}},
                    ...
                ]
            }

    Returns:
        Transformed pandas Series
    """
    trans_type = transformation_config.get('type')

    # Handle conditional transformations
    if trans_type == 'conditional':
        return data.apply(lambda x: apply_conditional_transformation(x, transformation_config))

    # Handle chained transformations
    if trans_type == 'chain':
        steps = transformation_config.get('steps', [])
        result = data
        for step in steps:
            result = apply_transformation(result, step)
        return result

    # Single transformation
    params = transformation_config.get('params', {})

    # Handle simple operations that don't need full transformation classes
    if trans_type == 'split':
        delimiter = params.get('delimiter', ',')
        return data.apply(lambda x: str(x).split(delimiter) if pd.notna(x) and x != '' else [])

    elif trans_type == 'extract_index':
        index = params.get('index', 0)
        return data.apply(lambda x: x[index] if isinstance(x, list) and len(x) > index else (x if not isinstance(x, list) else ''))

    elif trans_type == 'remove_prefix':
        prefix = params.get('prefix', '')
        def remove_prefix(x):
            if pd.isna(x) or x == '':
                return ''
            result = str(x).strip()
            if result.startswith(prefix):
                result = result[len(prefix):].strip()
            return result
        return data.apply(remove_prefix)

    elif trans_type == 'trim_all':
        return data.apply(lambda x: str(x).replace(' ', '') if pd.notna(x) and x != '' else '')

    elif trans_type == 'trim_ends':
        return data.apply(lambda x: str(x).strip() if pd.notna(x) and x != '' else '')

    # Handle transformation classes
    elif trans_type == 'parse_time':
        transform = TimeFormatTransformation(**params)
    elif trans_type == 'regex_extract':
        transform = RegexExtractTransformation(**params)
    elif trans_type == 'split_extract':
        transform = SplitExtractTransformation(**params)
    elif trans_type == 'clean_numeric':
        transform = CleanNumericTransformation(**params)
    elif trans_type == 'parse_date':
        transform = DateFormatTransformation(**params)
    else:
        # No transformation
        return data

    return transform.apply_series(data)


def preview_transformation_step(value: Any, step_config: Dict) -> Any:
    """
    Preview a single transformation step on a single value

    Args:
        value: Input value
        step_config: Transformation step config

    Returns:
        Transformed value (can be string, list, float, etc.)
    """
    trans_type = step_config.get('type')
    params = step_config.get('params', {})

    if trans_type == 'split':
        # Special step: split returns a list
        delimiter = params.get('delimiter', ',')
        # Handle list input (from previous split)
        if isinstance(value, list):
            # If value is already a list, convert to string first (join or just take first element)
            value = str(value)
        if pd.isna(value) or value == '':
            return []
        return str(value).split(delimiter)

    elif trans_type == 'extract_index':
        # Take element at index from a list or string
        index = params.get('index', 0)
        if isinstance(value, list):
            return value[index] if len(value) > index else ''
        return value

    elif trans_type == 'remove_prefix':
        # Remove a prefix from string
        prefix = params.get('prefix', '')
        if pd.isna(value) or value == '':
            return ''
        result = str(value).strip()
        if result.startswith(prefix):
            result = result[len(prefix):].strip()
        return result

    elif trans_type == 'trim_all':
        # Remove all spaces from string
        if pd.isna(value) or value == '':
            return ''
        return str(value).replace(' ', '')

    elif trans_type == 'trim_ends':
        # Remove leading and trailing spaces
        if pd.isna(value) or value == '':
            return ''
        return str(value).strip()

    elif trans_type == 'split_extract':
        transform = SplitExtractTransformation(**params)
        return transform.apply(value)

    elif trans_type == 'parse_time':
        transform = TimeFormatTransformation(**params)
        return transform.apply(value)

    elif trans_type == 'regex_extract':
        transform = RegexExtractTransformation(**params)
        return transform.apply(value)

    elif trans_type == 'clean_numeric':
        transform = CleanNumericTransformation(**params)
        return transform.apply(value)

    elif trans_type == 'parse_date':
        transform = DateFormatTransformation(**params)
        return transform.apply(value)

    elif trans_type == 'conditional':
        # Handle conditional transformations
        conditions = step_config.get('conditions', [])
        else_steps = step_config.get('else_steps', [])

        value_str = str(value) if pd.notna(value) else ''

        # Check the condition
        for condition in conditions:
            condition_type = condition.get('type')
            check_value = condition.get('value', '')
            steps = condition.get('steps', [])

            matched = False

            if condition_type == 'contains':
                matched = check_value in value_str
            elif condition_type == 'starts_with':
                matched = value_str.startswith(check_value)
            elif condition_type == 'ends_with':
                matched = value_str.endswith(check_value)
            elif condition_type == 'equals':
                matched = value_str == check_value
            elif condition_type == 'regex_match':
                import re
                matched = bool(re.search(check_value, value_str))

            if matched:
                # Apply THEN steps
                result = value
                for step in steps:
                    result = preview_transformation_step(result, step)
                return result

        # No condition matched, apply ELSE steps
        if else_steps:
            result = value
            for step in else_steps:
                result = preview_transformation_step(result, step)
            return result

        # No else steps, return original value
        return value

    return value


# Predefined transformation templates for common use cases
TRANSFORMATION_TEMPLATES = {
    "Time (hh:mm:ss) → Hours": {
        'type': 'parse_time',
        'params': {'output_unit': 'hours'}
    },
    "Time (hh:mm:ss) → Minutes": {
        'type': 'parse_time',
        'params': {'output_unit': 'minutes'}
    },
    "Extract Channel - Dual Format (Channel: / _ / space)": {
        'type': 'conditional',
        'conditions': [{
            'type': 'contains',
            'value': 'Channel:',
            'steps': [
                {'type': 'split', 'params': {'delimiter': '  '}},
                {'type': 'extract_index', 'params': {'index': 0}},
                {'type': 'remove_prefix', 'params': {'prefix': 'Channel: '}}
            ]
        }],
        'else_steps': [
            {
                'type': 'conditional',
                'conditions': [{
                    'type': 'contains',
                    'value': '_',
                    'steps': [
                        {'type': 'split', 'params': {'delimiter': '_'}},
                        {'type': 'extract_index', 'params': {'index': 0}}
                    ]
                }],
                'else_steps': [
                    {'type': 'split', 'params': {'delimiter': ' '}},
                    {'type': 'extract_index', 'params': {'index': 0}}
                ]
            }
        ]
    },
    "Extract Partner - Dual Format (Platform: / _ / space)": {
        'type': 'conditional',
        'conditions': [{
            'type': 'contains',
            'value': 'Platform:',
            'steps': [
                {'type': 'split', 'params': {'delimiter': '  '}},
                {'type': 'extract_index', 'params': {'index': 1}},
                {'type': 'remove_prefix', 'params': {'prefix': 'Platform: '}}
            ]
        }],
        'else_steps': [
            {
                'type': 'conditional',
                'conditions': [{
                    'type': 'contains',
                    'value': '_',
                    'steps': [
                        {'type': 'split', 'params': {'delimiter': '_'}},
                        {'type': 'extract_index', 'params': {'index': 1}}
                    ]
                }],
                'else_steps': [
                    {'type': 'split', 'params': {'delimiter': ' '}},
                    {'type': 'extract_index', 'params': {'index': 1}}
                ]
            }
        ]
    },
    "Extract Territory - Dual Format (Delivery Region: / _ / space)": {
        'type': 'conditional',
        'conditions': [{
            'type': 'contains',
            'value': 'Delivery Region:',
            'steps': [
                {'type': 'split', 'params': {'delimiter': '  '}},
                {'type': 'extract_index', 'params': {'index': 2}},
                {'type': 'remove_prefix', 'params': {'prefix': 'Delivery Region: '}}
            ]
        }],
        'else_steps': [
            {
                'type': 'conditional',
                'conditions': [{
                    'type': 'contains',
                    'value': '_',
                    'steps': [
                        {'type': 'split', 'params': {'delimiter': '_'}},
                        {'type': 'extract_index', 'params': {'index': 2}}
                    ]
                }],
                'else_steps': [
                    {'type': 'split', 'params': {'delimiter': ' '}},
                    {'type': 'extract_index', 'params': {'index': 2}}
                ]
            }
        ]
    },
    "Clean Number (remove commas, $, %)": {
        'type': 'clean_numeric',
        'params': {}
    },
    "Parse Date (auto-detect format)": {
        'type': 'parse_date',
        'params': {}
    },
    "Extract Partner (split on '-', take first)": {
        'type': 'split_extract',
        'params': {
            'delimiter': '-',
            'index': 0
        }
    }
}
