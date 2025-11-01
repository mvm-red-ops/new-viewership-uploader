from typing import Dict, List
from difflib import SequenceMatcher
import re

class ColumnMapper:
    """Intelligent column mapping for viewership data"""

    def __init__(self, source_columns: List[str], required_columns: List[str]):
        """
        Initialize the column mapper

        Args:
            source_columns: List of column names from the uploaded file
            required_columns: List of required column names for the template
        """
        self.source_columns = source_columns
        self.required_columns = required_columns

        # Keyword patterns for each required column
        self.mapping_patterns = {
            "Partner": [
                r"partner",
                r"content_partner",
                r"studio",
                r"provider",
                r"distributor",
                r"supplier"
            ],
            "Channel": [
                r"^channel$",
                r"channel.?name",
                r"network",
                r"station",
                r"outlet"
            ],
            "Territory": [
                r"territory",
                r"region",
                r"country",
                r"market",
                r"geography",
                r"location"
            ],
            "Date": [
                r"date",
                r"period",
                r"week",
                r"month",
                r"timestamp",
                r"time",
                r"day"
            ],
            "Content Name": [
                r"^content\s*name$",
                r"^content.name$",
                r"asset.?name",
                r"program.?name",
                r"episode.?name",
                r"video.?name",
                r"title"
            ],
            "Content ID": [
                r"content.?id",
                r"asset.?id",
                r"program.?id",
                r"show.?id",
                r"video.?id",
                r"episode.?id",
                r"id"
            ],
            "Series": [
                r"series",
                r"show",
                r"program",
                r"program.?name",
                r"show.?name",
                r"series.?name"
            ],
            "Total Watch Time": [
                r"hours",
                r"minutes",
                r"viewership",
                r"viewing.?hours",
                r"viewing.?minutes",
                r"watch.?hours",
                r"watch.?minutes",
                r"watch.?time",
                r"consumption",
                r"duration",
                r"runtime",
                r"hours.?watched",
                r"minutes.?watched",
                r"total.?hours",
                r"total.?minutes",
                r"total.?time"
            ],
            # Optional Metrics columns
            "AVG_DURATION_PER_SESSION": [r"avg.?duration.?session", r"average.?duration.?session"],
            "AVG_DURATION_PER_VIEWER": [r"avg.?duration.?viewer", r"average.?duration.?viewer"],
            "AVG_SESSION_COUNT": [r"avg.?session", r"average.?session"],
            "CHANNEL_ADPOOL_IMPRESSIONS": [r"channel.?adpool.?impressions?", r"adpool.?impressions?"],
            "DURATION": [r"^duration", r"duration.?\(minutes\)"],
            "IMPRESSIONS": [r"^impressions?$"],
            "TOT_SESSIONS": [r"total.?sessions?", r"tot.?sessions?", r"^sessions?$", r"session.?count"],
            "UNIQUE_VIEWERS": [r"unique.?viewers?", r"uniques?"],
            "VIEWS": [r"^views?$", r"view.?count"],
            # Optional Geo columns
            "CITY": [r"city", r"cities"],
            "COUNTRY": [r"country", r"countries", r"nation"],
            # Optional Device columns
            "DEVICE_ID": [r"device.?id"],
            "DEVICE_NAME": [r"device.?name"],
            "DEVICE_TYPE": [r"device.?type", r"device"],
            # Optional Content columns
            "EPISODE_NUMBER": [r"episode.?num", r"ep.?num", r"episode"],
            "LANGUAGE": [r"language", r"lang"],
            "CONTENT_PROVIDER": [r"content.?provider", r"provider"],
            "REF_ID": [r"ref.?id", r"reference.?id"],
            "SEASON_NUMBER": [r"season.?num", r"season"],
            "SERIES_CODE": [r"series.?code"],
            "VIEWERSHIP_TYPE": [r"viewership.?type", r"view.?type"],
            # Optional Date columns
            "END_TIME": [r"end.?time", r"end.?date"],
            "MONTH": [r"^month$"],
            "QUARTER": [r"quarter", r"qtr"],
            "START_TIME": [r"start.?time", r"start.?date"],
            "YEAR_MONTH_DAY": [r"year.?month.?day", r"ymd"],
            "YEAR": [r"^year$", r"yr"],
            # Optional Monetary columns
            "CHANNEL_ADPOOL_REVENUE": [r"channel.?adpool.?revenue", r"adpool.?revenue"],
            "REVENUE": [r"revenue", r"rev"]
        }

    def suggest_mappings(self) -> Dict[str, str]:
        """
        Suggest mappings from source columns to required columns using pattern matching

        Returns:
            Dictionary mapping required column names to suggested source column names
        """
        mappings = {}

        for required_col in self.required_columns:
            best_match = self._find_best_match(required_col)
            if best_match:
                mappings[required_col] = best_match

        return mappings

    def _find_best_match(self, required_col: str) -> str:
        """
        Find the best matching source column for a required column

        Args:
            required_col: The required column name to find a match for

        Returns:
            The best matching source column name, or empty string if no good match
        """
        patterns = self.mapping_patterns.get(required_col, [])
        best_score = 0
        best_match = ""

        for source_col in self.source_columns:
            score = self._calculate_match_score(source_col, required_col, patterns)

            if score > best_score:
                best_score = score
                best_match = source_col

        # Only return match if score is above threshold
        if best_score > 0.3:
            return best_match

        return ""

    def _calculate_match_score(self, source_col: str, required_col: str, patterns: List[str]) -> float:
        """
        Calculate a match score between source and required columns

        Args:
            source_col: Source column name
            required_col: Required column name
            patterns: List of regex patterns to match against

        Returns:
            Match score between 0 and 1
        """
        source_col_lower = source_col.lower()
        required_col_lower = required_col.lower()

        # Direct string similarity
        similarity = SequenceMatcher(None, source_col_lower, required_col_lower).ratio()

        # Pattern matching bonus
        pattern_bonus = 0
        for pattern in patterns:
            if re.search(pattern, source_col_lower):
                pattern_bonus = 0.5
                break

        # Exact match bonus
        if source_col_lower == required_col_lower:
            return 1.0

        # Word-level matching
        source_words = set(re.findall(r'\w+', source_col_lower))
        required_words = set(re.findall(r'\w+', required_col_lower))

        if source_words and required_words:
            word_overlap = len(source_words.intersection(required_words)) / len(required_words)
            similarity = max(similarity, word_overlap)

        return min(similarity + pattern_bonus, 1.0)

    def validate_mappings(self, mappings: Dict[str, str]) -> Dict[str, List[str]]:
        """
        Validate the provided mappings

        Args:
            mappings: Dictionary of required column to source column mappings

        Returns:
            Dictionary containing validation errors (empty if valid)
        """
        errors = {}

        # Check for missing required columns
        missing = [col for col in self.required_columns if col not in mappings or not mappings[col]]
        if missing:
            errors['missing_columns'] = missing

        # Check for duplicate mappings
        source_cols_used = [v for v in mappings.values() if v]
        duplicates = [col for col in source_cols_used if source_cols_used.count(col) > 1]
        if duplicates:
            errors['duplicate_mappings'] = list(set(duplicates))

        # Check for invalid source columns
        invalid = [v for v in mappings.values() if v and v not in self.source_columns]
        if invalid:
            errors['invalid_source_columns'] = invalid

        return errors

    def get_mapping_confidence(self, mappings: Dict[str, str]) -> Dict[str, float]:
        """
        Calculate confidence scores for each mapping

        Args:
            mappings: Dictionary of required column to source column mappings

        Returns:
            Dictionary of required column to confidence score (0-1)
        """
        confidence = {}

        for required_col, source_col in mappings.items():
            if not source_col:
                confidence[required_col] = 0.0
            else:
                patterns = self.mapping_patterns.get(required_col, [])
                score = self._calculate_match_score(source_col, required_col, patterns)
                confidence[required_col] = score

        return confidence
