"""
Borrowed viewership file parsers.

To add a new format:
1. Create a class with can_parse(filename, file_bytes) and parse(filename, file_bytes)
2. Add it to PARSERS before GenericCsvParser (which is the catch-all)
"""

from dataclasses import dataclass, field
from typing import Optional
import io
import re
import pandas as pd


@dataclass
class ParseResult:
    df_summary: Optional[pd.DataFrame]  # columns: Channel, Territory, Year, Month, Quarter, HOV
    suggested_filename: str
    format_name: str
    format_note: str               # shown as a caption in the UI
    warnings: list = field(default_factory=list)


_MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}


def _quarter(month: int) -> str:
    return f'q{(month - 1) // 3 + 1}'


def _parse_hhmmss(s) -> float:
    """Parse hh:mm:ss to hours. Hours and minutes may exceed their normal ranges."""
    try:
        parts = str(s).strip().split(':')
        if len(parts) != 3:
            return 0.0
        h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
        return h + m / 60.0 + sec / 3600.0
    except Exception:
        return 0.0


# ── DIRECTV Monthly xlsm ──────────────────────────────────────────────────────

class DirectTVXlsmParser:
    """
    Handles DIRECTV EVERYWHERE and OV Live Stream monthly reports.
    Both share the same structure: device-level rows with Total Time Viewed in hh:mm:ss.
    Channel and month/year are extracted from the title cell; territory is always US.
    HOV is the sum across all device rows.
    """
    FORMAT_NAME = "DIRECTV Monthly xlsm"

    @staticmethod
    def can_parse(filename: str, file_bytes: bytes) -> bool:
        if not filename.lower().endswith('.xlsm'):
            return False
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, keep_vba=True)
            ws = wb.active
            for row in ws.iter_rows(values_only=True, max_row=15):
                if any(v == 'Total Time Viewed' for v in row):
                    return True
        except Exception:
            pass
        return False

    @staticmethod
    def parse(filename: str, file_bytes: bytes) -> ParseResult:
        import openpyxl
        warnings_out = []

        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, keep_vba=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        # Find title cell (contains "Data for:")
        title_str = None
        for row in rows[:6]:
            for cell in row:
                if cell and 'Data for:' in str(cell):
                    title_str = str(cell)
                    break
            if title_str:
                break

        if not title_str:
            return ParseResult(
                df_summary=None, suggested_filename=filename,
                format_name=DirectTVXlsmParser.FORMAT_NAME, format_note="",
                warnings=["Could not find title cell with 'Data for:' text"],
            )

        # Parse month/year from title
        m = re.search(r'Data for:\s+(\w+)\s+(\d{4})', title_str)
        if not m:
            warnings_out.append(f"Could not parse month/year from title: {title_str[:100]}")
            year, month, month_name_str = None, None, ''
        else:
            month_name_str = m.group(1)
            month = _MONTH_MAP.get(month_name_str.lower())
            year = int(m.group(2))
            if not month:
                warnings_out.append(f"Unrecognized month name: {month_name_str}")
                month = None

        # Parse channel from first line of title (before the newline)
        first_line = title_str.split('\n')[0].strip()
        if 'Confess by Nosey' in first_line:
            channel = 'Confess by Nosey'
        elif 'Nosey' in first_line:
            channel = 'Nosey'
        else:
            channel = 'Unknown'
            warnings_out.append(f"Could not determine channel from title: {first_line}")

        # Find Total Time Viewed column
        col_idx = None
        data_start_row = None
        for i, row in enumerate(rows):
            if any(v == 'Total Time Viewed' for v in row):
                col_idx = next(j for j, v in enumerate(row) if v == 'Total Time Viewed')
                data_start_row = i + 1
                break

        if col_idx is None:
            return ParseResult(
                df_summary=None, suggested_filename=filename,
                format_name=DirectTVXlsmParser.FORMAT_NAME, format_note="",
                warnings=["Could not find 'Total Time Viewed' column"],
            )

        # Sum hh:mm:ss across all device rows
        total_hov = 0.0
        device_row_count = 0
        for row in rows[data_start_row:]:
            if not any(v is not None and v != '' for v in row):
                continue
            time_val = row[col_idx]
            if time_val and ':' in str(time_val):
                total_hov += _parse_hhmmss(time_val)
                device_row_count += 1

        suggested_filename = f"{first_line} - {month_name_str} {year}" if m else first_line
        format_note = (
            f"`Total Time Viewed` (hh:mm:ss) summed across {device_row_count} device rows "
            f"→ **{total_hov:,.2f} HOV**"
        )

        if month is None or year is None:
            return ParseResult(
                df_summary=None, suggested_filename=suggested_filename,
                format_name=DirectTVXlsmParser.FORMAT_NAME,
                format_note=format_note, warnings=warnings_out,
            )

        df_summary = pd.DataFrame([{
            'Channel': channel,
            'Territory': 'United States',
            'Year': year,
            'Month': month,
            'Quarter': _quarter(month),
            'HOV': round(total_hov, 4),
            'Filename': suggested_filename,
        }])

        return ParseResult(
            df_summary=df_summary, suggested_filename=suggested_filename,
            format_name=DirectTVXlsmParser.FORMAT_NAME,
            format_note=format_note, warnings=warnings_out,
        )


# ── Generic CSV (catch-all) ───────────────────────────────────────────────────

class GenericCsvParser:
    """
    Handles CSVs with date, channel, territory, and minutes/hours columns.
    Supports daily (rolled up to monthly) or already-monthly data.
    """
    FORMAT_NAME = "Generic CSV"

    @staticmethod
    def can_parse(filename: str, file_bytes: bytes) -> bool:
        return filename.lower().endswith('.csv')

    @staticmethod
    def parse(filename: str, file_bytes: bytes) -> ParseResult:
        warnings_out = []

        df_raw = pd.read_csv(io.BytesIO(file_bytes))
        col_map = {c.lower().strip(): c for c in df_raw.columns}

        date_col = next(
            (col_map[k] for k in ['date', 'month', 'period', 'report date'] if k in col_map), None
        )
        channel_col = next(
            (col_map[k] for k in ['channel name', 'channel_name', 'channel'] if k in col_map), None
        )
        territory_col = next(
            (col_map[k] for k in ['territory', 'country', 'region'] if k in col_map), None
        )
        minutes_col = next(
            (col_map[k] for k in ['minutes streamed', 'minutes', 'total minutes', 'mins'] if k in col_map), None
        )
        hours_col = next(
            (col_map[k] for k in ['hours', 'hov', 'hours viewed', 'total hours'] if k in col_map), None
        )

        missing = [
            n for n, c in [('date/period', date_col), ('channel', channel_col), ('territory', territory_col)]
            if not c
        ]
        if not minutes_col and not hours_col:
            missing.append('minutes or hours column')

        if missing:
            return ParseResult(
                df_summary=None, suggested_filename=filename,
                format_name=GenericCsvParser.FORMAT_NAME, format_note="",
                warnings=[
                    f"Could not detect required columns: {', '.join(missing)}. "
                    f"Found: {list(df_raw.columns)}"
                ],
            )

        df_raw[date_col] = pd.to_datetime(df_raw[date_col], errors='coerce')
        df_raw['_year'] = df_raw[date_col].dt.year.astype(int)
        df_raw['_month'] = df_raw[date_col].dt.month.astype(int)
        df_raw['_quarter'] = df_raw[date_col].dt.quarter.map({1: 'q1', 2: 'q2', 3: 'q3', 4: 'q4'})

        if minutes_col:
            df_raw['_hov'] = pd.to_numeric(df_raw[minutes_col], errors='coerce') / 60.0
            unit_note = f"`{minutes_col}` ÷ 60 → HOV"
        else:
            df_raw['_hov'] = pd.to_numeric(df_raw[hours_col], errors='coerce')
            unit_note = f"`{hours_col}` → HOV"

        daily_check = df_raw.groupby([channel_col, territory_col, '_year', '_month'])[date_col].nunique()
        is_daily = bool((daily_check > 1).any())
        fmt_label = "Daily — rolled up to monthly" if is_daily else "Monthly"

        df_summary = (
            df_raw
            .groupby([channel_col, territory_col, '_year', '_month', '_quarter'], as_index=False)['_hov']
            .sum()
            .rename(columns={
                channel_col: 'Channel', territory_col: 'Territory',
                '_year': 'Year', '_month': 'Month', '_quarter': 'Quarter',
                '_hov': 'HOV',
            })
        )
        df_summary['HOV'] = df_summary['HOV'].round(4)
        df_summary['Filename'] = filename

        return ParseResult(
            df_summary=df_summary, suggested_filename=filename,
            format_name=GenericCsvParser.FORMAT_NAME,
            format_note=f"{fmt_label} · {unit_note}",
            warnings=warnings_out,
        )


# ── Registry ──────────────────────────────────────────────────────────────────
# Parsers are tried in order; the first whose can_parse() returns True is used.
# GenericCsvParser must stay last — it's the catch-all for CSV files.

PARSERS = [
    DirectTVXlsmParser,
    GenericCsvParser,
]


def detect_and_parse(filename: str, file_bytes: bytes) -> ParseResult:
    for parser_cls in PARSERS:
        if parser_cls.can_parse(filename, file_bytes):
            return parser_cls.parse(filename, file_bytes)
    return ParseResult(
        df_summary=None, suggested_filename=filename,
        format_name="Unknown", format_note="",
        warnings=[f"No parser matched for '{filename}'. Supported: .xlsm (DIRECTV), .csv (Generic)."],
    )
