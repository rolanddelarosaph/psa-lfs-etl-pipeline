# =============================================================================
# extractors/loader.py
# EXTRACT LAYER — reads a raw PSA LFS CSV file and returns a standardized
# DataFrame with consistent column names regardless of which quarter is loaded.
#
# Handles all 3 schema groups transparently:
#   Group A: PUFURB2K10  (2016 Q2 – 2019 Q4, except 2018 Q2)
#   Group B: PUFURB2015  (2020 Q1 – 2023 Q3)
#   Group C: No urban col (2023 Q4, 2024 Q1) → injected as NaN
#   Exception: 2018 Q2 uses HHSQN for household number
# =============================================================================

import os
import pandas as pd
from config.settings import get_schema, get_raw_filepath, COLUMN_RENAME_MAP


# Fixed raw columns present in ALL quarterly files
_FIXED_RAW_COLS = [
    "PUFREG", "PUFHHSIZE", "PUFC03_REL", "PUFC04_SEX", "PUFC05_AGE",
    "PUFC06_MSTAT", "PUFC07_GRADE", "PUFC08_CURSCH", "PUFC14_PROCC",
    "PUFC25_PBASIC", "PUFC17_NATEM", "PUFC23_PCLASS", "PUFC18_PNWHRS",
    "PUFC26_OJOB", "PUFC22_PFWRK",
]


def load_quarter(year: int, quarter: int) -> pd.DataFrame:
    """
    Load a single quarterly LFS raw CSV file.

    Selects the correct columns based on the schema for that year/quarter,
    renames everything to standardized column names, and returns a clean
    DataFrame ready for the transformation layer.

    Args:
        year:    Survey year (e.g. 2020)
        quarter: Survey quarter (1–4)

    Returns:
        pd.DataFrame with standardized column names

    Raises:
        FileNotFoundError: if the raw CSV does not exist at expected path
        KeyError: if expected columns are missing from the raw file
    """
    filepath = get_raw_filepath(year, quarter)

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Raw file not found: {filepath}\n"
            f"Expected at: {filepath}\n"
            f"Check RAW_DATA_DIR in config/settings.py"
        )

    schema = get_schema(year, quarter)
    hh_col    = schema["hh_col"]      # e.g. "PUFHHNUM" or "HHSQN"
    urban_col = schema["urban_col"]   # e.g. "PUFURB2K10", "PUFURB2015", or None

    # Build the list of columns to select from the raw file
    cols_to_select = [hh_col] + _FIXED_RAW_COLS

    if urban_col is not None:
        # Insert urban col after household number
        cols_to_select.insert(1, urban_col)

    # Read raw CSV — use low_memory=False for mixed-type columns
    try:
        raw = pd.read_csv(filepath, low_memory=False, usecols=cols_to_select)
    except ValueError as e:
        raise KeyError(
            f"Column selection failed for {year} Q{quarter}.\n"
            f"Tried to select: {cols_to_select}\n"
            f"Original error: {e}\n"
            f"Check that the raw file has the expected columns."
        )

    # Build rename map for this quarter
    rename_map = dict(COLUMN_RENAME_MAP)  # copy the fixed renames
    rename_map[hh_col]  = "Household_Number"

    if urban_col is not None:
        rename_map[urban_col] = "Urban_Rural"

    raw = raw.rename(columns=rename_map)

    # If urban col was missing in this raw file, inject it as NaN
    if urban_col is None:
        raw.insert(2, "Urban_Rural", pd.NA)

    # Add Year and Quarter columns at the front
    raw.insert(0, "Year",    year)
    raw.insert(1, "Quarter", quarter)

    return raw
